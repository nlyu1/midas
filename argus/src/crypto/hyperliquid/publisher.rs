use super::HyperliquidStreamable;
use super::UniverseManager;
use super::webstream::{HyperliquidPerpWebstreamSymbols, HyperliquidSpotWebstreamSymbols};
use super::{BboUpdate, OrderbookSnapshot, PerpAssetContext, SpotAssetContext, TradeUpdate};
use crate::constants::{RELAY_BATCH_DELAY_MS, RELAY_BATCH_SIZE, WORKER_INIT_DELAY_MS};
use crate::types::TradingSymbol;
use agora::utils::OrError;
use agora::{AgorableOption, ConnectionHandle, Relay};
use bimap::BiMap;
use futures::future;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Publisher that manages Hyperliquid market data streaming with automatic universe updates.
///
/// # Architecture Overview
///
/// The `HyperliquidPublisher` handles dynamic universe changes (symbols being added/removed on Hyperliquid)
/// while providing stable, continuous data streams to downstream consumers. This is achieved through a
/// versioned path system and relay-based bridging.
///
/// ## Path Structure
///
/// **Temporary (versioned paths):**
/// - `argus/tmp/hyperliquid/spot_{version}/{type}/{symbol}`
/// - `argus/tmp/hyperliquid/perp_{version}/{type}/{symbol}`
///
/// **Stable (consumer-facing paths):**
/// - `{agora_path}/spot/{type}/{symbol}`
/// - `{agora_path}/perp/{type}/{symbol}`
///
/// Where `{type}` is one of: `last_trade`, `bbo`, `orderbook`, `spot_context`, or `perp_context`
///
/// ## Data Flow
///
/// ```text
/// Hyperliquid WebSocket
///     ↓
/// HyperliquidWebstreamWorker (publishes to versioned paths)
///     ↓
/// argus/tmp/hyperliquid/{spot|perp}_{version}/{type}/{symbol}
///     ↓
/// Relay (subscribes from versioned, publishes to stable)
///     ↓
/// {agora_path}/{spot|perp}/{type}/{symbol}
///     ↓
/// Downstream Consumers (never see version changes)
/// ```

pub struct HyperliquidPublisher {
    perp_universe: Arc<tokio::sync::RwLock<Vec<TradingSymbol>>>,
    spot_universe: Arc<tokio::sync::RwLock<Vec<TradingSymbol>>>,
    task_handle: JoinHandle<()>,
    _agora_path: String,
}

fn agora_spot_prefix(version: u32) -> String {
    format!("argus/tmp/hyperliquid/spot_{}", version)
}

fn agora_perp_prefix(version: u32) -> String {
    format!("argus/tmp/hyperliquid/perp_{}", version)
}

/// Translates a universe of normalized symbols to Hyperliquid symbols using the BiMap.
/// Falls back to the original symbol if no mapping exists.
///
/// This is used to convert normalized symbols (used everywhere outside webstream.rs)
/// to Hyperliquid symbols (required by webstream workers for WebSocket subscriptions).
fn translate_normalized_to_hyperliquid(
    normalized_symbols: &[TradingSymbol],
    symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
) -> Vec<TradingSymbol> {
    normalized_symbols
        .iter()
        .map(|norm_symbol| {
            symbol_map
                .get_by_left(norm_symbol)
                .cloned()
                .unwrap_or_else(|| norm_symbol.clone())
        })
        .collect()
}

/// Generic relay manager for a single data type.
///
/// Manages relay instances for all symbols of a specific data type (e.g., all TradeUpdate relays).
/// Simplifies the repetitive relay management logic.
struct TypedRelaySet<T: HyperliquidStreamable> {
    relays: HashMap<TradingSymbol, Relay<AgorableOption<T>>>,
    market_type: String, // "spot" or "perp"
    agora_prefix: String,
    metaserver_connection: ConnectionHandle,
    local_gateway_port: u16,
    _phantom: PhantomData<T>,
}

impl<T: HyperliquidStreamable> TypedRelaySet<T> {
    /// Creates a new TypedRelaySet for the given symbols.
    ///
    /// # Arguments
    /// * `market_type` - Either "spot" or "perp"
    /// * `symbols` - **Normalized symbols** (e.g., "BTC_PERP", "WOW-USDC")
    async fn new(
        market_type: String,
        symbols: &[TradingSymbol],
        agora_prefix: String,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
    ) -> OrError<Self> {
        let mut relays = HashMap::new();

        for (batch_idx, chunk) in symbols.chunks(RELAY_BATCH_SIZE).enumerate() {
            for symbol in chunk {
                let dest_path = format!(
                    "{}/{}/{}/{}",
                    agora_prefix,
                    market_type,
                    T::payload_identifier(),
                    symbol.to_string()
                );
                let relay = Relay::new(
                    symbol.to_string(),
                    dest_path,
                    AgorableOption(None),
                    metaserver_connection.clone(),
                    local_gateway_port,
                )
                .await?;
                relays.insert(symbol.clone(), relay);
            }

            // Small delay between batches to allow TCP connections to close
            if batch_idx < (symbols.len() + RELAY_BATCH_SIZE - 1) / RELAY_BATCH_SIZE - 1 {
                tokio::time::sleep(tokio::time::Duration::from_millis(RELAY_BATCH_DELAY_MS)).await;
            }
        }

        Ok(Self {
            relays,
            market_type,
            agora_prefix,
            metaserver_connection,
            local_gateway_port,
            _phantom: PhantomData,
        })
    }

    /// Updates relays when the universe changes.
    ///
    /// # Arguments
    /// * `new_universe` - **Normalized symbols** for the new universe
    /// * `version` - Version number for the versioned temporary paths
    async fn bump(&mut self, new_universe: &[TradingSymbol], version: u32) -> OrError<()> {
        let current: Vec<_> = self.relays.keys().cloned().collect();
        let (new_symbols, removed_symbols) = universe_difference(&current, new_universe);

        let versioned_prefix = if self.market_type == "spot" {
            agora_spot_prefix(version)
        } else {
            agora_perp_prefix(version)
        };

        // Add new symbols
        for symbol in &new_symbols {
            let dest_path = format!(
                "{}/{}/{}/{}",
                self.agora_prefix,
                self.market_type,
                T::payload_identifier(),
                symbol.to_string()
            );
            let relay = Relay::new(
                symbol.to_string(),
                dest_path,
                AgorableOption(None),
                self.metaserver_connection.clone(),
                self.local_gateway_port,
            )
            .await?;
            self.relays.insert(symbol.clone(), relay);
        }

        // Swapon all symbols to new versioned source
        for symbol in new_universe {
            if let Some(relay) = self.relays.get_mut(symbol) {
                let src_path = format!(
                    "{}/{}/{}",
                    versioned_prefix,
                    T::payload_identifier(),
                    symbol.to_string()
                );
                relay
                    .swapon(src_path, self.metaserver_connection.clone())
                    .await?;
            }
        }

        // Remove deleted symbols
        for symbol in &removed_symbols {
            self.relays.remove(symbol);
        }

        Ok(())
    }
}

/// Manages Relay instances that bridge versioned temporary paths to stable consumer-facing paths.
///
/// When the universe changes, we spawn new workers on new versioned paths and use `swapon()` to
/// atomically switch the relay sources without disrupting downstream subscribers.
///
/// NOTE: This struct works entirely with **normalized symbols** (e.g., "BTC_PERP", "WOW-USDC"),
/// not Hyperliquid symbols. Translation from Hyperliquid→normalized happens at the HyperliquidPublisher level.
struct PublisherRelays {
    spot_last_trade: TypedRelaySet<TradeUpdate>,
    spot_bbo: TypedRelaySet<BboUpdate>,
    spot_orderbook: TypedRelaySet<OrderbookSnapshot>,
    spot_context: TypedRelaySet<SpotAssetContext>,

    perp_last_trade: TypedRelaySet<TradeUpdate>,
    perp_bbo: TypedRelaySet<BboUpdate>,
    perp_orderbook: TypedRelaySet<OrderbookSnapshot>,
    perp_context: TypedRelaySet<PerpAssetContext>,
}

/// Calculates the difference between two symbol universes.
/// Returns (new_symbols, removed_symbols) where:
/// - new_symbols: symbols that exist in `new` but not in `original`
/// - removed_symbols: symbols that exist in `original` but not in `new`
fn universe_difference(
    original: &[TradingSymbol],
    new: &[TradingSymbol],
) -> (Vec<TradingSymbol>, Vec<TradingSymbol>) {
    let original_set: std::collections::HashSet<_> = original.iter().collect();
    let new_set: std::collections::HashSet<_> = new.iter().collect();

    let new_symbols: Vec<TradingSymbol> = new
        .iter()
        .filter(|s| !original_set.contains(s))
        .cloned()
        .collect();

    let removed_symbols: Vec<TradingSymbol> = original
        .iter()
        .filter(|s| !new_set.contains(s))
        .cloned()
        .collect();

    (new_symbols, removed_symbols)
}

impl PublisherRelays {
    /// Creates a new PublisherRelays instance with initial relays for the given universes.
    ///
    /// This initializes all relay instances with stable destination paths. The relays start
    /// with no source; use `bump()` to connect them to the first versioned sources.
    ///
    /// # Arguments
    /// * `spot_universe` - **Normalized spot symbols** (e.g., "WOW-USDC", "PURR-USDC")
    /// * `perp_universe` - **Normalized perp symbols** (e.g., "BTC_PERP", "ETH_PERP")
    pub async fn new(
        agora_prefix: String,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        spot_universe: &[TradingSymbol],
        perp_universe: &[TradingSymbol],
    ) -> OrError<Self> {
        Ok(Self {
            spot_last_trade: TypedRelaySet::new(
                "spot".into(),
                spot_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            spot_bbo: TypedRelaySet::new(
                "spot".into(),
                spot_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            spot_orderbook: TypedRelaySet::new(
                "spot".into(),
                spot_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            spot_context: TypedRelaySet::new(
                "spot".into(),
                spot_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            perp_last_trade: TypedRelaySet::new(
                "perp".into(),
                perp_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            perp_bbo: TypedRelaySet::new(
                "perp".into(),
                perp_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            perp_orderbook: TypedRelaySet::new(
                "perp".into(),
                perp_universe,
                agora_prefix.clone(),
                metaserver_connection.clone(),
                local_gateway_port,
            )
            .await?,
            perp_context: TypedRelaySet::new(
                "perp".into(),
                perp_universe,
                agora_prefix,
                metaserver_connection,
                local_gateway_port,
            )
            .await?,
        })
    }

    /// Updates relays when the universe changes.
    ///
    /// For new symbols: Creates new relay instances.
    /// For existing symbols: Calls `swapon()` to switch to new versioned source paths.
    /// For removed symbols: Drops the relay (removing it from the HashMap).
    ///
    /// # Arguments
    /// * `spot_universe` - **Normalized spot symbols** for the new universe
    /// * `perp_universe` - **Normalized perp symbols** for the new universe
    /// * `version` - Version number for the versioned paths
    pub async fn bump(
        &mut self,
        spot_universe: &[TradingSymbol],
        perp_universe: &[TradingSymbol],
        version: u32,
    ) -> OrError<()> {
        // Delegate to TypedRelaySet::bump for each data type
        self.spot_last_trade.bump(spot_universe, version).await?;
        self.spot_bbo.bump(spot_universe, version).await?;
        self.spot_orderbook.bump(spot_universe, version).await?;
        self.spot_context.bump(spot_universe, version).await?;

        self.perp_last_trade.bump(perp_universe, version).await?;
        self.perp_bbo.bump(perp_universe, version).await?;
        self.perp_orderbook.bump(perp_universe, version).await?;
        self.perp_context.bump(perp_universe, version).await?;

        Ok(())
    }
}

impl HyperliquidPublisher {
    /// Creates a new HyperliquidPublisher that manages all Hyperliquid market data.
    ///
    /// # Arguments
    ///
    /// * `agora_path` - Base path for stable consumer-facing Agora paths (e.g., "argus/hyperliquid")
    /// * `metaserver_connection` - Connection to the Agora metaserver
    /// * `local_gateway_port` - Port for the local Agora gateway
    /// * `universe_update_interval` - How often to check Hyperliquid API for universe changes
    /// * `check_interval` - How often to check if the universe has changed and needs version bump
    ///
    /// # Returns
    ///
    /// A `HyperliquidPublisher` that automatically streams all Hyperliquid data to stable paths
    pub async fn new(
        agora_path: &str,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        universe_update_interval: Duration,
        check_interval: Duration,
    ) -> OrError<Self> {
        let universe_manager = Arc::new(UniverseManager::new(universe_update_interval).await?);
        let perp_universe = Arc::new(RwLock::new(universe_manager.perp_universe().await?));
        let spot_universe = Arc::new(RwLock::new(universe_manager.spot_universe().await?));

        let agora_path_clone = agora_path.to_string();
        let perp_clone = perp_universe.clone();
        let spot_clone = spot_universe.clone();
        let metaserver_connection_clone = metaserver_connection.clone();
        let universe_manager_clone = universe_manager.clone();

        let task_handle = tokio::spawn(async move {
            // Publish to versioned paths: argus/tmp/hyperliquid/spot_{version}/{type}/{symbol}
            let mut version: u32 = 0;

            // Get initial universe snapshots
            let mut current_perp_universe = perp_clone.read().await.clone();
            let mut current_spot_universe = spot_clone.read().await.clone();

            // Get symbol mapper snapshot from universe manager
            let symbol_mapper = universe_manager_clone.symbol_map().await;

            // Spawn initial monitor for version 0
            let mut monitor_handle = Self::monitor_symbols(
                current_perp_universe.clone(),
                current_spot_universe.clone(),
                version,
                metaserver_connection_clone.clone(),
                local_gateway_port,
                symbol_mapper,
            );
            // Sleep to let all the agora paths set up
            tokio::time::sleep(Duration::from_millis(WORKER_INIT_DELAY_MS)).await;

            // Initialize relays with stable destination paths
            let mut relays = PublisherRelays::new(
                agora_path_clone.clone(),
                metaserver_connection_clone.clone(),
                local_gateway_port,
                &current_spot_universe,
                &current_perp_universe,
            )
            .await
            .expect("Failed to initialize relays");

            // Connect relays to initial versioned sources (version 0)
            relays
                .bump(&current_spot_universe, &current_perp_universe, version)
                .await
                .expect("Failed to connect initial relays");

            loop {
                tokio::time::sleep(check_interval).await;

                let new_perp = universe_manager_clone.perp_universe().await.unwrap();
                let new_spot = universe_manager_clone.spot_universe().await.unwrap();

                // Check for differences. Only bump version if universe actually changed
                let (new_spot_symbols, removed_spot_symbols) =
                    universe_difference(&current_spot_universe, &new_spot);
                let (new_perp_symbols, removed_perp_symbols) =
                    universe_difference(&current_perp_universe, &new_perp);

                let universe_changed = !new_spot_symbols.is_empty()
                    || !removed_spot_symbols.is_empty()
                    || !new_perp_symbols.is_empty()
                    || !removed_perp_symbols.is_empty();

                if universe_changed {
                    version += 1;
                    println!(
                        "HyperliquidPublisher: Universe changed! Bumping version to {}",
                        version
                    );

                    // Spawn new monitor with FRESH symbol_map snapshot (critical for universe changes!)
                    let new_monitor_handle = Self::monitor_symbols(
                        new_perp.clone(),
                        new_spot.clone(),
                        version,
                        metaserver_connection_clone.clone(),
                        local_gateway_port,
                        universe_manager_clone.symbol_map().await,
                    );

                    // Sleep to let new workers initialize
                    tokio::time::sleep(Duration::from_millis(WORKER_INIT_DELAY_MS)).await;

                    // Update relays to point to new versioned sources
                    if let Err(e) = relays.bump(&new_spot, &new_perp, version).await {
                        eprintln!("HyperliquidPublisher: Error bumping relays: {}", e);
                    }

                    // Update current universe
                    current_perp_universe = new_perp;
                    current_spot_universe = new_spot;

                    // Abort old monitor (this drops old workers)
                    monitor_handle.abort();
                    monitor_handle = new_monitor_handle;
                }
            }
        });

        Ok(Self {
            perp_universe,
            spot_universe,
            task_handle,
            _agora_path: agora_path.to_string(),
        })
    }

    pub fn perp_universe(&self) -> OrError<Vec<TradingSymbol>> {
        let read_guard = self
            .perp_universe
            .try_read()
            .map_err(|e| anyhow::anyhow!("Could not read perp_universe: {}", e))?;
        Ok(read_guard.clone())
    }

    /// Spawns workers for the given normalized symbol universes
    ///
    /// # Arguments
    /// * `perp_universe` - Normalized perpetual symbols (e.g., "BTC_PERP", "ETH_PERP")
    /// * `spot_universe` - Normalized spot symbols (e.g., "WOW-USDC", "PURR-USDC")
    /// * `symbol_mapper` - BiMap for normalized↔Hyperliquid translation (immutable snapshot)
    fn monitor_symbols(
        perp_universe: Vec<TradingSymbol>,
        spot_universe: Vec<TradingSymbol>,
        version: u32,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        symbol_mapper: BiMap<TradingSymbol, TradingSymbol>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let spot_prefix = agora_spot_prefix(version);
            let perp_prefix = agora_perp_prefix(version);

            // Convert normalized symbols to Hyperliquid format for webstream workers
            let hyperliquid_spot =
                translate_normalized_to_hyperliquid(&spot_universe, &symbol_mapper);
            let hyperliquid_perp =
                translate_normalized_to_hyperliquid(&perp_universe, &symbol_mapper);

            // These workers must stay in scope for the lifetime of this task
            // When the task is aborted, they will be dropped and stop streaming
            let _spot_worker = HyperliquidSpotWebstreamSymbols::new(
                &hyperliquid_spot,
                &spot_prefix,
                metaserver_connection.clone(),
                local_gateway_port,
                symbol_mapper.clone(),
            )
            .await
            .unwrap();

            let _perp_worker = HyperliquidPerpWebstreamSymbols::new(
                &hyperliquid_perp,
                &perp_prefix,
                metaserver_connection,
                local_gateway_port,
                symbol_mapper,
            )
            .await
            .unwrap();

            // Keep workers alive until task is aborted
            future::pending::<()>().await;
        })
    }

    pub fn spot_universe(&self) -> OrError<Vec<TradingSymbol>> {
        let read_guard = self
            .spot_universe
            .try_read()
            .map_err(|e| anyhow::anyhow!("Could not read spot universe: {}", e))?;
        Ok(read_guard.clone())
    }
}

impl Drop for HyperliquidPublisher {
    fn drop(&mut self) {
        self.task_handle.abort()
    }
}
