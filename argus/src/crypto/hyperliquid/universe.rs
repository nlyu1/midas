use crate::constants::HYPERLIQUID_INFO_ENDPOINT;
use crate::types::TradingSymbol;
use agora::utils::OrError;
use bimap::BiMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Hyperliquid REST API response for perpetuals metadata
#[derive(Debug, Deserialize, Serialize)]
struct PerpMeta {
    universe: Vec<PerpAsset>,
    #[serde(rename = "marginTables")]
    margin_tables: Vec<serde_json::Value>, // We don't need to parse this for now
}

/// Hyperliquid perpetual asset metadata
#[derive(Debug, Deserialize, Serialize)]
struct PerpAsset {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u8,
    #[serde(rename = "maxLeverage")]
    max_leverage: u32,
    #[serde(rename = "marginTableId")]
    margin_table_id: u32,
    #[serde(rename = "isDelisted")]
    #[serde(default)]
    is_delisted: bool,
    #[serde(rename = "onlyIsolated")]
    #[serde(default)]
    only_isolated: bool,
}

/// Hyperliquid REST API response for spot metadata
#[derive(Debug, Deserialize, Serialize)]
struct SpotMeta {
    universe: Vec<SpotAsset>,
    tokens: Vec<SpotToken>,
}

/// Hyperliquid spot asset (trading pair) metadata
#[derive(Debug, Deserialize, Serialize)]
struct SpotAsset {
    name: String,
    tokens: [u32; 2], // Indices into the tokens array
    index: u32,
    #[serde(rename = "isCanonical")]
    is_canonical: bool,
}

/// Hyperliquid EVM contract info
#[derive(Debug, Deserialize, Serialize)]
struct EvmContract {
    address: String,
    evm_extra_wei_decimals: i32, // Can be negative (e.g., -2 for UBTC)
}

/// Hyperliquid spot token metadata
#[derive(Debug, Deserialize, Serialize)]
struct SpotToken {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u8,
    #[serde(rename = "weiDecimals")]
    wei_decimals: u8,
    index: u32,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "isCanonical")]
    is_canonical: bool,
    #[serde(rename = "evmContract")]
    evm_contract: Option<EvmContract>,
    #[serde(rename = "fullName")]
    full_name: Option<String>,
    #[serde(rename = "deployerTradingFeeShare")]
    deployer_trading_fee_share: String,
}

/// Request body for fetching perpetuals metadata
#[derive(Debug, Serialize)]
struct MetaRequest {
    #[serde(rename = "type")]
    request_type: String,
}

/// Request body for fetching spot metadata
#[derive(Debug, Serialize)]
struct SpotMetaRequest {
    #[serde(rename = "type")]
    request_type: String,
}

/// Fetches perpetual universe from Hyperliquid REST API
async fn fetch_perp_meta() -> OrError<PerpMeta> {
    let client = reqwest::Client::new();
    let request = MetaRequest {
        request_type: "meta".to_string(),
    };

    let response = client
        .post(HYPERLIQUID_INFO_ENDPOINT)
        .json(&request)
        .send()
        .await
        .map_err(|e| format!("Hyperliquid REST API request error (perp meta): {}", e))?;

    if !response.status().is_success() {
        return Err(format!(
            "Hyperliquid REST API error (perp meta): HTTP {}",
            response.status()
        ));
    }

    let meta: PerpMeta = response.json().await.map_err(|e| {
        format!(
            "Hyperliquid REST API response parse error (perp meta): {}",
            e
        )
    })?;

    Ok(meta)
}

/// Fetches spot universe from Hyperliquid REST API
async fn fetch_spot_meta() -> OrError<SpotMeta> {
    let client = reqwest::Client::new();
    let request = SpotMetaRequest {
        request_type: "spotMeta".to_string(),
    };

    let response = client
        .post(HYPERLIQUID_INFO_ENDPOINT)
        .json(&request)
        .send()
        .await
        .map_err(|e| format!("Hyperliquid REST API request error (spot meta): {}", e))?;

    if !response.status().is_success() {
        return Err(format!(
            "Hyperliquid REST API error (spot meta): HTTP {}",
            response.status()
        ));
    }

    let meta: SpotMeta = response.json().await.map_err(|e| {
        format!(
            "Hyperliquid REST API response parse error (spot meta): {}",
            e
        )
    })?;

    Ok(meta)
}

/// Sanitizes a symbol string to contain only alphanumeric characters and hyphens
fn sanitize_symbol(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}

/// Extracts actively traded (non-delisted) perpetual symbols from metadata
/// Returns normalized symbols (e.g., "BTC_PERP") with BiMap entries (normalized ↔ hyperliquid)
///
/// BiMap left=normalized ("BTC_PERP"), right=hyperliquid ("BTC")
fn extract_active_perp_symbols(
    meta: &PerpMeta,
    symbol_map: &mut BiMap<TradingSymbol, TradingSymbol>,
) -> OrError<Vec<TradingSymbol>> {
    let mut symbols = Vec::new();
    for asset in &meta.universe {
        if !asset.is_delisted {
            // Hyperliquid API name (e.g., "BTC")
            let hyperliquid_name = asset.name.clone();

            // Normalized name with _PERP suffix (e.g., "BTC_PERP")
            let normalized_name = format!("{}_PERP", sanitize_symbol(&hyperliquid_name));

            let normalized_symbol = TradingSymbol::from_str(&normalized_name)?;
            let hyperliquid_symbol = TradingSymbol::from_str(&hyperliquid_name)?;

            // Insert bidirectional mapping: normalized ↔ hyperliquid
            symbol_map.insert(normalized_symbol.clone(), hyperliquid_symbol.clone());

            // Return normalized symbols (for use outside of webstream layer)
            symbols.push(normalized_symbol);
        }
    }
    Ok(symbols)
}

/// Extracts all spot symbols from metadata with bidirectional mapping
/// Returns normalized symbols (e.g., "WOW-USDC", "PURR-USDC") with BiMap entries (normalized ↔ hyperliquid)
/// Note: Spot markets don't have a delisted flag like perps, so we return all symbols
///
/// BiMap left=normalized ("WOW-USDC"), right=hyperliquid ("@109" or "PURR/USDC")
fn extract_active_spot_symbols(
    meta: &SpotMeta,
    symbol_map: &mut BiMap<TradingSymbol, TradingSymbol>,
) -> OrError<Vec<TradingSymbol>> {
    // Build token index → token name mapping
    let mut token_names: std::collections::HashMap<u32, String> = std::collections::HashMap::new();
    for token in &meta.tokens {
        token_names.insert(token.index, token.name.clone());
    }

    let mut symbols = Vec::new();
    for asset in &meta.universe {
        // Hyperliquid API name (e.g., "PURR/USDC" or "@109")
        let hyperliquid_name = asset.name.clone();

        // Get token names for this pair
        let token0_idx = asset.tokens[0];
        let token1_idx = asset.tokens[1];

        let token0_name = token_names
            .get(&token0_idx)
            .ok_or(format!("Token index {} not found in metadata", token0_idx))?;
        let token1_name = token_names
            .get(&token1_idx)
            .ok_or(format!("Token index {} not found in metadata", token1_idx))?;

        // Construct normalized name: TOKEN0-TOKEN1 (e.g., "WOW-USDC", "PURR-USDC")
        // Replace / with - and sanitize
        let normalized_name = format!(
            "{}-{}",
            sanitize_symbol(token0_name),
            sanitize_symbol(token1_name)
        );

        let normalized_symbol = TradingSymbol::from_str(&normalized_name)?;
        let hyperliquid_symbol = TradingSymbol::from_str(&hyperliquid_name)?;

        // Insert bidirectional mapping: normalized ↔ hyperliquid
        symbol_map.insert(normalized_symbol.clone(), hyperliquid_symbol.clone());

        // Return normalized symbols (for use outside of webstream layer)
        symbols.push(normalized_symbol);
    }
    Ok(symbols)
}

/// Manages the universe of Hyperliquid trading symbols
///
/// Automatically polls REST API to keep universe up to date with active (non-delisted) symbols.
/// Runs separate background tasks for perpetuals and spot markets.
pub struct UniverseManager {
    perp_universe: Arc<RwLock<Vec<TradingSymbol>>>,
    spot_universe: Arc<RwLock<Vec<TradingSymbol>>>,
    symbol_map: Arc<RwLock<BiMap<TradingSymbol, TradingSymbol>>>,
    _perp_universe_update_handle: JoinHandle<()>,
    _spot_universe_update_handle: JoinHandle<()>,
}

impl UniverseManager {
    /// Creates a new UniverseManager
    ///
    /// Starts two background tasks which ping the REST API once every `update_duration`
    /// for perpetuals and spot separately. Only stores actively traded (non-delisted) symbols.
    ///
    /// # Arguments
    /// * `update_duration` - How often to poll the REST API (e.g., Duration::from_secs(60) for 1 minute)
    ///
    /// # Returns
    /// * `OrError<Self>` - UniverseManager instance or error message
    pub async fn new(update_duration: Duration) -> OrError<Self> {
        // Initialize with empty universes
        let perp_universe = Arc::new(RwLock::new(Vec::new()));
        let spot_universe = Arc::new(RwLock::new(Vec::new()));
        let symbol_map = Arc::new(RwLock::new(BiMap::new()));

        // Do initial fetch to populate universes and symbol map
        match fetch_perp_meta().await {
            Ok(meta) => {
                let mut map_write = symbol_map.write().await;
                match extract_active_perp_symbols(&meta, &mut map_write) {
                    Ok(symbols) => {
                        let mut perp_write = perp_universe.write().await;
                        *perp_write = symbols;
                        println!(
                            "Hyperliquid UniverseManager: Initial perp universe loaded ({} symbols)",
                            perp_write.len()
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "Hyperliquid UniverseManager: Error extracting perp symbols: {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Hyperliquid UniverseManager: Initial perp fetch failed: {}",
                    e
                );
            }
        }

        match fetch_spot_meta().await {
            Ok(meta) => {
                let mut map_write = symbol_map.write().await;
                match extract_active_spot_symbols(&meta, &mut map_write) {
                    Ok(symbols) => {
                        let mut spot_write = spot_universe.write().await;
                        *spot_write = symbols;
                        println!(
                            "Hyperliquid UniverseManager: Initial spot universe loaded ({} symbols)",
                            spot_write.len()
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "Hyperliquid UniverseManager: Error extracting spot symbols: {}",
                            e
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Hyperliquid UniverseManager: Initial spot fetch failed: {}",
                    e
                );
            }
        }

        // Spawn background task for perp universe updates
        let perp_universe_clone = perp_universe.clone();
        let symbol_map_clone_perp = symbol_map.clone();
        let perp_update_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(update_duration);
            loop {
                interval.tick().await;

                match fetch_perp_meta().await {
                    Ok(meta) => {
                        // Need to rebuild symbol map for perps
                        let mut temp_map = BiMap::new();
                        match extract_active_perp_symbols(&meta, &mut temp_map) {
                            Ok(symbols) => {
                                // Update universe
                                let mut perp_write = perp_universe_clone.write().await;
                                let old_count = perp_write.len();
                                *perp_write = symbols;
                                let new_count = perp_write.len();

                                // Update symbol map (remove old perp entries, add new ones)
                                let mut map_write = symbol_map_clone_perp.write().await;
                                // Remove entries where normalized symbol ends with _PERP
                                map_write.retain(|normalized, _| {
                                    !normalized.to_string().ends_with("_PERP")
                                });
                                // Insert new perp mappings
                                for (normalized, hyperliquid) in temp_map.iter() {
                                    map_write.insert(normalized.clone(), hyperliquid.clone());
                                }

                                if old_count != new_count {
                                    println!(
                                        "Hyperliquid UniverseManager: Perp universe updated ({} -> {} symbols)",
                                        old_count, new_count
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Hyperliquid UniverseManager: Error extracting perp symbols: {}",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Hyperliquid UniverseManager: Perp fetch error: {}", e);
                    }
                }
            }
        });

        // Spawn background task for spot universe updates
        let spot_universe_clone = spot_universe.clone();
        let symbol_map_clone_spot = symbol_map.clone();
        let spot_update_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(update_duration);
            loop {
                interval.tick().await;

                match fetch_spot_meta().await {
                    Ok(meta) => {
                        // Need to rebuild symbol map for spots
                        let mut temp_map = BiMap::new();
                        match extract_active_spot_symbols(&meta, &mut temp_map) {
                            Ok(symbols) => {
                                // Update universe
                                let mut spot_write = spot_universe_clone.write().await;
                                let old_count = spot_write.len();
                                *spot_write = symbols;
                                let new_count = spot_write.len();

                                // Update symbol map (remove old spot entries, add new ones)
                                let mut map_write = symbol_map_clone_spot.write().await;
                                // Remove entries where normalized symbol does NOT end with _PERP (spot symbols)
                                map_write.retain(|normalized, _| {
                                    normalized.to_string().ends_with("_PERP")
                                });
                                // Insert new spot mappings
                                for (normalized, hyperliquid) in temp_map.iter() {
                                    map_write.insert(normalized.clone(), hyperliquid.clone());
                                }

                                if old_count != new_count {
                                    println!(
                                        "Hyperliquid UniverseManager: Spot universe updated ({} -> {} symbols)",
                                        old_count, new_count
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Hyperliquid UniverseManager: Error extracting spot symbols: {}",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Hyperliquid UniverseManager: Spot fetch error: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            perp_universe,
            spot_universe,
            symbol_map,
            _perp_universe_update_handle: perp_update_handle,
            _spot_universe_update_handle: spot_update_handle,
        })
    }

    /// Returns a snapshot of the current perpetual universe
    ///
    /// # Returns
    /// * `OrError<Vec<TradingSymbol>>` - List of normalized perpetual symbols (e.g., "BTC_PERP", "ETH_PERP")
    pub async fn perp_universe(&self) -> OrError<Vec<TradingSymbol>> {
        let perp_read = self.perp_universe.read().await;
        // Ok(perp_read.clone()[0..120].to_vec())
        Ok(perp_read.clone())
    }

    /// Returns a snapshot of the current spot universe
    ///
    /// # Returns
    /// * `OrError<Vec<TradingSymbol>>` - List of normalized spot symbols (e.g., "WOW-USDC", "PURR-USDC")
    pub async fn spot_universe(&self) -> OrError<Vec<TradingSymbol>> {
        let spot_read = self.spot_universe.read().await;
        Ok(spot_read.clone())
    }

    /// Translates a normalized symbol to its Hyperliquid API format
    ///
    /// # Arguments
    /// * `normalized` - Normalized symbol (e.g., "BTC_PERP", "WOW-USDC")
    ///
    /// # Returns
    /// * `Option<TradingSymbol>` - Hyperliquid API symbol (e.g., "BTC", "@109") or None if not found
    pub async fn translate_to_hyperliquid(
        &self,
        normalized: &TradingSymbol,
    ) -> Option<TradingSymbol> {
        let map_read = self.symbol_map.read().await;
        map_read.get_by_left(normalized).cloned()
    }

    /// Translates a Hyperliquid API symbol to its normalized format
    ///
    /// # Arguments
    /// * `hyperliquid` - Hyperliquid API symbol (e.g., "BTC", "@109", "PURR/USDC")
    ///
    /// # Returns
    /// * `Option<TradingSymbol>` - Normalized symbol (e.g., "BTC_PERP", "WOW-USDC") or None if not found
    pub async fn translate_to_normalized(
        &self,
        hyperliquid: &TradingSymbol,
    ) -> Option<TradingSymbol> {
        let map_read = self.symbol_map.read().await;
        map_read.get_by_right(hyperliquid).cloned()
    }

    /// Returns a clone of the symbol map for use in workers
    ///
    /// Workers get an immutable snapshot of the map at creation time.
    /// When universe changes, old workers are aborted and new workers get a fresh snapshot.
    ///
    /// # Returns
    /// * `BiMap<TradingSymbol, TradingSymbol>` - Cloned symbol map
    pub async fn symbol_map(&self) -> BiMap<TradingSymbol, TradingSymbol> {
        let map_read = self.symbol_map.read().await;
        map_read.clone()
    }
}

impl Drop for UniverseManager {
    fn drop(&mut self) {
        self._perp_universe_update_handle.abort();
        self._spot_universe_update_handle.abort();
    }
}
