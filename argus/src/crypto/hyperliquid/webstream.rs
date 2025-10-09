use super::HyperliquidStreamable;
use super::{BboUpdate, OrderbookSnapshot, PerpAssetContext, SpotAssetContext, TradeUpdate};
use crate::constants::HYPERLIQUID_WEBSTREAM_ENDPOINT;
use crate::types::TradingSymbol;
use agora::utils::OrError;
use agora::{AgorableOption, ConnectionHandle, Publisher};
use bimap::BiMap;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub struct HyperliquidWebstreamWorker<T: HyperliquidStreamable> {
    agora_paths: Vec<String>,
    dispatch_handle: JoinHandle<()>,
    _phantom: PhantomData<T>,
}

impl<T: HyperliquidStreamable> HyperliquidWebstreamWorker<T> {
    /// Creates a new HyperliquidWebstreamWorker
    ///
    /// # Arguments
    /// * `symbols` - **Hyperliquid symbols** (e.g., "BTC", "@109", "PURR/USDC")
    /// * `symbol_mapper` - BiMap for translating Hyperliquid → normalized (ONLY used in this direction)
    pub async fn new(
        symbols: &[TradingSymbol],
        agora_prefix: &str,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        symbol_mapper: BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Self> {
        if symbols.is_empty() {
            return Err(
                "HyperliquidWebstreamWorker error: symbols list cannot be empty".to_string(),
            );
        }
        if symbols.len() > 1024 {
            return Err(
                "HyperliquidWebstreamWorker error: don't pass in more than 1024 tasks per worker"
                    .to_string(),
            );
        }

        // Translate Hyperliquid symbols → normalized for Agora paths
        // symbols are Hyperliquid format (e.g., "BTC", "@109")
        // Agora paths use normalized format (e.g., "BTC_PERP", "WOW-USDC")
        let mut normalized_symbols: Vec<TradingSymbol> = Vec::new();
        for hyperliquid_symbol in symbols {
            if let Some(normalized) = symbol_mapper.get_by_right(hyperliquid_symbol) {
                normalized_symbols.push(normalized.clone());
            } else {
                eprintln!(
                    "HyperliquidWebstreamWorker: Warning - no mapping found for Hyperliquid symbol {}",
                    hyperliquid_symbol.to_string()
                );
                // Fallback: use the symbol as-is
                normalized_symbols.push(hyperliquid_symbol.clone());
            }
        }

        // Agora paths use normalized symbols
        let agora_paths: Vec<String> = normalized_symbols
            .iter()
            .map(|normalized_symbol: &TradingSymbol| {
                format!(
                    "{}/{}/{}",
                    agora_prefix,
                    T::payload_identifier(),
                    normalized_symbol.to_string()
                )
            })
            .collect();

        // Create publishers for each normalized symbol
        let mut publishers: Vec<Publisher<AgorableOption<T>>> = Vec::new();
        for (normalized_symbol, agora_path) in normalized_symbols.iter().zip(agora_paths.iter()) {
            let publisher_name = normalized_symbol.to_string();
            let publisher = Publisher::<AgorableOption<T>>::new(
                publisher_name,
                agora_path.clone(),
                AgorableOption(None),
                metaserver_connection,
                local_gateway_port,
            )
            .await?;
            publishers.push(publisher);
        }

        // Build normalized_symbol->publisher mapping for message dispatch
        let symbol_to_publisher: HashMap<String, usize> = normalized_symbols
            .iter()
            .enumerate()
            .map(|(idx, normalized_symbol)| (normalized_symbol.to_string(), idx))
            .collect();

        let ws_url = HYPERLIQUID_WEBSTREAM_ENDPOINT.to_string();
        let subscription_type = T::subscription_type();

        // Use Hyperliquid symbols directly for WebSocket subscription (NO TRANSLATION)
        let coins: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();

        println!("Connecting to Hyperliquid WebSocket: {}", ws_url);
        println!(
            "Subscribing to {} for coins: {:?}",
            subscription_type, coins
        );
        let agora_prefix_clone = agora_prefix.to_string();

        // Spawn worker task to handle websocket connection
        let worker_task = tokio::spawn(async move {
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        println!("Connected to Hyperliquid WebSocket");
                        let (mut write, mut read) = ws_stream.split();

                        // Send subscription messages for each coin
                        for coin in &coins {
                            let subscription = serde_json::json!({
                                "method": "subscribe",
                                "subscription": {
                                    "type": subscription_type,
                                    "coin": coin
                                }
                            });
                            if let Err(e) = write
                                .send(Message::Text(subscription.to_string().into()))
                                .await
                            {
                                eprintln!(
                                    "HyperliquidWebstreamWorker error sending subscription for {}: {}",
                                    coin, e
                                );
                                break;
                            }
                        }

                        println!("Sent {} subscriptions", coins.len());

                        // Spawn heartbeat task to keep connection alive
                        // Hyperliquid server closes connections with no activity for 60s
                        // Send ping every 30s to stay well below threshold
                        let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel::<Message>(10);
                        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

                        let heartbeat_task = tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(tokio::time::Duration::from_secs(30));
                            loop {
                                tokio::select! {
                                    _ = interval.tick() => {
                                        let ping_msg = serde_json::json!({"method": "ping"});
                                        if let Err(_) = ping_tx.send(Message::Text(ping_msg.to_string().into())).await {
                                            // Channel closed, exit
                                            break;
                                        }
                                    }
                                    _ = shutdown_rx.recv() => {
                                        // Shutdown signal received
                                        break;
                                    }
                                }
                            }
                        });

                        // Process incoming messages and heartbeat pings
                        loop {
                            tokio::select! {
                                // Handle incoming WebSocket messages
                                message = read.next() => {
                                    match message {
                                        Some(Ok(Message::Ping(ping_data))) => {
                                            if let Err(e) = write.send(Message::Pong(ping_data)).await {
                                                eprintln!(
                                                    "HyperliquidWebstreamWorker error sending Pong: {}",
                                                    e
                                                );
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Text(text))) => {
                                    // Parse the channel wrapper
                                    #[derive(Deserialize)]
                                    struct ChannelMessage {
                                        channel: String,
                                        data: Option<serde_json::Value>,
                                    }

                                    match serde_json::from_str::<ChannelMessage>(&text) {
                                        Ok(msg) => {
                                            // Handle subscription confirmation
                                            if msg.channel == "subscriptionResponse" {
                                                println!("Subscription confirmed");
                                                continue;
                                            }

                                            // Handle heartbeat pong response
                                            if msg.channel == "pong" {
                                                // Heartbeat acknowledged by server
                                                continue;
                                            }

                                            // Check if this is the right channel type
                                            if msg.channel != subscription_type {
                                                continue;
                                            }

                                            // Extract coin from the data
                                            // For most channels, the data contains a "coin" field
                                            let Some(data) = msg.data else {
                                                eprintln!("HyperliquidWebstreamWorker: message missing data field");
                                                continue;
                                            };

                                            if let Some(coin_value) = data.get("coin") {
                                                if let Some(coin_str_ref) = coin_value.as_str() {
                                                    let hyperliquid_coin = coin_str_ref.to_string();

                                                    // Translate Hyperliquid symbol to normalized (no locking!)
                                                    let normalized_coin = if let Ok(hyperliquid_symbol) = TradingSymbol::from_str(&hyperliquid_coin) {
                                                        if let Some(normalized) = symbol_mapper.get_by_right(&hyperliquid_symbol) {
                                                            normalized.to_string()
                                                        } else {
                                                            eprintln!(
                                                                "HyperliquidWebstreamWorker: Warning - no mapping found for Hyperliquid symbol {}",
                                                                hyperliquid_coin
                                                            );
                                                            hyperliquid_coin.clone()
                                                        }
                                                    } else {
                                                        hyperliquid_coin.clone()
                                                    };

                                                    // Parse the message using the Hyperliquid coin name
                                                    // (of_channel_data expects the original Hyperliquid format)
                                                    match T::of_channel_data(data, &hyperliquid_coin) {
                                                        Ok(parsed_msg) => {
                                                            // Route to correct publisher using normalized symbol
                                                            if let Some(&publisher_idx) =
                                                                symbol_to_publisher.get(&normalized_coin)
                                                            {
                                                                if let Some(publisher) = publishers
                                                                    .get_mut(publisher_idx)
                                                                {
                                                                    if let Err(e) = publisher
                                                                        .publish(AgorableOption(
                                                                            Some(parsed_msg),
                                                                        ))
                                                                        .await
                                                                    {
                                                                        eprintln!(
                                                                            "HyperliquidWebstreamWorker publish error for {}: {}",
                                                                            normalized_coin, e
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            eprintln!(
                                                                "HyperliquidWebstreamWorker parse error: {}",
                                                                e
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "HyperliquidWebstreamWorker {} JSON parse error for {}: {}",
                                                agora_prefix_clone,
                                                text,
                                                e
                                            );
                                        }
                                    }
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            eprintln!(
                                                "HyperliquidWebstreamWorker: received Close frame, disconnecting"
                                            );
                                            break;
                                        }
                                        Some(Ok(_)) => {
                                            // Ignore other message types
                                        }
                                        Some(Err(e)) => {
                                            eprintln!("HyperliquidWebstreamWorker websocket error: {}", e);
                                            break;
                                        }
                                        None => {
                                            eprintln!("HyperliquidWebstreamWorker: connection closed");
                                            break;
                                        }
                                    }
                                }
                                // Handle heartbeat pings from background task
                                ping_msg = ping_rx.recv() => {
                                    if let Some(msg) = ping_msg {
                                        if let Err(e) = write.send(msg).await {
                                            eprintln!("HyperliquidWebstreamWorker error sending heartbeat ping: {}", e);
                                            break;
                                        }
                                    } else {
                                        // Channel closed
                                        break;
                                    }
                                }
                            }
                        }

                        // Connection closed, clean up heartbeat task
                        let _ = shutdown_tx.send(()).await;
                        heartbeat_task.abort();
                    }
                    Err(e) => {
                        eprintln!(
                            "HyperliquidWebstreamWorker connection error: {}, retrying in 5s...",
                            e
                        );
                    }
                }
                // Wait before retry
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });

        Ok(Self {
            agora_paths,
            dispatch_handle: worker_task,
            _phantom: PhantomData,
        })
    }

    /// Returns the agora paths for all publishers managed by this worker
    pub fn agora_paths(&self) -> &[String] {
        &self.agora_paths
    }
}

impl<T: HyperliquidStreamable> Drop for HyperliquidWebstreamWorker<T> {
    fn drop(&mut self) {
        self.dispatch_handle.abort()
    }
}

// We need to hold internal references to these workers, because dropping them would stop streaming.

/// Aggregator for Perpetual market data workers
pub struct HyperliquidPerpWebstreamSymbols {
    symbols: Vec<TradingSymbol>,
    _trade_worker: HyperliquidWebstreamWorker<TradeUpdate>,
    _bbo_worker: HyperliquidWebstreamWorker<BboUpdate>,
    _orderbook_worker: HyperliquidWebstreamWorker<OrderbookSnapshot>,
    _context_worker: HyperliquidWebstreamWorker<PerpAssetContext>,
}

impl HyperliquidPerpWebstreamSymbols {
    /// Creates workers for perpetual market data
    ///
    /// # Arguments
    /// * `symbols` - **Hyperliquid symbols** (e.g., "BTC", "ETH")
    /// * `symbol_mapper` - BiMap for Hyperliquid→normalized translation only
    pub async fn new(
        symbols: &[TradingSymbol],
        agora_prefix: &str,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        symbol_mapper: BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Self> {
        let trade_worker = HyperliquidWebstreamWorker::<TradeUpdate>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let bbo_worker = HyperliquidWebstreamWorker::<BboUpdate>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let orderbook_worker = HyperliquidWebstreamWorker::<OrderbookSnapshot>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let context_worker = HyperliquidWebstreamWorker::<PerpAssetContext>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper,
        )
        .await?;

        Ok(Self {
            symbols: symbols.to_vec(),
            _trade_worker: trade_worker,
            _bbo_worker: bbo_worker,
            _orderbook_worker: orderbook_worker,
            _context_worker: context_worker,
        })
    }

    pub fn symbols(&self) -> &[TradingSymbol] {
        &self.symbols
    }
}

/// Aggregator for Spot market data workers
pub struct HyperliquidSpotWebstreamSymbols {
    symbols: Vec<TradingSymbol>,
    _trade_worker: HyperliquidWebstreamWorker<TradeUpdate>,
    _bbo_worker: HyperliquidWebstreamWorker<BboUpdate>,
    _orderbook_worker: HyperliquidWebstreamWorker<OrderbookSnapshot>,
    _context_worker: HyperliquidWebstreamWorker<SpotAssetContext>,
}

impl HyperliquidSpotWebstreamSymbols {
    /// Creates workers for spot market data
    ///
    /// # Arguments
    /// * `symbols` - **Hyperliquid symbols** (e.g., "@109", "PURR/USDC")
    /// * `symbol_mapper` - BiMap for Hyperliquid→normalized translation only
    pub async fn new(
        symbols: &[TradingSymbol],
        agora_prefix: &str,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
        symbol_mapper: BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Self> {
        let trade_worker = HyperliquidWebstreamWorker::<TradeUpdate>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let bbo_worker = HyperliquidWebstreamWorker::<BboUpdate>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let orderbook_worker = HyperliquidWebstreamWorker::<OrderbookSnapshot>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper.clone(),
        )
        .await?;

        let context_worker = HyperliquidWebstreamWorker::<SpotAssetContext>::new(
            symbols,
            agora_prefix,
            metaserver_connection.clone(),
            local_gateway_port,
            symbol_mapper,
        )
        .await?;

        Ok(Self {
            symbols: symbols.to_vec(),
            _trade_worker: trade_worker,
            _bbo_worker: bbo_worker,
            _orderbook_worker: orderbook_worker,
            _context_worker: context_worker,
        })
    }

    pub fn symbols(&self) -> &[TradingSymbol] {
        &self.symbols
    }
}
