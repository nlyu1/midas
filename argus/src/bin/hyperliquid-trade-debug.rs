#!/usr/bin/env rust
//! Hyperliquid Trade Debug Tool - Comprehensive Infrastructure Testing
//!
//! This tool tests BOTH:
//! 1. Direct WebSocket connection (proven working)
//! 2. Your Agora publisher/subscriber infrastructure
//!
//! It helps identify where trades are being lost in the pipeline.
//!
//! Usage:
//!   # First, start the metaserver in another terminal:
//!   cd ../agora && cargo run --bin metaserver -- -p 8000
//!
//!   # Then run this debug tool:
//!   cargo run --bin hyperliquid-trade-debug

use agora::utils::OrError;
use agora::{AgorableOption, ConnectionHandle, Subscriber};
use argus::constants::{AGORA_GATEWAY_PORT, AGORA_METASERVER_DEFAULT_PORT, HYPERLIQUID_AGORA_PREFIX};
use argus::crypto::hyperliquid::{TradeUpdate, UniverseManager};
use argus::types::TradingSymbol;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Stats for direct WebSocket
#[derive(Default)]
struct DirectStats {
    total_messages: u64,
    trade_messages: u64,
    total_trades: u64,
    symbols_seen: std::collections::HashSet<String>,
}

/// Stats for Agora subscriber
#[derive(Default)]
struct AgoraStats {
    total_updates: u64,
    symbols_seen: std::collections::HashSet<String>,
}

/// Comparison stats
#[derive(Default)]
struct ComparisonStats {
    direct: DirectStats,
    agora: Arc<RwLock<AgoraStats>>,
    start_time: Instant,
}

impl ComparisonStats {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            ..Default::default()
        }
    }

    async fn print_comparison(&self) {
        let agora = self.agora.read().await;
        let elapsed = self.start_time.elapsed();

        println!("\n╔════════════════════════════════════════════════════════════════╗");
        println!("║                    COMPARISON REPORT                           ║");
        println!("╠════════════════════════════════════════════════════════════════╣");
        println!("║ Runtime: {:.1}s                                                 ║", elapsed.as_secs_f64());
        println!("╠════════════════════════════════════════════════════════════════╣");
        println!("║ DIRECT WEBSOCKET (baseline - should be working)               ║");
        println!("║   Total messages:     {:>8}                                  ║", self.direct.total_messages);
        println!("║   Trade messages:     {:>8}                                  ║", self.direct.trade_messages);
        println!("║   Individual trades:  {:>8}                                  ║", self.direct.total_trades);
        println!("║   Unique symbols:     {:>8}                                  ║", self.direct.symbols_seen.len());
        println!("╠════════════════════════════════════════════════════════════════╣");
        println!("║ AGORA SUBSCRIBER (testing your infrastructure)                ║");
        println!("║   Total trade updates:{:>8}                                  ║", agora.total_updates);
        println!("║   Unique symbols:     {:>8}                                  ║", agora.symbols_seen.len());
        println!("╠════════════════════════════════════════════════════════════════╣");

        // Analysis
        if self.direct.total_trades > 0 && agora.total_updates == 0 {
            println!("║ ⚠️  ISSUE DETECTED: Direct WS receiving trades, Agora is not  ║");
            println!("║     Problem is in the publisher → relay → subscriber chain    ║");
        } else if self.direct.total_trades > 0 && agora.total_updates > 0 {
            let ratio = agora.total_updates as f64 / self.direct.total_trades as f64;
            println!("║ ✓ Both systems receiving trades                                ║");
            println!("║   Agora/Direct ratio: {:.2}                                     ║", ratio);
            if ratio < 0.5 {
                println!("║   ⚠️  Agora receiving significantly fewer trades             ║");
            }
        } else if self.direct.total_trades == 0 {
            println!("║ ⚠️  No trades received on either system                        ║");
            println!("║     This might be a low-activity period                        ║");
        }

        println!("╠════════════════════════════════════════════════════════════════╣");
        println!("║ SYMBOL COMPARISON                                              ║");

        let direct_only: Vec<_> = self.direct.symbols_seen
            .difference(&agora.symbols_seen)
            .collect();
        let agora_only: Vec<_> = agora.symbols_seen
            .difference(&self.direct.symbols_seen)
            .collect();

        if !direct_only.is_empty() {
            println!("║ Symbols only in Direct WS: {:?}                               ",
                direct_only.iter().take(5).collect::<Vec<_>>());
        }
        if !agora_only.is_empty() {
            println!("║ Symbols only in Agora: {:?}                                   ",
                agora_only.iter().take(5).collect::<Vec<_>>());
        }
        if direct_only.is_empty() && agora_only.is_empty() && !self.direct.symbols_seen.is_empty() {
            println!("║ ✓ Same symbols in both systems                                 ║");
        }

        println!("╚════════════════════════════════════════════════════════════════╝\n");
    }
}

/// Hyperliquid WebSocket message wrapper
#[derive(Debug, Deserialize)]
struct ChannelMessage {
    channel: String,
    data: Option<serde_json::Value>,
}

/// Raw trade from Hyperliquid
#[derive(Debug, Deserialize)]
struct RawTrade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    tid: u64,
}

/// Subscription message for Hyperliquid WebSocket
#[derive(Debug, Serialize)]
struct Subscription {
    method: String,
    subscription: SubscriptionType,
}

#[derive(Debug, Serialize)]
struct SubscriptionType {
    #[serde(rename = "type")]
    sub_type: String,
    coin: String,
}

/// Task 1: Direct WebSocket monitoring (proven to work)
async fn monitor_direct_websocket(
    symbols: Vec<String>,
    stats: Arc<RwLock<ComparisonStats>>,
) -> OrError<()> {
    println!("🔌 [DIRECT] Connecting to Hyperliquid WebSocket...");

    let (ws_stream, _) = connect_async(WS_URL).await
        .map_err(|e| format!("Direct WS connection failed: {}", e))?;

    println!("✅ [DIRECT] Connected!");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to trades
    for symbol in &symbols {
        let subscription = Subscription {
            method: "subscribe".to_string(),
            subscription: SubscriptionType {
                sub_type: "trades".to_string(),
                coin: symbol.clone(),
            },
        };

        let msg = serde_json::to_string(&subscription)
            .map_err(|e| format!("Failed to serialize subscription: {}", e))?;
        write.send(Message::Text(msg.into())).await
            .map_err(|e| format!("Failed to send subscription: {}", e))?;
    }

    println!("📡 [DIRECT] Subscribed to {} symbols", symbols.len());

    // Message loop
    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(msg) = serde_json::from_str::<ChannelMessage>(&text) {
                    if msg.channel == "trades" {
                        if let Some(data) = msg.data {
                            if let Ok(trades) = serde_json::from_value::<Vec<RawTrade>>(data) {
                                let mut stats_guard = stats.write().await;
                                stats_guard.direct.total_messages += 1;
                                stats_guard.direct.trade_messages += 1;
                                stats_guard.direct.total_trades += trades.len() as u64;

                                for trade in &trades {
                                    stats_guard.direct.symbols_seen.insert(trade.coin.clone());

                                    // Print first few trades for visibility
                                    if stats_guard.direct.total_trades <= 10 {
                                        let side_icon = if trade.side == "B" { "🟢" } else { "🔴" };
                                        println!("  [DIRECT] {} {} {} @ {} (tid: {})",
                                            side_icon, trade.coin, trade.sz, trade.px, trade.tid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(ping_data))) => {
                write.send(Message::Pong(ping_data)).await.ok();
            }
            Some(Ok(Message::Close(_))) | None => {
                println!("🔌 [DIRECT] Connection closed");
                break;
            }
            Some(Err(e)) => {
                eprintln!("❌ [DIRECT] WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

/// Task 2: Monitor Agora subscribers for each symbol
async fn monitor_agora_subscribers(
    perp_universe: Vec<TradingSymbol>,
    metaserver_connection: ConnectionHandle,
    stats: Arc<RwLock<ComparisonStats>>,
) -> OrError<()> {
    println!("📥 [AGORA] Setting up subscribers for {} symbols...", perp_universe.len());

    // Create subscribers for each perpetual
    let mut subscribers = Vec::new();

    // Limit to first 5 for debugging
    let symbols_to_monitor: Vec<_> = perp_universe.iter().take(5).collect();

    for symbol in &symbols_to_monitor {
        let agora_path = format!(
            "{}/perp/last_trade/{}",
            HYPERLIQUID_AGORA_PREFIX,
            symbol.to_string()
        );

        println!("  Setting up subscriber for: {}", agora_path);

        match Subscriber::<AgorableOption<TradeUpdate>>::new(
            format!("debug_{}", symbol.to_string()),
            agora_path.clone(),
            metaserver_connection.clone(),
            AGORA_GATEWAY_PORT,
        ).await {
            Ok(subscriber) => {
                subscribers.push((symbol.clone(), subscriber, agora_path));
            }
            Err(e) => {
                eprintln!("⚠️  [AGORA] Failed to create subscriber for {}: {}", symbol, e);
            }
        }
    }

    if subscribers.is_empty() {
        return Err("No Agora subscribers could be created".to_string());
    }

    println!("✅ [AGORA] Created {} subscribers", subscribers.len());

    // Spawn tasks to monitor each subscriber
    let mut tasks = Vec::new();

    for (symbol, mut subscriber, path) in subscribers {
        let stats_clone = stats.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            loop {
                match subscriber.recv().await {
                    Ok(AgorableOption(Some(trade))) => {
                        let mut stats_guard = stats_clone.write().await;
                        stats_guard.agora.total_updates += 1;
                        stats_guard.agora.symbols_seen.insert(symbol_clone.to_string());

                        // Print first few trades for visibility
                        if stats_guard.agora.total_updates <= 10 {
                            let side_icon = if trade.is_buy { "🟢" } else { "🔴" };
                            println!("  [AGORA] {} {} {} @ {} (tid: {})",
                                side_icon, trade.symbol, trade.size, trade.price, trade.trade_id);
                        }
                    }
                    Ok(AgorableOption(None)) => {
                        // No data yet, continue
                    }
                    Err(e) => {
                        eprintln!("❌ [AGORA] Subscriber error for {}: {}", path, e);
                        break;
                    }
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all subscriber tasks (they run indefinitely)
    for task in tasks {
        task.await.ok();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    Hyperliquid Trade Debug - Infrastructure Comparison        ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("🔍 Step 1: Fetching universe from UniverseManager...");

    // Initialize universe manager
    let universe_manager = Arc::new(
        UniverseManager::new(Duration::from_secs(300))
            .await
            .map_err(|e| format!("Failed to create UniverseManager: {}", e))?
    );

    let perp_universe = universe_manager.perp_universe()
        .await
        .map_err(|e| format!("Failed to get perp universe: {}", e))?;

    println!("✅ UniverseManager loaded {} perpetuals", perp_universe.len());

    // Get Hyperliquid symbols for direct WS (need to translate)
    let symbol_map = universe_manager.symbol_map().await;
    let mut hyperliquid_symbols = Vec::new();

    for normalized in perp_universe.iter().take(5) {
        if let Some(hyperliquid) = symbol_map.get_by_left(normalized) {
            hyperliquid_symbols.push(hyperliquid.to_string());
            println!("  {} (normalized) → {} (Hyperliquid)", normalized, hyperliquid);
        }
    }

    if hyperliquid_symbols.is_empty() {
        return Err("No symbols found to monitor".into());
    }

    // Connect to metaserver for Agora subscribers
    println!("\n🔗 Step 2: Connecting to Agora metaserver...");
    let metaserver_connection = ConnectionHandle::new_local(AGORA_METASERVER_DEFAULT_PORT)
        .map_err(|e| format!("Failed to connect to metaserver: {}", e))?;
    println!("✅ Connected to metaserver");

    // Initialize stats
    let stats = Arc::new(RwLock::new(ComparisonStats::new()));

    println!("\n🚀 Step 3: Starting monitoring tasks...\n");
    println!("════════════════════════════════════════════════════════════════");
    println!("Monitoring {} symbols for 60 seconds...", hyperliquid_symbols.len());
    println!("Press Ctrl+C to stop early\n");

    // Spawn both monitoring tasks
    let direct_stats = stats.clone();
    let direct_symbols = hyperliquid_symbols.clone();
    let direct_task = tokio::spawn(async move {
        if let Err(e) = monitor_direct_websocket(direct_symbols, direct_stats).await {
            eprintln!("Direct WebSocket task error: {}", e);
        }
    });

    let agora_stats = stats.clone();
    let agora_task = tokio::spawn(async move {
        if let Err(e) = monitor_agora_subscribers(perp_universe, metaserver_connection, agora_stats).await {
            eprintln!("Agora subscriber task error: {}", e);
        }
    });

    // Run for 60 seconds or until Ctrl+C
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(60)) => {
            println!("\n⏰ 60 seconds elapsed");
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\n\n⚠️  Interrupted by user");
        }
    }

    // Stop tasks
    direct_task.abort();
    agora_task.abort();

    // Print comparison
    println!("\n════════════════════════════════════════════════════════════════");
    let stats_guard = stats.read().await;
    stats_guard.print_comparison().await;

    println!("\n💡 DIAGNOSTIC RECOMMENDATIONS:");
    println!("════════════════════════════════════════════════════════════════");

    let agora = stats_guard.agora.read().await;

    if stats_guard.direct.total_trades > 0 && agora.total_updates == 0 {
        println!("
❌ TRADES ARE BEING LOST IN THE PIPELINE

Possible issues:
1. Publisher not running or crashed
   → Check if HyperliquidPublisher is active
   → Look for error messages in publisher logs

2. WebSocket worker not parsing trades correctly
   → Check argus/src/crypto/hyperliquid/trades.rs:68-85
   → It only processes first trade in array (BUG!)

3. Relay/Publisher path mismatch
   → Verify paths: argus/tmp/hyperliquid/perp_*/last_trade/*
   → Subscribers expect: {}/perp/last_trade/*

4. Symbol mapping issue
   → Hyperliquid symbols (BTC) vs normalized (BTC_PERP)
   → Check translation in webstream worker

Next steps:
→ Run: cargo run --bin hyperliquid-publisher
→ Then run this debug tool again
→ Check metaserver logs for path registration
        ");
    } else if stats_guard.direct.total_trades > 0 && agora.total_updates > 0 {
        let ratio = agora.total_updates as f64 / stats_guard.direct.total_trades as f64;
        if ratio < 0.5 {
            println!("
⚠️  PARTIAL TRADE LOSS DETECTED (receiving {}% of trades)

Likely cause: trades.rs only processes FIRST trade per message
→ Hyperliquid sends arrays of trades
→ Current code: let raw = &trades[0];  // ONLY FIRST!
→ Fix: Process ALL trades in the array

File to fix: argus/src/crypto/hyperliquid/trades.rs:68-85
            ", (ratio * 100.0) as u32);
        } else {
            println!("✅ Infrastructure appears to be working correctly!");
        }
    } else if stats_guard.direct.total_trades == 0 {
        println!("
⚠️  NO TRADES RECEIVED

This might be normal during low activity periods.
Try:
→ Running during high-activity hours
→ Monitoring more symbols
→ Checking if Hyperliquid API is accessible
        ");
    }

    Ok(())
}
