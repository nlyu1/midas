#!/usr/bin/env rust
//! Hyperliquid Trade Stream Debug Tool
//!
//! This is a minimal WebSocket client that connects directly to Hyperliquid
//! to debug trade streaming issues. It bypasses all Agora infrastructure
//! to isolate WebSocket connectivity and message parsing.
//!
//! Usage:
//!   cargo run --bin hyperliquid-trade
//!
//! What this does:
//! 1. Connects to Hyperliquid WebSocket API
//! 2. Subscribes to trades for top perpetuals by volume
//! 3. Prints all raw messages received
//! 4. Parses and displays trade data
//! 5. Keeps detailed statistics
//!
//! This helps identify if the issue is:
//! - WebSocket connection problems
//! - Subscription format issues
//! - Message parsing problems
//! - Or something in the Agora publishing layer

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const REST_URL: &str = "https://api.hyperliquid.xyz/info";

/// Stats tracker for monitoring stream health
#[derive(Default)]
struct StreamStats {
    total_messages: u64,
    subscription_responses: u64,
    pong_responses: u64,
    trade_messages: u64,
    total_trades: u64,
    bbo_messages: u64,
    context_messages: u64,
    unknown_messages: u64,
    parse_errors: u64,
    start_time: Option<Instant>,
}

impl StreamStats {
    fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.map(|s| s.elapsed()).unwrap_or_default();
        println!("\n╔════════════════════════════════════════════════════════════════╗");
        println!("║                      STREAM STATISTICS                         ║");
        println!("╠════════════════════════════════════════════════════════════════╣");
        println!(
            "║ Runtime:                 {:>8.1} seconds                      ║",
            elapsed.as_secs_f64()
        );
        println!(
            "║ Total messages:          {:>8}                               ║",
            self.total_messages
        );
        println!(
            "║ Subscription responses:  {:>8}                               ║",
            self.subscription_responses
        );
        println!(
            "║ Pong responses:          {:>8}                               ║",
            self.pong_responses
        );
        println!(
            "║ Trade messages:          {:>8}                               ║",
            self.trade_messages
        );
        println!(
            "║ Individual trades:       {:>8}                               ║",
            self.total_trades
        );
        println!(
            "║ BBO messages:            {:>8}                               ║",
            self.bbo_messages
        );
        println!(
            "║ Context messages:        {:>8}                               ║",
            self.context_messages
        );
        println!(
            "║ Unknown messages:        {:>8}                               ║",
            self.unknown_messages
        );
        println!(
            "║ Parse errors:            {:>8}                               ║",
            self.parse_errors
        );
        println!("╚════════════════════════════════════════════════════════════════╝\n");

        if self.trade_messages > 0 {
            let avg_trades_per_msg = self.total_trades as f64 / self.trade_messages as f64;
            println!("📊 Average trades per message: {:.2}", avg_trades_per_msg);
        }

        if self.total_trades > 0 {
            let trades_per_sec = self.total_trades as f64 / elapsed.as_secs_f64();
            println!("📈 Trades per second: {:.2}", trades_per_sec);
        }
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
    side: String, // "B" for buy, "A" for sell
    px: String,
    sz: String,
    time: u64, // milliseconds
    tid: u64,
}

/// Parsed trade for display
#[derive(Debug)]
struct Trade {
    coin: String,
    side: String,
    price: f64,
    size: f64,
    time: DateTime<Utc>,
    trade_id: u64,
}

impl Trade {
    fn from_raw(raw: RawTrade) -> Result<Self, String> {
        let price = raw
            .px
            .parse::<f64>()
            .map_err(|e| format!("Failed to parse price '{}': {}", raw.px, e))?;
        let size = raw
            .sz
            .parse::<f64>()
            .map_err(|e| format!("Failed to parse size '{}': {}", raw.sz, e))?;
        let time = DateTime::from_timestamp_millis(raw.time as i64)
            .ok_or_else(|| format!("Invalid timestamp: {}", raw.time))?;

        Ok(Self {
            coin: raw.coin,
            side: raw.side,
            price,
            size,
            time,
            trade_id: raw.tid,
        })
    }

    fn display(&self) {
        let side_icon = if self.side == "B" { "🟢" } else { "🔴" };
        let timestamp = self.time.format("%H:%M:%S%.3f");
        println!(
            "{} [{}] {} {:>6} {:>12.4} @ ${:<12.2} (tid: {})",
            side_icon,
            timestamp,
            if self.side == "B" { "BUY " } else { "SELL" },
            self.coin,
            self.size,
            self.price,
            self.trade_id
        );
    }
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

/// Fetch top N perpetuals by volume from REST API
async fn fetch_top_perps(n: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!(
        "🔍 Fetching top {} perpetuals by volume from REST API...",
        n
    );

    let client = reqwest::Client::new();
    let request = serde_json::json!({
        "type": "metaAndAssetCtxs"
    });

    let response = client.post(REST_URL).json(&request).send().await?;

    if !response.status().is_success() {
        return Err(format!("REST API error: {}", response.status()).into());
    }

    let json: serde_json::Value = response.json().await?;

    // Parse response: [metadata, asset_contexts]
    let metadata = json.get(0).ok_or("Missing metadata in response")?;
    let asset_contexts = json
        .get(1)
        .ok_or("Missing asset contexts in response")?
        .as_array()
        .ok_or("Asset contexts is not an array")?;

    let universe = metadata
        .get("universe")
        .ok_or("Missing universe in metadata")?
        .as_array()
        .ok_or("Universe is not an array")?;

    // Build list of (symbol, volume) for non-delisted assets
    let mut perps_with_volume: Vec<(String, f64)> = Vec::new();

    for (i, asset) in universe.iter().enumerate() {
        let is_delisted = asset
            .get("isDelisted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_delisted {
            continue;
        }

        let name = asset
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or("Missing asset name")?
            .to_string();

        if i < asset_contexts.len() {
            let volume = asset_contexts[i]
                .get("dayNtlVlm")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            if volume > 0.0 {
                perps_with_volume.push((name, volume));
            }
        }
    }

    // Sort by volume descending
    perps_with_volume.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // Take top N
    let top_symbols: Vec<String> = perps_with_volume
        .iter()
        .take(n)
        .map(|(name, _)| name.clone())
        .collect();

    // Print summary
    println!("\n📈 Top {} perpetuals by 24h volume:", n);
    for (i, (name, volume)) in perps_with_volume.iter().take(n).enumerate() {
        println!("  {}. {} - ${}", i + 1, name, volume);
    }
    println!();

    Ok(top_symbols)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║       Hyperliquid Trade Stream Debug Tool                     ║");
    println!("║       Direct WebSocket Connection (No Agora)                   ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    // Fetch top perpetuals
    let symbols = fetch_top_perps(5).await?;

    if symbols.is_empty() {
        eprintln!("❌ No active perpetuals found!");
        return Ok(());
    }

    println!("🔌 Connecting to Hyperliquid WebSocket: {}", WS_URL);

    let (ws_stream, _) = connect_async(WS_URL).await?;
    println!("✅ Connected successfully!\n");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to trades for each symbol
    println!("📡 Subscribing to trades for {} symbols...", symbols.len());
    for symbol in &symbols {
        let subscription = Subscription {
            method: "subscribe".to_string(),
            subscription: SubscriptionType {
                sub_type: "trades".to_string(),
                coin: symbol.clone(),
            },
        };

        let msg = serde_json::to_string(&subscription)?;
        write.send(Message::Text(msg.into())).await?;
        println!("  ✓ Subscribed to trades for {}", symbol);
    }
    println!("\n🎧 Listening for trades (Ctrl+C to stop)...\n");
    println!("{}", "═".repeat(80));

    let mut stats = StreamStats::new();
    let start_time = Instant::now();
    let mut last_summary = Instant::now();

    // Spawn heartbeat task
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel::<Message>(10);
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let ping_msg = serde_json::json!({"method": "ping"});
            if let Err(_) = ping_tx
                .send(Message::Text(ping_msg.to_string().into()))
                .await
            {
                break;
            }
        }
    });

    // Message processing loop
    loop {
        tokio::select! {
            // Handle incoming messages
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        stats.total_messages += 1;

                        // Try to parse as ChannelMessage
                        match serde_json::from_str::<ChannelMessage>(&text) {
                            Ok(msg) => {
                                match msg.channel.as_str() {
                                    "subscriptionResponse" => {
                                        stats.subscription_responses += 1;
                                        println!("✓ Subscription confirmed");
                                    }
                                    "pong" => {
                                        stats.pong_responses += 1;
                                        // Quiet - don't spam console with pongs
                                    }
                                    "trades" => {
                                        stats.trade_messages += 1;

                                        if let Some(data) = msg.data {
                                            // Parse trades array
                                            match serde_json::from_value::<Vec<RawTrade>>(data.clone()) {
                                                Ok(raw_trades) => {
                                                    stats.total_trades += raw_trades.len() as u64;

                                                    for raw_trade in raw_trades {
                                                        match Trade::from_raw(raw_trade) {
                                                            Ok(trade) => trade.display(),
                                                            Err(e) => {
                                                                stats.parse_errors += 1;
                                                                eprintln!("⚠️  Trade parse error: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    stats.parse_errors += 1;
                                                    eprintln!("⚠️  Failed to parse trades array: {}", e);
                                                    eprintln!("   Raw data: {}", data);
                                                }
                                            }
                                        } else {
                                            eprintln!("⚠️  Trade message missing data field");
                                        }
                                    }
                                    "l2Book" | "bbo" => {
                                        stats.bbo_messages += 1;
                                        // Quiet - we're focused on trades
                                    }
                                    "activeAssetCtx" => {
                                        stats.context_messages += 1;
                                        // Quiet - we're focused on trades
                                    }
                                    other => {
                                        stats.unknown_messages += 1;
                                        println!("❓ Unknown channel: {}", other);
                                    }
                                }
                            }
                            Err(e) => {
                                stats.parse_errors += 1;
                                eprintln!("⚠️  JSON parse error: {}", e);
                                eprintln!("   Raw message: {}", text);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(ping_data))) => {
                        // Respond to server ping
                        write.send(Message::Pong(ping_data)).await?;
                    }
                    Some(Ok(Message::Close(_))) => {
                        println!("\n🔌 Server closed connection");
                        break;
                    }
                    Some(Ok(_)) => {
                        // Other message types (binary, pong, etc.)
                    }
                    Some(Err(e)) => {
                        eprintln!("❌ WebSocket error: {}", e);
                        break;
                    }
                    None => {
                        println!("\n🔌 Connection closed");
                        break;
                    }
                }

                // Print summary every 30 seconds
                if last_summary.elapsed().as_secs() >= 30 {
                    stats.print_summary();
                    last_summary = Instant::now();
                }
            }

            // Handle heartbeat pings
            ping_msg = ping_rx.recv() => {
                if let Some(msg) = ping_msg {
                    write.send(msg).await?;
                } else {
                    break;
                }
            }

            // Handle Ctrl+C
            _ = tokio::signal::ctrl_c() => {
                println!("\n\n⚠️  Interrupted by user");
                break;
            }
        }
    }

    // Cleanup
    heartbeat_task.abort();

    // Print final statistics
    println!("\n{}", "═".repeat(80));
    stats.print_summary();

    let elapsed = start_time.elapsed();
    println!(
        "✅ Debug session completed in {:.1} seconds",
        elapsed.as_secs_f64()
    );

    Ok(())
}
