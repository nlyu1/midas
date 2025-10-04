use argus::constants::AGORA_METASERVER_DEFAULT_PORT;
use argus::crypto::binance::BinanceWebstreamSymbol;
use argus::types::TradingSymbol;
use std::net::Ipv6Addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  Binance Webstream Worker");
    println!("========================================");
    println!();
    println!("⚠️  REMINDER: Start Agora MetaServer first!");
    println!(
        "   Run: cd ../agora && cargo run --bin metaserver -- -p {}",
        AGORA_METASERVER_DEFAULT_PORT
    );
    println!();

    // Hardcoded tickers from binance_stream.rs
    let ticker_strs = ["solusdt", "ethusdt", "btcusdt", "dogeusdt", "xrpusdt"];
    let symbols: Vec<TradingSymbol> = ticker_strs
        .iter()
        .map(|s| TradingSymbol::from_str(s).expect("Valid symbol"))
        .collect();

    println!("📊 Trading Symbols: {}", ticker_strs.join(", "));
    println!();

    let metaserver_addr = Ipv6Addr::LOCALHOST;
    let metaserver_port = AGORA_METASERVER_DEFAULT_PORT;
    let agora_prefix = "argus/binance".to_string();

    // Initialize all workers together
    let workers =
        BinanceWebstreamSymbol::new(metaserver_addr, metaserver_port, agora_prefix, &symbols)
            .await?;

    println!("========================================");
    println!("✅ All workers initialized successfully!");
    println!("========================================");
    println!();
    println!("Workers are now streaming data from Binance...");
    println!("Press Ctrl+C to stop.");
    println!();

    // Keep the workers alive
    tokio::signal::ctrl_c().await?;

    println!();
    println!("🛑 Shutting down workers...");

    // Workers will be dropped here, cleaning up resources
    drop(workers);

    println!("✅ Shutdown complete.");

    Ok(())
}
