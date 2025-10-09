use agora::ConnectionHandle;
use argus::constants::{
    AGORA_GATEWAY_PORT, AGORA_METASERVER_DEFAULT_PORT, HYPERLIQUID_AGORA_PREFIX,
};
use argus::crypto::hyperliquid::HyperliquidScribe;
use clap::Parser;
use std::time::Duration;

#[derive(Parser)]
#[command(
    version,
    about = "Hyperliquid Scribe - writes all Hyperliquid market data to parquet files",
    long_about = None
)]
struct Args {
    #[arg(short, long, default_value_t = AGORA_METASERVER_DEFAULT_PORT)]
    port: u16,

    #[arg(short, long, default_value = HYPERLIQUID_AGORA_PREFIX)]
    agora_path: String,

    #[arg(short = 'H', long, default_value = "localhost")]
    metaserver_host: String,

    #[arg(short = 'g', long, default_value_t = AGORA_GATEWAY_PORT)]
    local_gateway_port: u16,

    #[arg(short = 'o', long, default_value = "/tmp/hyperliquid")]
    output_dir: String,

    #[arg(short = 'f', long, default_value_t = 30)]
    flush_interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("========================================");
    println!("  Hyperliquid Scribe");
    println!("========================================");
    println!();
    println!("Configuration:");
    println!("  Metaserver: {}:{}", args.metaserver_host, args.port);
    println!("  Local gateway port: {}", args.local_gateway_port);
    println!("  Agora path: {}", args.agora_path);
    println!("  Output directory: {}", args.output_dir);
    println!("  Flush interval: {}s", args.flush_interval);
    println!();
    println!("REMINDER: Ensure the following are running:");
    println!("  1. Agora MetaServer (cargo run --bin metaserver)");
    println!("  2. Agora Gateway (cargo run --bin gateway)");
    println!("  3. Hyperliquid Publisher (cargo run --bin hyperliquid)");
    println!();

    // Connect to metaserver
    let metaserver_connection = if args.metaserver_host == "localhost" {
        ConnectionHandle::new_local(args.port)?
    } else {
        let addr = args.metaserver_host.parse()?;
        ConnectionHandle::new(addr, args.port)
    };

    println!("Initializing scribe...");

    let flush_duration = Duration::from_secs(args.flush_interval);
    let scribe = HyperliquidScribe::new(
        &args.agora_path,
        &args.output_dir,
        metaserver_connection,
        flush_duration,
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    println!();
    println!("========================================");
    println!("Scribe is now running!");
    println!("========================================");
    println!();
    println!("Writing market data to: {}", args.output_dir);
    println!("  Structure: {{output_dir}}/{{spot|perp}}/{{data_type}}/{{symbol}}_{{timestamp}}.pq");
    println!();
    println!("Data types being recorded:");
    println!("  ✓ Trades (last_trade)");
    println!("  ✓ Best Bid/Offer (bbo)");
    println!("  ✓ L2 Orderbook (orderbook)");
    println!("  ✓ Spot Context (spot_context)");
    println!("  ✓ Perp Context (perp_context)");
    println!();
    println!("Files are flushed every {} seconds", args.flush_interval);
    println!();
    println!("TIP: Use the Archiver to organize these files into hive-partitioned structure:");
    println!("  cargo run --bin archiver");
    println!();
    println!("Press Ctrl+C to stop and flush remaining data.");
    println!();

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!();
    println!("Received shutdown signal. Flushing data...");

    // Graceful shutdown with final flush
    scribe.shutdown().await.map_err(|e| anyhow::anyhow!(e))?;

    println!();
    println!("Shutdown complete. All data flushed to disk.");

    Ok(())
}
