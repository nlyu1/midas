use argus::constants::{ARGUS_DATA_PATH, HYPERLIQUID_DATA_SUFFIX};
use argus::crypto::hyperliquid::HyperliquidArchiver;
use clap::Parser;

#[derive(Parser)]
#[command(
    version,
    about = "Hyperliquid Archiver - organizes temporary parquet files into hive-partitioned structure",
    long_about = None
)]
struct Args {
    #[arg(
        short = 't',
        long,
        default_value = "/tmp/hyperliquid",
        help = "Temporary directory where HyperliquidScribe writes files"
    )]
    tmp_dir: String,

    #[arg(
        short = 'o',
        long,
        help = "Output directory for organized data (default: $ARGUS_DATA_PATH/$HYPERLIQUID_DATA_SUFFIX)"
    )]
    output_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Determine output directory
    let output_dir = args.output_dir.unwrap_or_else(|| {
        format!("{}/{}", ARGUS_DATA_PATH, HYPERLIQUID_DATA_SUFFIX)
    });

    println!();
    println!("Configuration:");
    println!("  Temporary dir: {}", args.tmp_dir);
    println!("  Output dir: {}", output_dir);
    println!();

    // Initialize archiver
    let archiver = HyperliquidArchiver::new(&args.tmp_dir, &output_dir)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    println!("Archiver is now running!");
    println!();
    println!("The archiver monitors temporary files and organizes them into:");
    println!("  {{output_dir}}/{{spot|perp}}/{{data_type}}/date={{date}}/symbol={{symbol}}/data.parquet");
    println!();
    println!("Spot data types: last_trade, bbo, orderbook, spot_context");
    println!("Perp data types: last_trade, bbo, orderbook, perp_context");
    println!();
    println!("Files are scanned every 10 seconds.");
    println!("Old files are archived when newer data is detected for the same symbol.");
    println!();
    println!("Press Ctrl+C to stop.");
    println!();

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!();
    println!("Received shutdown signal. Shutting down archiver...");

    // Graceful shutdown
    archiver.shutdown().await.map_err(|e| anyhow::anyhow!(e))?;

    println!();
    println!("Shutdown complete.");

    Ok(())
}
