use agora::{AgorableOption, ConnectionHandle};
use argus::constants::{AGORA_METASERVER_DEFAULT_PORT, ARGUS_DATA_PATH};
use argus::crypto::hyperliquid::BboUpdate;
use argus::scribe::AgoraDirScribe;
use std::io::{self, Write};
use std::time::Duration;

type T = BboUpdate;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  Argus File Scribe");
    println!("========================================");
    println!();
    println!("⚠️  REMINDER: Start Agora MetaServer and gateway first!");
    println!(
        "   Run: cd ../agora && cargo run --bin metaserver -- -p {}",
        AGORA_METASERVER_DEFAULT_PORT
    );
    println!();

    // Get port (default to localhost)
    print!(
        "Enter metaserver port (default: {}): ",
        AGORA_METASERVER_DEFAULT_PORT
    );
    io::stdout().flush()?;
    let mut port_input = String::new();
    io::stdin().read_line(&mut port_input)?;
    let port: u16 = if port_input.trim().is_empty() {
        AGORA_METASERVER_DEFAULT_PORT
    } else {
        port_input
            .trim()
            .parse()
            .map_err(|_| "Invalid port number")?
    };

    // Get agora_prefix
    print!("Enter agora prefix (e.g., argus/hyperliquid/bbo. Only BBOs are supported): ");
    io::stdout().flush()?;
    let mut agora_prefix = String::new();
    io::stdin().read_line(&mut agora_prefix)?;
    let agora_prefix = agora_prefix.trim().to_string();

    // Get flush_duration
    print!("Enter flush duration in seconds (e.g., 60): ");
    io::stdout().flush()?;
    let mut flush_duration_input = String::new();
    io::stdin().read_line(&mut flush_duration_input)?;
    let flush_duration_secs: u64 = flush_duration_input
        .trim()
        .parse()
        .map_err(|_| "Invalid flush duration")?;
    let flush_duration = Duration::from_secs(flush_duration_secs);

    // Get output_dir
    let default_output_dir = format!("{}/tmp", ARGUS_DATA_PATH);
    print!(
        "Enter output directory (default., {}): ",
        default_output_dir
    );
    io::stdout().flush()?;
    let mut output_dir = String::new();
    io::stdin().read_line(&mut output_dir)?;
    let output_dir = if output_dir.trim().is_empty() {
        default_output_dir
    } else {
        output_dir.trim().parse().map_err(|_| "invalid string")?
    };

    println!();
    println!("Configuration:");
    println!("  Metaserver port: {}", port);
    println!("  Agora prefix: {}", agora_prefix);
    println!("  Flush duration: {} seconds", flush_duration_secs);
    println!("  Output directory: {}", output_dir);
    println!();

    // Connect to metaserver
    let metaserver_connection = ConnectionHandle::new_local(port)?;
    println!("✅ Connected to metaserver at localhost:{}", port);

    // Create the scribe
    // Note: Publishers use AgorableOption<TradeUpdate>, not TradeUpdate directly
    let scribe = AgoraDirScribe::<AgorableOption<T>>::new(
        &agora_prefix,
        metaserver_connection,
        flush_duration,
        &output_dir,
    )
    .await?;

    println!();
    println!("========================================");
    println!("✅ File scribe initialized successfully!");
    println!("========================================");
    println!();
    println!("Scribe is now collecting data from Agora and writing to disk...");
    println!("Active scribes: {}", scribe.count());
    println!("Press Ctrl+C to stop.");
    println!();

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!();
    println!("🛑 Shutting down scribe...");

    // Gracefully shutdown with final flush
    scribe.shutdown().await?;

    println!("Shutdown complete.");

    Ok(())
}
