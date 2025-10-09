use agora::ConnectionHandle;
use argus::constants::{
    AGORA_GATEWAY_PORT, AGORA_METASERVER_DEFAULT_PORT, HYPERLIQUID_AGORA_PREFIX,
};
use argus::crypto::hyperliquid::HyperliquidPublisher;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("  Hyperliquid Universal Publisher");
    println!("========================================");
    println!();
    println!("REMINDER: Start Agora MetaServer and gateway first!");
    println!(
        "   Run: cd ../agora && cargo run --bin metaserver -- -p {}",
        AGORA_METASERVER_DEFAULT_PORT
    );
    println!();

    let metaserver_connection = ConnectionHandle::new_local(AGORA_METASERVER_DEFAULT_PORT)?;
    println!("Initializing Hyperliquid Publisher...");
    println!("  - Universe update interval: 60 seconds");
    println!("  - Change check interval: 5 seconds");
    println!();

    let agora_path = HYPERLIQUID_AGORA_PREFIX;

    // Create publisher that automatically manages all Hyperliquid symbols
    let _publisher = HyperliquidPublisher::new(
        agora_path,
        metaserver_connection,
        AGORA_GATEWAY_PORT,
        Duration::from_secs(60), // Check Hyperliquid API for universe changes every 60s
        Duration::from_secs(5),  // Check for detected universe changes every 5s
    )
    .await?;

    println!("========================================");
    println!("Publisher initialized successfully!");
    println!("========================================");
    println!();
    println!("Publisher is now streaming ALL Hyperliquid data:");
    println!("  - Perpetuals markets (all active symbols)");
    println!("  - Spot markets (all active symbols)");
    println!();
    println!("Data types published:");
    println!(
        "  - Trades → {}/{{perp|spot}}/last_trade/{{symbol}}",
        agora_path
    );
    println!("  - BBO → {}/{{perp|spot}}/bbo/{{symbol}}", agora_path);
    println!(
        "  - L2 Orderbook → {}/{{perp|spot}}/orderbook/{{symbol}}",
        agora_path
    );
    println!(
        "  - Asset Context → {}/{{perp|spot}}/{{perp_context|spot_context}}/{{symbol}}",
        agora_path
    );
    println!();
    println!("The publisher automatically handles:");
    println!("  ✓ New symbols being listed");
    println!("  ✓ Symbols being delisted");
    println!("  ✓ Seamless transitions without subscriber disruption");
    println!();
    println!("Press Ctrl+C to stop.");
    println!();

    // Keep the publisher alive
    tokio::signal::ctrl_c().await?;

    println!();
    println!("Shutting down publisher...");
    println!("Shutdown complete.");

    Ok(())
}
