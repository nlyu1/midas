use super::{BboUpdate, OrderbookSnapshot, PerpAssetContext, SpotAssetContext, TradeUpdate};
use crate::{AgoraDirScribe, Archiver};
use agora::utils::OrError;
use agora::{AgorableOption, ConnectionHandle};
use anyhow::Context;
use std::time::Duration;

/// Manages scribing of all Hyperliquid market data to disk
///
/// Subscegex-ibes to all data streams published by HyperliquidPublisher and writes them
/// to temporary parquet files. These files should then be organized by the Archiver.
///
/// # Data Structure
/// Writes to: `{output_dir}/{data_type}/{symbol}_{timestamp}.pq`
/// Where data_type is: last_trade, bbo, orderbook, spot_context, perp_context
pub struct HyperliquidScribe {
    // Spot market scribes
    spot_trade_scribe: AgoraDirScribe<AgorableOption<TradeUpdate>>,
    spot_bbo_scribe: AgoraDirScribe<AgorableOption<BboUpdate>>,
    spot_orderbook_scribe: AgoraDirScribe<AgorableOption<OrderbookSnapshot>>,
    spot_context_scribe: AgoraDirScribe<AgorableOption<SpotAssetContext>>,

    // Perp market scribes
    perp_trade_scribe: AgoraDirScribe<AgorableOption<TradeUpdate>>,
    perp_bbo_scribe: AgoraDirScribe<AgorableOption<BboUpdate>>,
    perp_orderbook_scribe: AgoraDirScribe<AgorableOption<OrderbookSnapshot>>,
    perp_context_scribe: AgoraDirScribe<AgorableOption<PerpAssetContext>>,
}

impl HyperliquidScribe {
    /// Creates a new HyperliquidScribe that writes all Hyperliquid data to disk
    ///
    /// # Arguments
    /// * `agora_path` - Base Agora path where HyperliquidPublisher publishes (e.g., "argus/hyperliquid")
    /// * `output_dir` - Base filesystem directory for temporary parquet files (e.g., "/tmp/hyperliquid")
    /// * `metaserver_connection` - Connection to the Agora metaserver
    /// * `flush_duration` - How often to flush accumulated data to disk
    ///
    /// # Returns
    /// A `HyperliquidScribe` that continuously writes market data to parquet files
    pub async fn new(
        agora_path: &str,
        output_dir: &str,
        metaserver_connection: ConnectionHandle,
        flush_duration: Duration,
    ) -> OrError<Self> {
        println!("Initializing HyperliquidScribe...");
        println!("  Agora path: {}", agora_path);
        println!("  Output dir: {}", output_dir);
        println!("  Flush interval: {:?}", flush_duration);

        // Create output directories
        std::fs::create_dir_all(output_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create output directory {}: {}", output_dir, e))?;

        let data_types = vec![
            "last_trade",
            "bbo",
            "orderbook",
            "spot_context",
            "perp_context",
        ];

        for data_type in &data_types {
            let spot_dir = format!("{}/spot/{}", output_dir, data_type);
            let perp_dir = format!("{}/perp/{}", output_dir, data_type);
            std::fs::create_dir_all(&spot_dir)
                .context("Failed to create spot directory")?;
            std::fs::create_dir_all(&perp_dir)
                .context("Failed to create perp directory")?;
        }

        println!("  Created output directory structure");

        // Initialize spot scribes
        println!("\nInitializing spot market scribes...");
        let spot_trade_scribe = AgoraDirScribe::new(
            &format!("{}/spot/last_trade", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/spot/last_trade", output_dir),
        )
        .await?;
        println!("  ✓ Spot trades: {} symbols", spot_trade_scribe.count());

        let spot_bbo_scribe = AgoraDirScribe::new(
            &format!("{}/spot/bbo", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/spot/bbo", output_dir),
        )
        .await?;
        println!("  ✓ Spot BBO: {} symbols", spot_bbo_scribe.count());

        let spot_orderbook_scribe = AgoraDirScribe::new(
            &format!("{}/spot/orderbook", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/spot/orderbook", output_dir),
        )
        .await?;
        println!(
            "  ✓ Spot orderbook: {} symbols",
            spot_orderbook_scribe.count()
        );

        let spot_context_scribe = AgoraDirScribe::new(
            &format!("{}/spot/spot_context", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/spot/spot_context", output_dir),
        )
        .await?;
        println!("  ✓ Spot context: {} symbols", spot_context_scribe.count());

        // Initialize perp scribes
        println!("\nInitializing perp market scribes...");
        let perp_trade_scribe = AgoraDirScribe::new(
            &format!("{}/perp/last_trade", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/perp/last_trade", output_dir),
        )
        .await?;
        println!("  ✓ Perp trades: {} symbols", perp_trade_scribe.count());

        let perp_bbo_scribe = AgoraDirScribe::new(
            &format!("{}/perp/bbo", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/perp/bbo", output_dir),
        )
        .await?;
        println!("  ✓ Perp BBO: {} symbols", perp_bbo_scribe.count());

        let perp_orderbook_scribe = AgoraDirScribe::new(
            &format!("{}/perp/orderbook", agora_path),
            metaserver_connection.clone(),
            flush_duration,
            &format!("{}/perp/orderbook", output_dir),
        )
        .await?;
        println!(
            "  ✓ Perp orderbook: {} symbols",
            perp_orderbook_scribe.count()
        );

        let perp_context_scribe = AgoraDirScribe::new(
            &format!("{}/perp/perp_context", agora_path),
            metaserver_connection,
            flush_duration,
            &format!("{}/perp/perp_context", output_dir),
        )
        .await?;
        println!("  ✓ Perp context: {} symbols", perp_context_scribe.count());

        println!("\n✅ HyperliquidScribe initialized successfully!");

        Ok(Self {
            spot_trade_scribe,
            spot_bbo_scribe,
            spot_orderbook_scribe,
            spot_context_scribe,
            perp_trade_scribe,
            perp_bbo_scribe,
            perp_orderbook_scribe,
            perp_context_scribe,
        })
    }

    /// Gracefully shutdown all scribes, flushing remaining data to disk
    pub async fn shutdown(self) -> OrError<()> {
        println!("\nShutting down HyperliquidScribe...");

        println!("  Flushing spot trades...");
        self.spot_trade_scribe.shutdown().await?;
        println!("  Flushing spot BBO...");
        self.spot_bbo_scribe.shutdown().await?;
        println!("  Flushing spot orderbook...");
        self.spot_orderbook_scribe.shutdown().await?;
        println!("  Flushing spot context...");
        self.spot_context_scribe.shutdown().await?;

        println!("  Flushing perp trades...");
        self.perp_trade_scribe.shutdown().await?;
        println!("  Flushing perp BBO...");
        self.perp_bbo_scribe.shutdown().await?;
        println!("  Flushing perp orderbook...");
        self.perp_orderbook_scribe.shutdown().await?;
        println!("  Flushing perp context...");
        self.perp_context_scribe.shutdown().await?;

        println!("✅ HyperliquidScribe shutdown complete");
        Ok(())
    }
}

/// Manages archiving of all Hyperliquid temporary parquet files
///
/// Organizes temporary files written by HyperliquidScribe into hive-partitioned structure.
/// Manages separate archivers for spot and perp markets.
///
/// # Input Structure (from HyperliquidScribe)
/// Reads from: `{tmp_dir}/{spot|perp}/{data_type}/{symbol}_{timestamp}.pq`
///
/// # Output Structure (hive-partitioned)
/// Writes to: `{output_dir}/{spot|perp}/{data_type}/date={date}/symbol={symbol}/data.parquet`
pub struct HyperliquidArchiver {
    spot_archiver: Archiver,
    perp_archiver: Archiver,
}

impl HyperliquidArchiver {
    /// Creates a new HyperliquidArchiver that organizes Hyperliquid data files
    ///
    /// # Arguments
    /// * `tmp_dir` - Base temporary directory where HyperliquidScribe writes (e.g., "/tmp/hyperliquid")
    /// * `output_dir` - Base directory for organized data (e.g., "/home/nlyu/Data/argus/hyperliquid")
    ///
    /// # Returns
    /// A `HyperliquidArchiver` that continuously organizes market data files
    pub async fn new(tmp_dir: &str, output_dir: &str) -> OrError<Self> {
        println!("========================================");
        println!("  Hyperliquid Archiver");
        println!("========================================");
        println!();
        println!("Initializing HyperliquidArchiver...");
        println!("  Temporary dir: {}", tmp_dir);
        println!("  Output dir: {}", output_dir);
        println!();

        // Define data types for each market
        let spot_data_types = vec![
            "last_trade".to_string(),
            "bbo".to_string(),
            "orderbook".to_string(),
            "spot_context".to_string(),
        ];

        let perp_data_types = vec![
            "last_trade".to_string(),
            "bbo".to_string(),
            "orderbook".to_string(),
            "perp_context".to_string(),
        ];

        // Paths for spot market
        let spot_tmp_dir = format!("{}/spot", tmp_dir);
        let spot_output_dir = format!("{}/spot", output_dir);

        println!("Initializing spot market archiver...");
        let spot_archiver =
            Archiver::new(&spot_output_dir, &spot_data_types, &spot_tmp_dir).await?;
        println!("  ✓ Spot archiver initialized");
        println!("    Source: {}", spot_tmp_dir);
        println!("    Target: {}", spot_output_dir);
        println!("    Data types: {:?}", spot_data_types);
        println!();

        // Paths for perp market
        let perp_tmp_dir = format!("{}/perp", tmp_dir);
        let perp_output_dir = format!("{}/perp", output_dir);

        println!("Initializing perp market archiver...");
        let perp_archiver =
            Archiver::new(&perp_output_dir, &perp_data_types, &perp_tmp_dir).await?;
        println!("  ✓ Perp archiver initialized");
        println!("    Source: {}", perp_tmp_dir);
        println!("    Target: {}", perp_output_dir);
        println!("    Data types: {:?}", perp_data_types);
        println!();

        println!("========================================");
        println!("✅ HyperliquidArchiver initialized successfully!");
        println!("========================================");
        println!();
        println!("Organizing files:");
        println!("  Spot: {} → {}", spot_tmp_dir, spot_output_dir);
        println!("  Perp: {} → {}", perp_tmp_dir, perp_output_dir);
        println!();
        println!(
            "Output format: {{output_dir}}/{{spot|perp}}/{{data_type}}/date={{date}}/symbol={{symbol}}/data.parquet"
        );
        println!();
        println!("Background tasks are scanning for files every 10 seconds.");
        println!("Files are archived when newer data is detected for the same symbol.");
        println!();

        Ok(Self {
            spot_archiver,
            perp_archiver,
        })
    }

    /// Gracefully shutdown all archivers
    pub async fn shutdown(mut self) -> OrError<()> {
        println!();
        println!("Shutting down HyperliquidArchiver...");

        println!("  Shutting down spot archiver...");
        self.spot_archiver.shutdown().await?;
        println!("  Shutting down perp archiver...");
        self.perp_archiver.shutdown().await?;

        println!("✅ HyperliquidArchiver shutdown complete");
        Ok(())
    }

    /// Get reference to spot archiver
    pub fn spot_archiver(&self) -> &Archiver {
        &self.spot_archiver
    }

    /// Get reference to perp archiver
    pub fn perp_archiver(&self) -> &Archiver {
        &self.perp_archiver
    }
}
