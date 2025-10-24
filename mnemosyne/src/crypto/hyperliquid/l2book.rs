use anyhow::{Context, Result};
use lz4_flex::frame::FrameDecoder;
use polars::prelude::*;
use serde_json::Value;
use std::io::Read;
use std::path::Path;

/// Read single Hyperliquid L2 book LZ4 file into DataFrame.
/// Replicates Python `read_hyperliquid_l2book_lz4` from notebook.
pub fn read_hyperliquid_l2book_lz4(symbol_file: &Path, symbol: &str) -> Result<DataFrame> {
    // Decompress LZ4
    let file = std::fs::File::open(symbol_file)?;
    let mut decoder = FrameDecoder::new(file);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    let data_str = String::from_utf8(decompressed)?;

    // Column vectors
    let mut time_vec = Vec::new();
    let mut timestamp_vec = Vec::new();
    let mut is_bid_vec = Vec::new();
    let mut price_vec = Vec::new();
    let mut csize_vec = Vec::new();
    let mut depth_vec = Vec::new();
    let mut num_orders_vec = Vec::new();

    // Parse each line
    for line in data_str.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let record: Value = serde_json::from_str(line)?;

        // Validate
        let ver_num = record["ver_num"].as_i64().context("Missing ver_num")?;
        let channel = record["raw"]["channel"].as_str().context("Missing channel")?;
        let record_symbol = record["raw"]["data"]["coin"].as_str().context("Missing coin")?;

        anyhow::ensure!(ver_num == 1, "Expected ver_num=1");
        anyhow::ensure!(channel == "l2Book", "Expected channel=l2Book");
        anyhow::ensure!(record_symbol == symbol, "Symbol mismatch");

        let time_str = record["time"].as_str().context("Missing time")?;
        let timestamp = record["raw"]["data"]["time"].as_i64().context("Missing timestamp")?;
        let levels = record["raw"]["data"]["levels"].as_array().context("Missing levels")?;

        anyhow::ensure!(levels.len() == 2, "Expected 2 levels (bids/asks)");

        // Process bids (k=0) and asks (k=1)
        for (k, side_levels) in levels.iter().enumerate() {
            let is_bid = k == 0;
            let side_array = side_levels.as_array().context("Invalid level array")?;
            let mut csize = 0.0;

            for (j, level) in side_array.iter().enumerate() {
                let px = level["px"].as_str().context("Missing px")?.parse::<f64>()?;
                let sz = level["sz"].as_str().context("Missing sz")?.parse::<f64>()?;
                let n = level["n"].as_i64().context("Missing n")? as i16;

                csize += sz;

                time_vec.push(time_str.to_string());
                timestamp_vec.push(timestamp);
                is_bid_vec.push(is_bid);
                price_vec.push(px);
                csize_vec.push(csize);
                depth_vec.push(j as i16);
                num_orders_vec.push(n);
            }
        }
    }

    // Build DataFrame with type conversions
    let df = df!(
        "time" => time_vec,
        "timestamp" => timestamp_vec,
        "is_bid" => is_bid_vec,
        "price" => price_vec,
        "csize" => csize_vec,
        "depth" => depth_vec,
        "num_orders_at_level" => num_orders_vec,
    )?;

    let result = df
        .lazy()
        .select([
            col("time").str().to_datetime(
                Some(TimeUnit::Microseconds),
                None,
                StrptimeOptions::default(),
                lit("raise"),
            ),
            col("price"),
            col("csize"),
            col("depth").cast(DataType::Int16),
            col("num_orders_at_level").cast(DataType::Int16),
            col("is_bid"),
            (col("timestamp") * lit(1000))
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias("created_time"),
        ])
        .collect()?;

    Ok(result)
}
