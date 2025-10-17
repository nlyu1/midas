/// Development binary for testing AWS S3 SDK integration with Binance public bucket.
///
/// This replicates the functionality of `get_all_trade_pairs` from s3_helpers.rs
/// using the official AWS SDK for Rust instead of direct HTTP requests.
///
/// Binance S3 bucket details:
/// - Bucket: data.binance.vision (public, no auth required)
/// - Region: ap-northeast-1
/// - Example: s3://data.binance.vision/data/spot/daily/trades/BTCUSDT/
use anyhow::Result;
use aws_sdk_s3::Client;
use regex::Regex;
use std::collections::HashSet;

const BUCKET: &str = "data.binance.vision";
const REGION: &str = "ap-northeast-1";

/// Normalize S3 prefix: ensure "data/" prefix and trailing "/" (S3 API requirement)
/// Example: "spot/daily/trades" → "data/spot/daily/trades/"
fn normalize_s3_prefix(prefix: &str) -> String {
    let with_data = if !prefix.starts_with("data/") {
        format!("data/{}", prefix)
    } else {
        prefix.to_string()
    };

    if !with_data.ends_with('/') {
        format!("{}/", with_data)
    } else {
        with_data
    }
}

/// Get all trading pair subdirectories from S3 using CommonPrefixes (directory listing).
/// Uses delimiter="/" to list subdirectories without recursing into them.
/// Returns trading pairs, e.g.: ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
///
/// This replicates the functionality of s3_helpers::get_all_trade_pairs using AWS SDK.
///
/// # Arguments
/// * `client` - AWS S3 client configured for ap-northeast-1
/// * `prefix` - S3 prefix path, e.g., "spot/daily/trades" or "futures/um/daily/trades"
///
/// # Example
/// ```
/// let pairs = get_all_trade_pairs(&client, "spot/daily/trades").await?;
/// // Returns: ["BTC", "ETH", "BNB", ...] (symbols with USDT suffix stripped)
/// ```
async fn get_all_trade_pairs(client: &Client, prefix: &str) -> Result<Vec<String>> {
    let fixed_prefix = normalize_s3_prefix(prefix);
    let mut trade_pairs = HashSet::new();

    println!(
        "Querying S3: bucket={}, prefix={}, delimiter=/",
        BUCKET, fixed_prefix
    );

    // Use paginator to automatically handle pagination (>1000 results)
    // The delimiter="/" makes S3 return CommonPrefixes (subdirectories) instead of individual files
    let mut paginator = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(&fixed_prefix)
        .delimiter("/")
        .into_paginator()
        .send();

    let mut page_count = 0;
    let mut total_prefixes = 0;

    while let Some(result) = paginator.next().await {
        page_count += 1;
        let output = result?;

        // CommonPrefixes contains the "subdirectories" when using delimiter="/"
        // Example common prefix: "data/spot/daily/trades/BTCUSDT/"
        for cp in output.common_prefixes() {
            if let Some(prefix_str) = cp.prefix() {
                total_prefixes += 1;

                // Parse subdirectory name from full S3 prefix
                // Example: "data/spot/daily/trades/BTCUSDT/" -> "BTCUSDT"
                if let Some(pair) = prefix_str.strip_prefix(&fixed_prefix) {
                    if let Some(pair) = pair.strip_suffix('/') {
                        if !pair.is_empty() {
                            trade_pairs.insert(pair.to_string());
                        }
                    }
                }
            }
        }

        println!("  Page {}: {} prefixes", page_count, total_prefixes);
    }

    println!(
        "Completed: {} pages, {} unique trading pairs",
        page_count,
        trade_pairs.len()
    );

    Ok(trade_pairs.into_iter().collect())
}

/// Get all S3 object keys (files) under a prefix with automatic pagination.
/// Returns full file paths, e.g.: "data/spot/daily/trades/ETHUSDT/ETHUSDT-trades-2024-02-05.zip"
///
/// This replicates the functionality of s3_helpers::get_all_keys using AWS SDK.
///
/// # Arguments
/// * `client` - AWS S3 client configured for ap-northeast-1
/// * `prefix` - S3 prefix path, e.g., "spot/daily/trades/ETHUSDT" or "futures/um/daily/trades/BTCUSDT"
///
/// # Example
/// ```
/// let keys = get_all_keys(&client, "spot/daily/trades/ETHUSDT").await?;
/// // Returns: ["data/spot/daily/trades/ETHUSDT/ETHUSDT-trades-2024-02-05.zip", ...]
/// ```
async fn get_all_keys(client: &Client, prefix: &str) -> Result<Vec<String>> {
    let fixed_prefix = normalize_s3_prefix(prefix);
    let mut all_keys = Vec::new();

    println!(
        "Querying S3: bucket={}, prefix={}, delimiter=/",
        BUCKET, fixed_prefix
    );

    // Use paginator to automatically handle pagination (>1000 results)
    // The delimiter="/" prevents recursion into subdirectories
    let mut paginator = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(&fixed_prefix)
        .delimiter("/")
        .into_paginator()
        .send();

    let mut page_count = 0;
    let mut total_files = 0;

    while let Some(result) = paginator.next().await {
        page_count += 1;
        let output = result?;

        // Contents contains the actual file objects (not directories)
        // Each object's key is the full S3 path
        for object in output.contents() {
            if let Some(key) = object.key() {
                total_files += 1;
                all_keys.push(key.to_string());
            }
        }

        println!("  Page {}: {} total files", page_count, total_files);
    }

    println!(
        "Completed: {} pages, {} total keys",
        page_count,
        all_keys.len()
    );

    Ok(all_keys)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Binance S3 SDK Integration Test ===\n");

    // Configure AWS SDK for ap-northeast-1 region
    // Note: Binance S3 bucket is public, but AWS SDK may still require credentials
    // If credentials are not available, this will fail. For anonymous access, we need
    // to explicitly configure anonymous credentials (see commented code below).

    let config = aws_config::defaults(aws_config::BehaviorVersion::v2025_08_07())
        .region(aws_config::Region::new(REGION))
        .load()
        .await;

    let client = Client::new(&config);

    // Test 1: Spot trading pairs
    println!("Test 1: Fetching Spot trading pairs");
    println!("=====================================");
    let spot_prefix = "spot/daily/trades";
    let spot_pairs = get_all_trade_pairs(&client, spot_prefix).await?;

    println!("\nSpot Results:");
    println!("  Total pairs: {}", spot_pairs.len());
    println!("  Sample pairs (first 20):");
    for (i, pair) in spot_pairs.iter().take(20).enumerate() {
        println!("    {:2}. {}", i + 1, pair);
    }
    if spot_pairs.len() > 20 {
        println!("    ... and {} more", spot_pairs.len() - 20);
    }

    // Test 2: Filter to USDT pairs only (matching the actual use case)
    println!("\n\nTest 2: Filter USDT pairs");
    println!("=========================");
    let usdt_pairs: Vec<String> = spot_pairs
        .into_iter()
        .filter(|x| x.ends_with("USDT"))
        .map(|x| x.strip_suffix("USDT").unwrap().to_string())
        .collect();

    println!("  Total USDT pairs: {}", usdt_pairs.len());
    println!("  Sample USDT symbols (first 20):");
    for (i, symbol) in usdt_pairs.iter().take(20).enumerate() {
        println!("    {:2}. {}", i + 1, symbol);
    }
    if usdt_pairs.len() > 20 {
        println!("    ... and {} more", usdt_pairs.len() - 20);
    }

    // Test 3: Get all files for ETH (concrete example matching upstream usage)
    println!("\n\nTest 3: Get all files for ETHUSDT");
    println!("===================================");

    let eth_prefix = "spot/daily/trades/ETHUSDT";
    let eth_keys = get_all_keys(&client, eth_prefix).await?;

    println!("\nETH Raw Keys Results:");
    println!("  Total keys: {}", eth_keys.len());
    println!("  Sample keys (first 10):");
    for (i, key) in eth_keys.iter().take(10).enumerate() {
        println!("    {:2}. {}", i + 1, key);
    }

    // Replicate upstream processing: filter out CHECKSUM files
    let eth_data_files: Vec<String> = eth_keys
        .into_iter()
        .filter(|x| !x.ends_with(".CHECKSUM"))
        .collect();

    println!("\n  After filtering .CHECKSUM:");
    println!("    Data files: {}", eth_data_files.len());
    println!("    Sample data files (first 10):");
    for (i, key) in eth_data_files.iter().take(10).enumerate() {
        println!("    {:2}. {}", i + 1, key);
    }

    // Extract dates using regex (matching upstream behavior)
    use regex::Regex;
    let date_re = Regex::new(r"(\d{4}-\d{2}-\d{2})").unwrap();
    let dates: Vec<String> = eth_data_files
        .iter()
        .filter_map(|x| {
            date_re
                .captures(x)
                .and_then(|caps| caps.get(1))
                .map(|date_match| date_match.as_str().to_string())
        })
        .collect();

    println!("\n  Extracted dates:");
    println!("    Total dates: {}", dates.len());
    println!("    Sample dates (first 20):");
    for (i, date) in dates.iter().take(20).enumerate() {
        println!("    {:2}. {}", i + 1, date);
    }
    if dates.len() > 20 {
        println!("    ... and {} more", dates.len() - 20);
    }

    Ok(())
}

// ============================================
// Alternative: Anonymous access configuration
// ============================================
// If the above fails due to missing AWS credentials, uncomment this:
//
// use aws_sdk_s3::config::Credentials;
//
// let creds = Credentials::new("", "", None, None, "anonymous");
// let config = aws_sdk_s3::Config::builder()
//     .region(aws_config::Region::new(REGION))
//     .credentials_provider(creds)
//     .build();
// let client = Client::from_conf(config);
