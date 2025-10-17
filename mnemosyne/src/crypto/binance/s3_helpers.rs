/// S3 API utilities for Binance public data bucket using AWS SDK.
///
/// Queries `s3://data.binance.vision` (ap-northeast-1) to discover available data files.
/// Handles pagination automatically via AWS SDK paginators.
use anyhow::Result;
use aws_sdk_s3::Client;
use std::collections::HashSet;

const BUCKET: &str = "data.binance.vision";
const REGION: &str = "ap-northeast-1";

async fn create_s3_client() -> Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .load()
        .await;
    Client::new(&config)
}

/// Normalize S3 prefix: ensure "data/" prefix and trailing "/" (S3 API requirement)
/// Example: "spot/daily/trades" → "data/spot/daily/trades/"
#[inline]
fn normalize_s3_prefix(prefix: &str) -> String {
    let with_data = if !prefix.starts_with("data/") {
        format!("data/{}", prefix)
    } else {
        prefix.to_string()
    };

    if !with_data.ends_with("/") {
        format!("{}/", with_data)
    } else {
        with_data
    }
}

/// List all S3 object keys (files) under a prefix with automatic pagination.
/// Returns full file paths, e.g.: "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-10-05.zip"
pub async fn get_all_keys(_base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let client = create_s3_client().await;
    let fixed_prefix = normalize_s3_prefix(prefix);
    let mut all_keys = Vec::new();

    let mut paginator = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(&fixed_prefix)
        .delimiter("/")
        .into_paginator()
        .send();

    while let Some(result) = paginator.next().await {
        let output = result?;
        for object in output.contents() {
            if let Some(key) = object.key() {
                all_keys.push(key.to_string());
            }
        }
    }

    Ok(all_keys)
}

/// Get all trading pair subdirectories from S3 using CommonPrefixes (directory listing).
/// Returns trading pairs, e.g.: ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
pub async fn get_all_trade_pairs(_base_url: &str, prefix: &str) -> Result<Vec<String>> {
    let client = create_s3_client().await;
    let fixed_prefix = normalize_s3_prefix(prefix);
    let mut trade_pairs = HashSet::new();

    let mut paginator = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(&fixed_prefix)
        .delimiter("/")
        .into_paginator()
        .send();

    while let Some(result) = paginator.next().await {
        let output = result?;
        for cp in output.common_prefixes() {
            if let Some(prefix_str) = cp.prefix() {
                if let Some(pair) = prefix_str.strip_prefix(&fixed_prefix) {
                    if let Some(pair) = pair.strip_suffix('/') {
                        if !pair.is_empty() {
                            trade_pairs.insert(pair.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(trade_pairs.into_iter().collect())
}
