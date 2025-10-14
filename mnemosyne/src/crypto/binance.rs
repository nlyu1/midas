pub mod last_trades;
pub mod s3_helpers;

use last_trades::{BinanceTradeBook, SpotTradeSchema, UmFuturesTradeSchema};
use polars::prelude::*;

// ============================================
// Binance S3 Configuration
// ============================================

/// S3 base URL for API queries (listing files, metadata)
/// Used in: `{base}?prefix=data/spot/daily/trades/BTCUSDT&delimiter=/`
pub const BINANCE_S3_BASE_URL: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

/// Timestamp format change cutoff: 2025-01-01 00:00:00 UTC (1735689600000 ms since epoch)
/// Binance changed data format: Pre-2025 uses milliseconds, Post-2025 uses microseconds
/// This function returns the cutoff as milliseconds for comparison with raw timestamp values
pub fn binance_timestamp_cutoff_ms() -> i64 {
    use chrono::{NaiveDate, NaiveTime};
    NaiveDate::from_ymd_opt(2025, 1, 1)
        .unwrap()
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .and_utc()
        .timestamp_millis()
}

// ============================================
// CSV Schema Abstraction
// ============================================

/// Trait for compile-time CSV schema configuration (spot vs futures have different formats)
/// Implemented by SpotTradeSchema (7 columns, no header) and UmFuturesTradeSchema (6 columns, header)
pub trait BinanceCsvSchema: Send + Sync + 'static {
    /// Polars schema defining column names and types
    fn get_schema() -> Schema;

    /// Whether CSV files include a header row (varies by market type)
    fn has_header() -> bool;
}

// ============================================
// Concrete Type Aliases
// ============================================

/// Binance Spot market trade book (7-column CSV schema, no header)
/// Data source: `https://data.binance.vision/data/spot/daily/trades/...`
pub type BinanceSpotTradeBook = BinanceTradeBook<SpotTradeSchema>;

/// Binance USDT-margined perpetual futures trade book (6-column CSV schema, has header)
/// Data source: `https://data.binance.vision/data/futures/um/daily/trades/...`
pub type BinanceUmFuturesTradeBook = BinanceTradeBook<UmFuturesTradeSchema>;
