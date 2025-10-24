/// Binance data collection: constants, schemas, and type aliases.
///
/// ## Organization
/// - **Constants**: S3 URLs, timestamp cutoffs
/// - **Traits**: `BinanceCsvSchema` for compile-time CSV format configuration
/// - **Type Aliases**: `BinanceSpotTradeBook`, `BinanceUmFuturesTradeBook`
/// - **Modules**: `last_trades` (download/process), `s3_helpers` (S3 API queries)
///
/// ## Key Design Decision
/// Timestamp format changed at 2025-01-01 (milliseconds → microseconds).
/// All processing normalizes to microseconds for uniform Datetime representation.

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

// ============================================
// CSV Schema Abstraction
// ============================================

/// Trait for compile-time CSV schema configuration and data processing pipeline
/// Implemented by SpotTradeSchema (7 columns, no header) and UmFuturesTradeSchema (6 columns, header)
/// Each implementation defines both the CSV schema and the postprocessing steps (e.g., timestamp conversion)
pub trait BinanceSchemaPipeline: Send + Sync + 'static {
    /// Polars schema defining column names and types
    fn get_schema() -> Schema;

    /// Whether CSV files include a header row (varies by market type)
    fn has_header() -> bool;

    /// Postprocess the DataFrame after CSV parsing (e.g., timestamp normalization)
    /// Different market types have different timestamp formats that need correction
    fn postprocess_df(df: DataFrame) -> anyhow::Result<DataFrame>;
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
