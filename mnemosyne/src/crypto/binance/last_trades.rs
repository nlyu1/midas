/// Binance trade data downloader with schema-parameterized CSV parsing.
///
/// ## Overview
/// Implements lossless collection of Binance spot and futures last_trade data via:
/// - S3 universe discovery (all available symbol-date pairs)
/// - Parallel download from Binance public CDN
/// - CSV→Parquet conversion with timestamp normalization
/// - Hive-partitioned storage: `date={date}/symbol={symbol}/data.parquet`
///
/// ## Data Sources
/// - **Spot**: `https://data.binance.vision/data/spot/daily/trades/{SYMBOL}USDT/{SYMBOL}USDT-trades-{date}.zip`
/// - **UM Futures**: `https://data.binance.vision/data/futures/um/daily/trades/{SYMBOL}USDT/{SYMBOL}USDT-trades-{date}.zip`
///
/// ## Schema Handling
/// Uses compile-time type parameter `S: BinanceCsvSchema` to handle format differences:
/// - **SpotTradeSchema**: 7 columns, no header
/// - **UmFuturesTradeSchema**: 6 columns, has header (missing `is_best_match`)
///
/// ## Timestamp Normalization
/// Binance changed format at 2025-01-01: pre-2025 milliseconds → post-2025 microseconds.
/// All data normalized to microseconds for consistent Datetime type.
use crate::crypto::CryptoDataInterface;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use once_cell;
use polars::prelude::*;
use regex::Regex;
use std::fs;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crypto::binance::s3_helpers::{get_all_keys, get_all_trade_pairs};
use crate::crypto::binance::{BINANCE_S3_BASE_URL, BinanceCsvSchema, binance_timestamp_cutoff_ms};

/// Regex for extracting dates from S3 paths (e.g., "BTCUSDT-trades-2019-09-08.zip" -> "2019-09-08")
static DATE_RE: once_cell::sync::Lazy<Regex> =
    once_cell::sync::Lazy::new(|| Regex::new(r"(\d{4}-\d{2}-\d{2})").unwrap());

// ============================================
// CSV Schema Implementations
// ============================================

/// Spot trade schema (7 columns, no header row in CSV files)
/// Corresponds to Binance spot market daily trade data format
pub struct SpotTradeSchema;

impl BinanceCsvSchema for SpotTradeSchema {
    fn get_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("trade_id".into(), DataType::Int64),
            Field::new("price".into(), DataType::Float64),
            Field::new("quantity".into(), DataType::Float64),
            Field::new("quote_quantity".into(), DataType::Float64),
            Field::new("time".into(), DataType::Int64),
            Field::new("is_buyer_maker".into(), DataType::Boolean),
            Field::new("is_best_match".into(), DataType::Boolean),
        ])
    }

    fn has_header() -> bool {
        false
    }
}

/// UM (USDT-margined) Futures trade schema (6 columns, includes header row in CSV files)
/// Note: Futures CSVs have one fewer column than spot (no "is_best_match" field)
pub struct UmFuturesTradeSchema;

impl BinanceCsvSchema for UmFuturesTradeSchema {
    fn get_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("id".into(), DataType::Int64),
            Field::new("price".into(), DataType::Float64),
            Field::new("qty".into(), DataType::Float64),
            Field::new("quote_qty".into(), DataType::Float64),
            Field::new("time".into(), DataType::Int64),
            Field::new("is_buyer_maker".into(), DataType::Boolean),
        ])
    }

    fn has_header() -> bool {
        true
    }
}

// ============================================
// Module-level helpers (single source of truth)
// ============================================

/// Convert Binance timestamps handling format change at 2025-01-01.
/// Pre-2025 data uses milliseconds, post-2025 uses microseconds.
/// Normalizes all timestamps to microseconds for consistent datetime representation.
/// Returns dataframe with 'time' column converted to Datetime(Microseconds, None).
fn convert_binance_timestamps(df: DataFrame) -> Result<DataFrame> {
    let cutoff_ms = binance_timestamp_cutoff_ms();

    let result = df
        .lazy()
        .with_column(
            // Conditional conversion based on cutoff date
            when(col("time").gt_eq(lit(cutoff_ms)))
                .then(col("time")) // Post-2025: already microseconds, use as-is
                .otherwise(col("time") * lit(1000)) // Pre-2025: milliseconds -> multiply by 1000
                .cast(DataType::Datetime(TimeUnit::Microseconds, None)),
        )
        .collect()?;
    Ok(result)
}

/// Build the download URL for a specific symbol and date.
/// Example output: "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-10-05.zip"
/// URL structure: {base}/{suffix}/{symbol}{peg}/{symbol}{peg}-{suffix}-{date}.zip
fn build_download_url(
    data_base_url: &str,
    binance_data_suffix: &str,
    symbol: &str,
    date: NaiveDate,
    peg_symbol: &str,
) -> String {
    format!(
        "{}/{}/{}{}/{}{}-{}-{}.zip",
        data_base_url,
        binance_data_suffix,
        symbol,
        peg_symbol,
        symbol,
        peg_symbol,
        binance_data_suffix,
        date.format("%Y-%m-%d")
    )
}

/// Synchronous processing: unzip → read CSV → normalize timestamps → write parquet → delete zip.
/// This is the core processing function called by both sequential and parallel execution paths.
///
/// Processing pipeline:
/// 1. Extract CSV from zip archive
/// 2. Parse CSV with schema-specific settings (header presence varies by market type)
/// 3. Normalize timestamps to microseconds (handles pre/post-2025 format change)
/// 4. Write to Hive-partitioned parquet with LZ4 compression
/// 5. Clean up zip file to save disk space
///
/// Returns: Number of rows processed
fn process_zip_to_parquet<S: BinanceCsvSchema>(
    zip_path: &Path,
    hive_path: &Path,
    symbol: &str,
    date: NaiveDate,
) -> Result<usize> {
    // Ensure hive directory structure exists: date={date}/symbol={symbol}/
    if let Some(parent) = hive_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema = S::get_schema();
    let has_header = S::has_header();

    // Phase 1: Extract CSV from zip archive
    let file = fs::File::open(zip_path)?;
    let mut archive = ::zip::ZipArchive::new(file)?;

    // Locate the CSV file within the zip (typically single-file archives)
    let csv_file_idx = (0..archive.len())
        .find(|&i| {
            archive
                .by_index(i)
                .map(|f| f.name().ends_with(".csv"))
                .unwrap_or(false)
        })
        .context("No CSV file found in zip archive")?;

    let mut csv_file = archive.by_index(csv_file_idx)?;
    let mut csv_content = String::new();
    csv_file.read_to_string(&mut csv_content)?;

    // Release zip file handles immediately (optimization: reduces open file count)
    drop(csv_file);
    drop(archive);

    // Phase 2: Parse CSV with schema-specific configuration
    let cursor = std::io::Cursor::new(csv_content);
    let mut df = CsvReadOptions::default()
        .with_has_header(has_header) // Spot: no header, Futures: has header
        .with_schema(Some(Arc::new(schema))) // Enforce strict typing
        .into_reader_with_file_handle(cursor)
        .finish()?;

    // Phase 3: Normalize timestamps (pre-2025: ms, post-2025: us -> all to us)
    df = convert_binance_timestamps(df)?;

    let num_rows = df.height();

    // Phase 4: Write Hive-partitioned parquet with LZ4 compression (fast + reasonable compression)
    let mut file = fs::File::create(hive_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Lz4Raw)
        .finish(&mut df)?;

    println!("{} {} {}", symbol, date, num_rows);

    // Phase 5: Delete zip to save disk space (parquet is ~10x smaller)
    fs::remove_file(zip_path)?;

    Ok(num_rows)
}

// ============================================
// Main BinanceTradeBook struct
// ============================================

pub struct BinanceTradeBook<S: BinanceCsvSchema> {
    /// Raw zip storage directory. Example (Spot USDT): `/data/mnemosyne/binance/raw/spot/last_trade/peg_symbol=USDT`
    /// Files: `BTCUSDT/BTCUSDT-trades-2025-10-05.zip`
    raw_data_path: PathBuf,

    /// Hive parquet directory. Example (Spot USDT): `/data/mnemosyne/binance/lossless/spot/last_trade/peg_symbol=USDT`
    /// Files: `date=2025-10-05/symbol=BTC/data.parquet`
    hive_data_path: PathBuf,

    /// Universe cache: `{hive_data_path}/universe.parquet`
    universe_cache_path: PathBuf,

    /// S3 listing API base. Example: `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision`
    /// Used in: `{base}?prefix=data/{prefix}/{symbol}{peg}&delimiter=/`
    base_url: String,

    /// Download CDN base. Example (Spot): `https://data.binance.vision/data/spot/daily`
    /// Used in: `{base}/{suffix}/{symbol}{peg}/{symbol}{peg}-{suffix}-{date}.zip`
    data_base_url: String,

    /// Data type suffix. Example: `"trades"` appears in URLs (`/trades/...`) and filenames (`BTCUSDT-trades-...`)
    binance_data_suffix: String,

    /// S3 listing prefix. Example: `"spot/daily/trades"` → S3 query: `?prefix=data/spot/daily/trades/BTCUSDT`
    prefix: String,

    /// Currency filter. Example: `"USDT"` → fetches BTCUSDT, not BTCUSDC; appears in symbols/filenames
    peg_symbol: String,

    /// Optional earliest date (applied to universe after S3 fetch)
    earliest_date: Option<NaiveDate>,

    /// Optional latest date (applied to universe after S3 fetch)
    latest_date: Option<NaiveDate>,

    _schema: PhantomData<S>,
}

impl<S: BinanceCsvSchema> BinanceTradeBook<S> {
    pub fn new(
        hive_data_path: PathBuf,
        raw_data_path: PathBuf,
        data_base_url: String,
        binance_data_suffix: String,
        prefix: String,
        peg_symbol: String,
        earliest_date: Option<NaiveDate>,
        latest_date: Option<NaiveDate>,
    ) -> Result<Self> {
        // Validate peg_symbol
        if peg_symbol != "USDT" && peg_symbol != "USDC" {
            anyhow::bail!(
                "Invalid peg_symbol '{}'. Must be either 'USDT' or 'USDC'",
                peg_symbol
            );
        }

        // Create directories if they don't exist
        fs::create_dir_all(&raw_data_path)?;
        fs::create_dir_all(&hive_data_path)?;

        let universe_cache_path = hive_data_path.join("universe.parquet");

        Ok(Self {
            raw_data_path,
            hive_data_path,
            universe_cache_path,
            base_url: BINANCE_S3_BASE_URL.to_string(),
            data_base_url,
            binance_data_suffix,
            prefix,
            peg_symbol,
            earliest_date,
            latest_date,
            _schema: PhantomData,
        })
    }
}

// Implement BinanceDataInterface trait
impl<S: BinanceCsvSchema> CryptoDataInterface for BinanceTradeBook<S> {
    fn build_download_url(&self, symbol: &str, date: NaiveDate) -> String {
        build_download_url(
            &self.data_base_url,
            &self.binance_data_suffix,
            symbol,
            date,
            &self.peg_symbol,
        )
    }

    fn build_hive_path(&self, symbol: &str, date: NaiveDate) -> PathBuf {
        self.hive_data_path
            .join(format!("date={}", date))
            .join(format!("symbol={}", symbol))
            .join("data.parquet")
    }

    fn build_raw_path(&self, symbol: &str, date: NaiveDate) -> PathBuf {
        self.raw_data_path
            .join(format!("{}{}", symbol, self.peg_symbol))
            .join(format!(
                "{}{}-{}-{}.zip",
                symbol,
                self.peg_symbol,
                self.binance_data_suffix,
                date.format("%Y-%m-%d")
            ))
    }

    fn hive_data_path(&self) -> &Path {
        &self.hive_data_path
    }

    fn raw_data_path(&self) -> &Path {
        &self.raw_data_path
    }

    fn universe_cache_path(&self) -> &Path {
        &self.universe_cache_path
    }

    fn date_filters(&self) -> (Option<NaiveDate>, Option<NaiveDate>) {
        (self.earliest_date, self.latest_date)
    }

    /// Fetch complete (symbol, date) universe from S3. Expensive: queries all symbols & dates.
    /// Cached to universe.parquet to avoid repeated API calls.
    /// Returns DataFrame: ["symbol": String, "date": Date]
    async fn fetch_new_universe(&self) -> Result<DataFrame> {
        // Phase 1: Discover trading pairs (e.g., BTCUSDT, ETHUSDT) for this market
        println!("Fetching new universe...waiting for trade pairs");
        let trade_pairs = get_all_trade_pairs(&self.base_url, &self.prefix).await?;
        let peg_suffix = &self.peg_symbol;
        // Filter to requested peg (e.g., keep BTCUSDT, discard BTCUSDC)
        let symbols: Vec<String> = trade_pairs
            .into_iter()
            .filter(|x| x.ends_with(peg_suffix))
            .map(|x| x.strip_suffix(peg_suffix).unwrap().to_string())
            .collect();
        println!("Fetching new universe...fetched all trade pairs. Waiting");

        // Phase 2: Fetch dates for each symbol with bounded concurrency (max 32 concurrent requests)
        // Unbounded concurrency causes connection pool exhaustion with 500+ symbols
        use futures::stream::{self, StreamExt};

        let results: Vec<Result<(String, Vec<String>), anyhow::Error>> = stream::iter(symbols)
            .map(|symbol| {
                let symbol_clone = symbol.clone();
                let base_url = self.base_url.clone();
                let peg_symbol = self.peg_symbol.clone();
                let prefix = format!("{}/{}{}", self.prefix, symbol, peg_symbol);
                println!("Fetching available dates for {}", symbol);

                async move {
                    // Add per-symbol timeout (30 seconds) to prevent individual symbols from hanging
                    let timeout_duration = std::time::Duration::from_secs(30);
                    let result = tokio::time::timeout(timeout_duration, async {
                        let paths = get_all_keys(&base_url, &prefix).await?;
                        let paths: Vec<String> = paths
                            .into_iter()
                            .filter(|x| !x.ends_with(".CHECKSUM")) // Exclude checksum files
                            .collect();

                        // Phase 3: Extract dates from S3 keys via regex
                        // Example: 'data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2019-09-08.zip' -> '2019-09-08'
                        let date_strings: Vec<String> = paths
                            .iter()
                            .filter_map(|x| {
                                DATE_RE
                                    .captures(x)
                                    .and_then(|caps| caps.get(1))
                                    .map(|date_match| date_match.as_str().to_string())
                            })
                            .collect();
                        Ok::<(String, Vec<String>), anyhow::Error>((symbol, date_strings))
                    })
                    .await;

                    match result {
                        Ok(Ok(data)) => {
                            println!(
                                "    Symbol {} universe fetched ({} dates)",
                                data.0,
                                data.1.len()
                            );
                            Ok(data)
                        }
                        Ok(Err(e)) => {
                            eprintln!("Warning: Error fetching symbol {}: {}", symbol_clone, e);
                            Ok((symbol_clone, Vec::new()))
                        }
                        Err(_) => {
                            eprintln!(
                                "Warning: Timeout fetching symbol {}. Skipping.",
                                symbol_clone
                            );
                            Ok((symbol_clone, Vec::new()))
                        }
                    }
                }
            })
            .buffer_unordered(32) // Limit concurrent requests (prevents connection exhaustion)
            .collect()
            .await;

        // Phase 4: Flatten to columnar format
        let mut symbols_vec = Vec::new();
        let mut dates_vec = Vec::new();

        for result in results {
            let (symbol, date_strings) = result?;
            for date_str in date_strings {
                symbols_vec.push(symbol.clone());
                dates_vec.push(date_str);
            }
        }

        // Phase 5: Build DataFrame, convert dates to Date dtype
        let mut universe_df = df!(
            "symbol" => symbols_vec,
            "date" => dates_vec,
        )?;

        universe_df = universe_df
            .lazy()
            .with_column(col("date").str().to_date(Default::default()))
            .collect()?;

        Ok(universe_df)
    }

    async fn download_raw(&self, symbol: &str, date: NaiveDate) -> Result<()> {
        let raw_path = self.build_raw_path(symbol, date);
        if raw_path.exists() {
            return Ok(());
        }
        if let Some(parent) = raw_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let url = self.build_download_url(symbol, date);
        let client = reqwest::Client::new();
        let response = client.get(&url).send().await?;

        if response.status().is_success() {
            let bytes = response.bytes().await?;
            fs::write(&raw_path, bytes)?;
            println!("Downloaded {}, {}", symbol, date);
            Ok(())
        } else {
            anyhow::bail!("HTTP {}", response.status())
        }
    }

    fn process_download_to_parquet(
        zip_path: &Path,
        hive_path: &Path,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        process_zip_to_parquet::<S>(zip_path, hive_path, symbol, date)
    }
}
