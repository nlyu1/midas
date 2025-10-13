use super::BinanceDataInterface;
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

static DATE_RE: once_cell::sync::Lazy<Regex> =
    once_cell::sync::Lazy::new(|| Regex::new(r"(\d{4}-\d{2}-\d{2})").unwrap());

// ============================================
// CSV Schema Implementations
// ============================================

/// Spot trade schema (7 columns, no header)
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

/// UM (USDT-margined) Futures trade schema (6 columns, has header)
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

/// Convert Binance timestamps handling format change (pre-2025: ms, post-2025: us).
/// Returns dataframe with 'time' column
fn convert_binance_timestamps(df: DataFrame) -> Result<DataFrame> {
    let cutoff_ms = binance_timestamp_cutoff_ms();

    let result = df
        .lazy()
        .with_column(
            when(col("time").gt_eq(lit(cutoff_ms)))
                .then(col("time")) // Already in microseconds
                .otherwise(col("time") * lit(1000)) // Convert ms to us
                .cast(DataType::Datetime(TimeUnit::Microseconds, None)),
        )
        .collect()?;
    Ok(result)
}

/// Build the download URL for a specific symbol and date
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

/// Synchronous function: unzip, read CSV, convert timestamps, write parquet, delete zip.
/// This is the SINGLE implementation used by both class methods and parallel workers.
///
/// Returns: Number of rows processed
fn process_zip_to_parquet<S: BinanceCsvSchema>(
    zip_path: &Path,
    hive_path: &Path,
    symbol: &str,
    date: NaiveDate,
) -> Result<usize> {
    // Create parent directory
    if let Some(parent) = hive_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema = S::get_schema();
    let has_header = S::has_header();

    // Open and read the zip file
    let file = fs::File::open(zip_path)?;
    let mut archive = ::zip::ZipArchive::new(file)?;

    // Find the CSV file in the archive
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

    // Drop the archive and file handles to release the zip file
    drop(csv_file);
    drop(archive);

    // Read CSV with proper schema
    let cursor = std::io::Cursor::new(csv_content);
    let mut df = CsvReadOptions::default()
        .with_has_header(has_header)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()?;

    // Convert timestamps
    df = convert_binance_timestamps(df)?;

    let num_rows = df.height();

    // Write parquet with optimized settings
    let mut file = fs::File::create(hive_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Lz4Raw)
        .finish(&mut df)?;

    println!("{} {} {}", symbol, date, num_rows);

    // Delete the zip file after successful parquet write
    fs::remove_file(zip_path)?;

    Ok(num_rows)
}

// ============================================
// Main BinanceTradeBook struct
// ============================================

pub struct BinanceTradeBook<S: BinanceCsvSchema> {
    /// Will populate with .zip files upon downloads
    raw_data_path: PathBuf,
    /// Will populate via /date={date}/symbol={symbol}/data.parquet style
    hive_data_path: PathBuf,
    /// Cache path for universe data
    universe_cache_path: PathBuf,
    /// Base URL for S3
    base_url: String,
    /// Data download URL
    data_base_url: String,
    /// Binance data suffix (e.g., "trades", "bookTicker")
    binance_data_suffix: String,
    /// S3 prefix for listing files
    prefix: String,
    /// Peg symbol (USDT or USDC)
    peg_symbol: String,
    /// Earliest date filter
    earliest_date: Option<NaiveDate>,
    /// Latest date filter
    latest_date: Option<NaiveDate>,
    /// Phantom data for schema type
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
impl<S: BinanceCsvSchema> BinanceDataInterface for BinanceTradeBook<S> {
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

    async fn fetch_new_universe(&self) -> Result<DataFrame> {
        let trade_pairs = get_all_trade_pairs(&self.base_url, &self.prefix).await?;
        let peg_suffix = &self.peg_symbol;
        let symbols: Vec<String> = trade_pairs
            .into_iter()
            .filter(|x| x.ends_with(peg_suffix))
            .map(|x| x.strip_suffix(peg_suffix).unwrap().to_string())
            .collect();

        // Fetch keys for all symbols concurrently
        let mut tasks = Vec::new();
        for symbol in symbols {
            let base_url = self.base_url.clone();
            let peg_symbol = self.peg_symbol.clone();
            let prefix = format!("{}/{}{}", self.prefix, symbol, peg_symbol);
            tasks.push(async move {
                let paths = get_all_keys(&base_url, &prefix).await?;
                let paths: Vec<String> = paths
                    .into_iter()
                    .filter(|x| !x.ends_with(".CHECKSUM"))
                    .collect();

                // Extract dates from paths
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
            });
        }

        let results = futures::future::join_all(tasks).await;

        let mut symbols_vec = Vec::new();
        let mut dates_vec = Vec::new();

        for result in results {
            let (symbol, date_strings) = result?;
            for date_str in date_strings {
                symbols_vec.push(symbol.clone());
                dates_vec.push(date_str);
            }
        }

        let mut universe_df = df!(
            "symbol" => symbols_vec,
            "date" => dates_vec,
        )?;

        // Convert date strings to Date dtype
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

// ============================================
// Type Aliases for Convenience
// ============================================

/// Spot trade book (7-column schema, no header)
pub type BinanceSpotTradeBook = BinanceTradeBook<SpotTradeSchema>;

/// UM Futures trade book (6-column schema, has header)
pub type BinanceUmFuturesTradeBook = BinanceTradeBook<UmFuturesTradeSchema>;
