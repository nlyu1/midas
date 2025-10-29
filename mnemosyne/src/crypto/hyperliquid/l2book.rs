use anyhow::{Context, Result};
use chrono::NaiveDate;
use lz4::Decoder;
use polars::prelude::*;
use rayon::prelude::*;
use serde_json::Value;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Raw L2 book data (column vectors before DataFrame conversion)
struct L2BookData {
    time: Vec<String>,
    timestamp: Vec<i64>,
    is_bid: Vec<bool>,
    price: Vec<f64>,
    csize: Vec<f64>,
    depth: Vec<i16>,
    num_orders_at_level: Vec<i16>,
}

/// Read single Hyperliquid L2 book LZ4 file into raw vectors.
/// Memory-efficient: returns raw data instead of DataFrame.
fn read_hyperliquid_l2book_lz4_raw(symbol_file: &Path, symbol: &str) -> Result<L2BookData> {
    // Decompress LZ4
    let file = std::fs::File::open(symbol_file)?;
    let mut decoder = Decoder::new(file)?;
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
        let channel = record["raw"]["channel"]
            .as_str()
            .context("Missing channel")?;
        let record_symbol = record["raw"]["data"]["coin"]
            .as_str()
            .context("Missing coin")?;

        anyhow::ensure!(ver_num == 1, "Expected ver_num=1");
        anyhow::ensure!(channel == "l2Book", "Expected channel=l2Book");
        anyhow::ensure!(record_symbol == symbol, "Symbol mismatch");

        let time_str = record["time"].as_str().context("Missing time")?;
        let timestamp = record["raw"]["data"]["time"]
            .as_i64()
            .context("Missing timestamp")?;
        let levels = record["raw"]["data"]["levels"]
            .as_array()
            .context("Missing levels")?;

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

    Ok(L2BookData {
        time: time_vec,
        timestamp: timestamp_vec,
        is_bid: is_bid_vec,
        price: price_vec,
        csize: csize_vec,
        depth: depth_vec,
        num_orders_at_level: num_orders_vec,
    })
}

/// Read single Hyperliquid L2 book LZ4 file into DataFrame (public API).
pub fn read_hyperliquid_l2book_lz4(symbol_file: &Path, symbol: &str) -> Result<DataFrame> {
    let data = read_hyperliquid_l2book_lz4_raw(symbol_file, symbol)?;

    let df = df!(
        "time" => data.time,
        "timestamp" => data.timestamp,
        "is_bid" => data.is_bid,
        "price" => data.price,
        "csize" => data.csize,
        "depth" => data.depth,
        "num_orders_at_level" => data.num_orders_at_level,
    )?;

    df.lazy()
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
            col("timestamp")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias("created_time"),
        ])
        .collect()
        .context("Failed to transform L2 book data")
}

/// Read all Hyperliquid L2 book files for a full date with parallel processing.
/// Replicates Python notebook logic from `dev_hyperliquid_market.ipynb`.
///
/// Parallelizes over all (hour, symbol) file pairs using rayon for efficient I/O.
///
/// # Arguments
/// * `raw_data_path` - Base path (e.g., "/bigdata/mnemosyne/hyperliquid/raw/futures/market_data")
/// * `date` - Date to read
///
/// # Example
/// ```no_run
/// use chrono::NaiveDate;
/// use std::path::Path;
/// use mnemosyne::crypto::hyperliquid::l2book::read_hyperliquid_l2book_bydate;
///
/// let date = NaiveDate::from_ymd_opt(2025, 9, 30).unwrap();
/// let df = read_hyperliquid_l2book_bydate(
///     Path::new("/bigdata/mnemosyne/hyperliquid/raw/futures/market_data"),
///     date
/// )?;
/// ```
pub fn read_hyperliquid_l2book_bydate(raw_data_path: &Path, date: NaiveDate) -> Result<DataFrame> {
    let date_str = date.format("%Y%m%d").to_string();

    // Collect all (path, symbol) pairs across all 24 hours
    let file_paths: Vec<(PathBuf, String)> = (0..24)
        .flat_map(|hour| {
            let hour_path = raw_data_path
                .join(&date_str)
                .join(hour.to_string())
                .join("l2Book");
            fs::read_dir(&hour_path)
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    if path.is_file() && path.extension()? == "lz4" {
                        let symbol = path.file_stem()?.to_str()?.to_string();
                        Some((path, symbol))
                    } else {
                        None
                    }
                })
        })
        .collect();

    anyhow::ensure!(
        !file_paths.is_empty(),
        "No L2 book files found for date {}",
        date
    );

    // Parallel read into raw vectors (memory-efficient: no intermediate DataFrames)
    let all_data: Vec<L2BookData> = file_paths
        .par_iter()
        .filter_map(|(path, symbol)| {
            read_hyperliquid_l2book_lz4_raw(path, symbol).ok()
        })
        .collect();

    anyhow::ensure!(
        !all_data.is_empty(),
        "Failed to read any L2 book files for date {}",
        date
    );

    // Aggregate all vectors into single dataset
    let mut combined = L2BookData {
        time: Vec::new(),
        timestamp: Vec::new(),
        is_bid: Vec::new(),
        price: Vec::new(),
        csize: Vec::new(),
        depth: Vec::new(),
        num_orders_at_level: Vec::new(),
    };

    for data in all_data {
        combined.time.extend(data.time);
        combined.timestamp.extend(data.timestamp);
        combined.is_bid.extend(data.is_bid);
        combined.price.extend(data.price);
        combined.csize.extend(data.csize);
        combined.depth.extend(data.depth);
        combined.num_orders_at_level.extend(data.num_orders_at_level);
    }

    // Build single DataFrame from combined data
    let df = df!(
        "time" => combined.time,
        "timestamp" => combined.timestamp,
        "is_bid" => combined.is_bid,
        "price" => combined.price,
        "csize" => combined.csize,
        "depth" => combined.depth,
        "num_orders_at_level" => combined.num_orders_at_level,
    )?;

    // Apply transformations once on full dataset
    df.lazy()
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
            col("timestamp")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias("created_time"),
        ])
        .collect()
        .context("Failed to transform aggregated L2 book data")
}

// PyO3 bindings
use pyo3::prelude::*;

/// Read all Hyperliquid L2 book files for a full date and save to Parquet (Python binding).
///
/// # Arguments
/// * `raw_data_path` - Base path to raw data directory
/// * `date_str` - Date in YYYY-MM-DD format (e.g., "2025-09-30")
/// * `save_path` - Output Parquet file path (parents created automatically)
///
/// # Example (Python)
/// ```python
/// import mnemosyne as ms
/// ms.read_hyperliquid_l2book_bydate_to(
///     "/bigdata/mnemosyne/hyperliquid/raw/futures/market_data",
///     "2025-09-30",
///     "/tmp/output.parquet"
/// )
/// ```
#[pyfunction]
pub fn py_read_hyperliquid_l2book_bydate_to(
    raw_data_path: &str,
    date_str: &str,
    save_path: &str,
) -> PyResult<()> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Invalid date format '{}': {}",
            date_str, e
        ))
    })?;

    let mut df = read_hyperliquid_l2book_bydate(Path::new(raw_data_path), date)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Create parent directories
    let save_path_buf = PathBuf::from(save_path);
    if let Some(parent) = save_path_buf.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
    }

    // Write with Brotli compression (level 3, matching crypto.rs pattern)
    let mut file = fs::File::create(&save_path_buf)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Brotli(Some(
            BrotliLevel::try_new(3)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?,
        )))
        .finish(&mut df)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok(())
}
