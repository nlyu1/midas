pub mod last_trades;
pub mod s3_helpers;

use anyhow::{Context, Result};
use chrono::NaiveDate;
pub use last_trades::{BinanceSpotTradeBook, BinanceUmFuturesTradeBook};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::WalkDir;

// Binance S3 constants
pub const BINANCE_S3_BASE_URL: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

/// Cutoff timestamp in milliseconds for timestamp format change (2025-01-01)
/// Pre-2025: timestamps in milliseconds, Post-2025: timestamps in microseconds
pub fn binance_timestamp_cutoff_ms() -> i64 {
    use chrono::{NaiveDate, NaiveTime};
    NaiveDate::from_ymd_opt(2025, 1, 1)
        .unwrap()
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .and_utc()
        .timestamp_millis()
}

/// Trait for defining CSV schema for different Binance data types
pub trait BinanceCsvSchema: Send + Sync + 'static {
    /// Get the CSV schema for this data type
    fn get_schema() -> Schema;

    /// Whether the CSV has a header row
    fn has_header() -> bool;
}

/// Helper function to extract (symbol, date) from first row of a parquet file
/// Returns None if file is corrupted (and deletes it)
/// Path checking (ends_with "data.parquet") should be done by caller
fn extract_symbol_date_first_row(path: &Path) -> Option<LazyFrame> {
    let path_str = path.display().to_string();

    // Helper to delete corrupted file and report
    let delete_and_report = |stage: &str, err: &dyn std::fmt::Display| {
        eprintln!("Corrupted parquet ({} failed): {}", stage, path_str);
        eprintln!("Error: {}", err);
        if let Err(e) = fs::remove_file(path) {
            eprintln!("Failed to delete {}: {}", path_str, e);
        } else {
            eprintln!("Deleted: {}", path_str);
        }
    };

    // Try to scan parquet (validates metadata)
    let lf = match LazyFrame::scan_parquet(PlPath::new(&path_str), ScanArgsParquet::default()) {
        Ok(lf) => lf,
        Err(e) => {
            delete_and_report("metadata scan", &e);
            return None;
        }
    };

    // Try to read first row (validates data pages)
    // Cast symbol to String to ensure consistent schema across all files
    let df = match lf
        .select([
            col("symbol").cast(DataType::String).first().alias("symbol"),
            col("date").first().alias("date"),
        ])
        .collect()
    {
        Ok(df) => df,
        Err(e) => {
            delete_and_report("data read", &e);
            return None;
        }
    };
    println!(
        "{:?}, verification successful. height {}",
        &path,
        df.height(),
    );

    Some(df.lazy())
}

#[derive(Debug, Default)]
pub struct UpdateStats {
    pub total: usize,
    pub success: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Trait for Binance data interfaces (spot, futures, etc.)
///
/// Separates implementation-specific concerns (URLs, paths, processing, downloading) from
/// common functionality (caching, diff calculation, parallel updates).
///
/// Universe DataFrame must have columns:
/// - "symbol": DataType::String
/// - "date": DataType::Date
#[allow(async_fn_in_trait)]
pub trait BinanceDataInterface: Send + Sync + Sized + 'static {
    // ============================================
    // REQUIRED: Implementation-specific methods
    // ============================================

    /// Build download URL for a specific symbol and date
    fn build_download_url(&self, symbol: &str, date: NaiveDate) -> String;

    /// Build hive-style path for processed parquet file
    fn build_hive_path(&self, symbol: &str, date: NaiveDate) -> PathBuf;

    /// Build raw path for downloaded zip file
    fn build_raw_path(&self, symbol: &str, date: NaiveDate) -> PathBuf;

    /// Get hive data directory path
    fn hive_data_path(&self) -> &Path;

    /// Get raw data directory path
    fn raw_data_path(&self) -> &Path;

    /// Get universe cache file path
    fn universe_cache_path(&self) -> &Path;

    /// Get date filters (earliest, latest)
    fn date_filters(&self) -> (Option<NaiveDate>, Option<NaiveDate>);

    /// Fetch fresh universe from S3 (implementation-specific S3 logic)
    /// Must return DataFrame with "symbol" (String) and "date" (Date) columns
    async fn fetch_new_universe(&self) -> Result<DataFrame>;

    /// Download raw data file (implementation-specific download logic for flexibility)
    async fn download_raw(&self, symbol: &str, date: NaiveDate) -> Result<()>;

    /// Process downloaded zip to parquet (calls implementation-specific helpers)
    /// Returns number of rows processed
    fn process_download_to_parquet(
        zip_path: &Path,
        hive_path: &Path,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize>;

    // ============================================
    // PROVIDED: Common functionality
    // ============================================

    /// Initialize universe with caching and date filtering
    async fn initialize_universe(&self, refresh: bool) -> Result<()> {
        let universe_cache_path = self.universe_cache_path();

        // Fetch new universe if refresh requested or cache missing
        if refresh || !universe_cache_path.exists() {
            let mut universe_df = self.fetch_new_universe().await?;

            // Create parent directory if needed
            if let Some(parent) = universe_cache_path.parent() {
                fs::create_dir_all(parent)?;
            }

            // Write to cache
            let mut file = fs::File::create(universe_cache_path)?;
            ParquetWriter::new(&mut file).finish(&mut universe_df)?;
            println!("Written new universe to {:?}", universe_cache_path)
        }

        Ok(())
    }

    /// Get the universe dataframe (reads from cache, applies date filters)
    async fn get_universe_df(&self) -> Result<DataFrame> {
        let universe_cache_path = self.universe_cache_path();

        if !universe_cache_path.exists() {
            anyhow::bail!("Universe not initialized. Call initialize_universe first.");
        }

        let path_str = universe_cache_path
            .to_str()
            .context("Invalid universe cache path")?
            .to_string();

        let (earliest_date, latest_date) = self.date_filters();

        // Wrap .collect() in spawn_blocking to avoid nested runtime issues
        let universe_df = tokio::task::spawn_blocking(move || {
            let mut universe_df =
                LazyFrame::scan_parquet(PlPath::new(&path_str), Default::default())?.collect()?;

            // Apply date filters if specified
            if earliest_date.is_some() || latest_date.is_some() {
                let mut lazy_df = universe_df.lazy();
                if let Some(earliest) = earliest_date {
                    lazy_df = lazy_df.filter(col("date").gt_eq(lit(earliest)));
                }
                if let Some(latest) = latest_date {
                    lazy_df = lazy_df.filter(col("date").lt_eq(lit(latest)));
                }
                universe_df = lazy_df.collect()?;
            }

            Ok::<DataFrame, anyhow::Error>(universe_df)
        })
        .await??;

        Ok(universe_df)
    }

    fn hive_symbol_date_cache_path(&self) -> PathBuf {
        self.hive_data_path().join("hive_symbol_date_pairs.parquet")
    }

    /// Get (symbol, date) pairs that exist on disk (with caching)
    async fn hive_symbol_date_pairs(&self, recompute: bool) -> Result<DataFrame> {
        let cache_path = self.hive_symbol_date_cache_path();
        println!("Trying to observe cache from: {:?}", cache_path);

        // Recompute if requested or cache doesn't exist
        if recompute || !cache_path.exists() {
            // When recomputing, always try to use existing cache to avoid re-validating all files
            let cached_pairs = if cache_path.exists() {
                let cache_path_str = cache_path
                    .to_str()
                    .context("Invalid cache path")?
                    .to_string();
                Some(
                    tokio::task::spawn_blocking(move || {
                        LazyFrame::scan_parquet(PlPath::new(&cache_path_str), Default::default())?
                            .collect()
                            .context("Failed to read existing cache")
                    })
                    .await??,
                )
            } else {
                None
            };
            println!(
                "Observed {} cached pairs",
                cached_pairs.clone().map(|d| d.height()).unwrap_or(0)
            );

            let hive_df = self.compute_hive_symbol_date_pairs(cached_pairs).await?;

            // Write to cache
            let cache_path_clone = cache_path.clone();
            let mut hive_df_clone = hive_df.clone();
            tokio::task::spawn_blocking(move || {
                let mut file = fs::File::create(&cache_path_clone)?;
                ParquetWriter::new(&mut file).finish(&mut hive_df_clone)?;
                Ok::<(), anyhow::Error>(())
            })
            .await??;

            println!("Written hive symbol-date pairs to {:?}", cache_path);
            return Ok(hive_df);
        }

        // Read from cache
        let cache_path_str = cache_path
            .to_str()
            .context("Invalid cache path")?
            .to_string();

        let hive_df = tokio::task::spawn_blocking(move || {
            LazyFrame::scan_parquet(PlPath::new(&cache_path_str), Default::default())?
                .collect()
                .context("Failed to read hive symbol-date pairs cache")
        })
        .await??;

        Ok(hive_df)
    }

    /// Compute (symbol, date) pairs that exist on disk (no caching, pure computation)
    async fn compute_hive_symbol_date_pairs(
        &self,
        cached_pairs: Option<DataFrame>,
    ) -> Result<DataFrame> {
        let hive_data_path = self.hive_data_path().to_path_buf();

        // Build HashSet of known paths from cache
        let (known_paths, cached_lf) = if let Some(ref cached) = cached_pairs {
            println!("Read {} symbol-date pairs from cache", cached.height());

            let symbol_col = cached.column("symbol")?.str()?;
            let date_col = cached.column("date")?.date()?;

            let mut set = HashSet::new();
            for i in 0..cached.height() {
                if let (Some(symbol), Some(days_since_epoch)) =
                    (symbol_col.get(i), date_col.phys.get(i))
                {
                    // Polars Date is stored as days since Unix epoch (1970-01-01)
                    if let Some(date) =
                        NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
                    {
                        let hive_path = self.build_hive_path(symbol, date);
                        set.insert(hive_path.to_string_lossy().to_string());
                    }
                }
            }
            // Normalize schema: only select symbol and date columns to match new data
            (
                set,
                Some(cached.clone().lazy().select([col("symbol"), col("date")])),
            )
        } else {
            (HashSet::new(), None)
        };

        // Wrap in spawn_blocking to avoid nested runtime issues
        let result = tokio::task::spawn_blocking(move || {
            println!("Scanning and validating parquet files in parallel...");

            // Parallelize validation: only validate NEW files not in cache
            let new_lazyframes: Vec<LazyFrame> = WalkDir::new(&hive_data_path)
                .min_depth(1)
                .into_iter()
                .par_bridge()
                .filter_map(|entry_result| {
                    let entry = entry_result.ok()?;
                    let path = entry.path();

                    // Filter: must end with data.parquet
                    if !path.ends_with("data.parquet") {
                        return None;
                    }
                    let path_str = path.to_string_lossy().to_string();
                    // Skip if already in cache
                    if known_paths.contains(&path_str) {
                        return None;
                    }
                    // Expensive operation: download & unzip data for symbol date path
                    extract_symbol_date_first_row(path)
                })
                .collect();

            println!(
                "Collected {} additional symbol-date pairs",
                new_lazyframes.len()
            );
            // Build final result: cached_pairs + new data
            if new_lazyframes.is_empty() && cached_lf.is_none() {
                // No files at all
                return Ok(DataFrame::empty_with_schema(&Schema::from_iter(vec![
                    Field::new("symbol".into(), DataType::String),
                    Field::new("date".into(), DataType::Date),
                ])));
            }
            let mut all_frames = Vec::new();
            // Add cached data first
            if let Some(lf) = cached_lf {
                all_frames.push(lf);
            }
            // Add new data
            all_frames.extend(new_lazyframes);
            // Concatenate all
            let onhive_keys = concat(
                all_frames,
                UnionArgs {
                    parallel: true,
                    rechunk: false,
                    to_supertypes: false,
                    ..Default::default()
                },
            )
            .context("Failed to concatenate file scans")?;

            onhive_keys
                .collect()
                .context("Failed to collect hive symbol-date pairs")
        })
        .await??;

        Ok(result)
    }

    /// Calculate missing (symbol, date) pairs (universe - on_disk)
    async fn nohive_symbol_date_pairs(&self, recompute: bool) -> Result<DataFrame> {
        let universe_df = self.get_universe_df().await?;
        let hive_df = self.hive_symbol_date_pairs(recompute).await?;

        // Perform anti-join in blocking task
        let result = tokio::task::spawn_blocking(move || {
            let universe_lf = universe_df.lazy().select(&[col("symbol"), col("date")]);

            if hive_df.height() == 0 {
                // No files on disk yet, return full universe
                return universe_lf
                    .collect()
                    .context("Failed to collect universe (no files on disk yet)");
            }

            let hive_lf = hive_df.lazy();

            // Anti-join: universe - hive = missing pairs
            let missing_pairs_lf = universe_lf.join(
                hive_lf,
                [col("symbol"), col("date")],
                [col("symbol"), col("date")],
                JoinArgs::new(JoinType::Anti),
            );

            missing_pairs_lf
                .collect()
                .context("Failed to execute anti-join query")
        })
        .await??;

        Ok(result)
    }

    /// Ensure data is ready (idempotent side effect: download + process if needed)
    ///
    /// This is the core shared logic used by both symbol_date_df and update_universe.
    async fn ensure_data_ready(self: Arc<Self>, symbol: &str, date: NaiveDate) -> Result<()> {
        let hive_path = self.build_hive_path(symbol, date);
        // Already processed
        if hive_path.exists() {
            return Ok(());
        }
        // Download if needed
        self.download_raw(symbol, date).await?;

        // Process zip to parquet (CPU-bound, use blocking task)
        let raw_path = self.build_raw_path(symbol, date);
        let symbol_owned = symbol.to_string();

        tokio::task::spawn_blocking(move || {
            Self::process_download_to_parquet(&raw_path, &hive_path, &symbol_owned, date)
        })
        .await??;

        Ok(())
    }

    /// Get data for a specific symbol and date
    async fn symbol_date_df(self: Arc<Self>, symbol: &str, date: NaiveDate) -> Result<LazyFrame> {
        let universe_df = self.get_universe_df().await?;

        let symbol_owned = symbol.to_string();
        // Check if this symbol/date exists in universe (wrap .collect() in spawn_blocking)
        let exists = tokio::task::spawn_blocking(move || {
            universe_df
                .clone()
                .lazy()
                .filter(
                    col("date")
                        .eq(lit(date))
                        .and(col("symbol").eq(lit(symbol_owned))),
                )
                .collect()
                .map(|df| df.height() > 0)
        })
        .await??;

        if !exists {
            anyhow::bail!("Unable to fetch {} on {}", symbol, date);
        }

        // Build paths before ensure_data_ready (which takes ownership)
        let hive_path = self.build_hive_path(symbol, date);

        // Ensure data is ready (download + process if needed)
        self.ensure_data_ready(symbol, date).await?;

        // Read and return
        let path_str = hive_path.to_str().context("Invalid hive path")?;
        println!("Reading from: {}", path_str);
        Ok(LazyFrame::scan_parquet(
            PlPath::new(path_str),
            Default::default(),
        )?)
    }

    /// Download and process all missing (symbol, date) pairs using parallel processing
    ///
    /// Uses rayon for thread-level parallelism with per-thread tokio runtimes.
    /// Each thread calls ensure_data_ready to perform async I/O for downloads
    /// and blocking I/O for processing.
    async fn update_universe(
        self: Arc<Self>,
        _num_workers: usize,
        recompute_hive_symbol_date_pairs: bool,
    ) -> Result<UpdateStats> {
        let missing = self
            .nohive_symbol_date_pairs(recompute_hive_symbol_date_pairs)
            .await?;

        // Extract (symbol, date) pairs from DataFrame with Date dtype
        let symbol_col = missing.column("symbol")?.str()?;
        let date_col = missing.column("date")?.date()?;

        let mut pairs: Vec<(String, NaiveDate)> = Vec::new();
        println!("Making up for {} symbol-date pairs.", missing.height());
        for i in 0..missing.height() {
            if let (Some(symbol), Some(days_since_epoch)) =
                (symbol_col.get(i), date_col.phys.get(i))
            {
                // Polars Date is stored as days since Unix epoch (1970-01-01)
                // Convert to NaiveDate
                if let Some(date) = NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
                {
                    pairs.push((symbol.to_string(), date));
                }
            }
        }

        if pairs.is_empty() {
            println!("No missing pairs to download");
            return Ok(UpdateStats::default());
        }

        println!("Found {} missing (symbol, date) pairs", pairs.len());

        // Wrap parallel processing in spawn_blocking to avoid nested runtime issues
        let self_clone = Arc::clone(&self);
        let results: Vec<(String, NaiveDate, String, Option<String>)> =
            tokio::task::spawn_blocking(move || {
                // Process pairs in parallel using rayon + per-thread tokio runtimes
                pairs
                    .into_par_iter()
                    .map(|(symbol, date)| {
                        let self_clone = Arc::clone(&self_clone);
                        let rt = tokio::runtime::Runtime::new().unwrap();

                        rt.block_on(async move {
                            let hive_path = self_clone.build_hive_path(&symbol, date);

                            // Skip if already exists
                            if hive_path.exists() {
                                return (symbol, date, "skipped".to_string(), None);
                            }
                            println!("  {} {}: Processing...", symbol, date);

                            // Pure side effect: ensure parquet exists, or find data on disk.
                            match self_clone.ensure_data_ready(&symbol, date).await {
                                Ok(_) => {
                                    println!("  {} {}: Done!", symbol, date);
                                    (symbol, date, "success".to_string(), None)
                                }
                                Err(e) => {
                                    eprintln!("  {} {}: Failed - {}", symbol, date, e);
                                    (symbol, date, "error".to_string(), Some(e.to_string()))
                                }
                            }
                        })
                    })
                    .collect()
            })
            .await?;

        // Compute summary statistics
        let stats = UpdateStats {
            total: results.len(),
            success: results
                .iter()
                .filter(|(_, _, status, _)| status == "success")
                .count(),
            skipped: results
                .iter()
                .filter(|(_, _, status, _)| status == "skipped")
                .count(),
            failed: results
                .iter()
                .filter(|(_, _, status, _)| !matches!(status.as_str(), "success" | "skipped"))
                .count(),
        };
        // Print errors if any
        let errors: Vec<_> = results
            .iter()
            .filter(|(_, _, status, _)| !matches!(status.as_str(), "success" | "skipped"))
            .collect();

        if !errors.is_empty() {
            println!("\n⚠ {} failures:", errors.len());
            for (symbol, date, status, error) in errors.iter().take(10) {
                println!("  {} {}: {} - {:?}", symbol, date, status, error);
            }
            if errors.len() > 10 {
                println!("  ... and {} more", errors.len() - 10);
            }
        }
        println!(
            "✓ Completed: {} successful, {} skipped, {} failed\nHive symbol-date cache is at {}",
            stats.success,
            stats.skipped,
            stats.failed,
            self.hive_symbol_date_cache_path().to_string_lossy()
        );

        Ok(stats)
    }
}
