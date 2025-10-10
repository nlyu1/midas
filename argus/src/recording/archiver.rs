// The archiver systemically organizes the temporary files written by the filescribe
//
// Input directory example: src_dir = /tmp/hyperliquid/
// dir will contain list of subdirs consisting of types, e.g. last_trade, bbo, etc
// Each src_dir/{data_type}/subdir will consist of /{symbol}_{time}.pq, see behavior in file.rs
//
// We'll also be given: target_dir (e.g.) = /tmp/agora/hyperliquid
// Should consist of two-level tree /{data_type}/date={date}/symbol={symbol}/data.parquet, in hive-partitioned format.
// Archiver is responsible for systematically moving temp files into this organized structure.
// TODO: add flushing summary.

use crate::constants::HYPERLIQUID_ARCHIVER_FLUSH_INTERVAL_SECONDS;
use crate::types::TradingSymbol;
use agora::utils::OrError;
use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tokio::time;

type DataType = String;

pub struct Archiver {
    data_types: Vec<String>,
    target_dir: String,
    src_dir: Arc<RwLock<String>>,
    last_updates: Arc<RwLock<HashMap<DataType, HashMap<TradingSymbol, DateTime<Local>>>>>,
    bg_handles: Vec<JoinHandle<()>>,
}

impl Archiver {
    /// Creates a new Archiver instance
    pub async fn new(
        target_dir: &str,
        data_types: &[String],
        initial_src_dir: &str,
    ) -> OrError<Self> {
        // Validate source directory
        Self::is_valid_src_dir(initial_src_dir)?;

        // Report existing data
        println!("Initializing Archiver...");
        println!("  Source dir: {}", initial_src_dir);
        println!("  Target dir: {}", target_dir);
        println!("  Data types: {:?}", data_types);

        for data_type in data_types {
            let src_path = format!("{}/{}", initial_src_dir, data_type);
            if let Ok(entries) = fs::read_dir(&src_path) {
                let count = entries.count();
                println!("  Found {} files in {}", count, data_type);
            }
        }

        // Initialize target directory structure
        fs::create_dir_all(target_dir)
            .map_err(|e| format!("Failed to create target directory {}: {}", target_dir, e))?;

        for data_type in data_types {
            let type_dir = format!("{}/{}", target_dir, data_type);
            fs::create_dir_all(&type_dir)
                .map_err(|e| format!("Failed to create data type directory {}: {}", type_dir, e))?;
        }

        println!("Target directory structure created successfully");

        // Initialize last_updates with empty maps for each data type
        let mut last_updates_map: HashMap<DataType, HashMap<TradingSymbol, DateTime<Local>>> =
            HashMap::new();
        for data_type in data_types {
            last_updates_map.insert(data_type.clone(), HashMap::new());
        }

        let last_updates = Arc::new(RwLock::new(last_updates_map));
        let src_dir = Arc::new(RwLock::new(initial_src_dir.to_string()));

        // Spawn background tasks for each data type
        let mut bg_handles = Vec::new();
        for data_type in data_types {
            let data_type = data_type.clone();
            let src_dir_clone = Arc::clone(&src_dir);
            let target_dir_clone = target_dir.to_string();
            let last_updates_clone = Arc::clone(&last_updates);

            let handle = tokio::spawn(async move {
                Self::track_single_data_type(
                    data_type,
                    src_dir_clone,
                    target_dir_clone,
                    last_updates_clone,
                )
                .await;
            });

            bg_handles.push(handle);
        }

        println!(
            "Archiver initialized with {} background tasks",
            bg_handles.len()
        );

        Ok(Self {
            data_types: data_types.to_vec(),
            target_dir: target_dir.to_string(),
            src_dir,
            last_updates,
            bg_handles,
        })
    }

    /// Background task that tracks and archives files for a single data type
    async fn track_single_data_type(
        data_type: String,
        src_dir: Arc<RwLock<String>>,
        target_dir: String,
        last_updates: Arc<RwLock<HashMap<DataType, HashMap<TradingSymbol, DateTime<Local>>>>>,
    ) {
        let mut interval = time::interval(time::Duration::from_secs(
            HYPERLIQUID_ARCHIVER_FLUSH_INTERVAL_SECONDS,
        ));

        loop {
            interval.tick().await;

            let current_src_dir = {
                let guard = src_dir.read().unwrap();
                guard.clone()
            };

            let scan_path = format!("{}/{}", current_src_dir, data_type);

            // Read all files in this data type directory
            let entries = match fs::read_dir(&scan_path) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("Failed to read directory {}: {}", scan_path, e);
                    continue;
                }
            };

            let mut files_with_metadata = Vec::new();

            // First pass: parse all files and collect metadata
            for entry in entries {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let path = entry.path();
                if !path.is_file() || path.extension().and_then(|s| s.to_str()) != Some("pq") {
                    continue;
                }

                let filepath = match path.to_str() {
                    Some(s) => s,
                    None => continue,
                };

                match Self::parse_tmp_filepath(filepath) {
                    Ok((parsed_data_type, symbol, timestamp)) => {
                        if parsed_data_type == data_type {
                            files_with_metadata.push((filepath.to_string(), symbol, timestamp));
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse filepath {}: {}", filepath, e);
                    }
                }
            }

            if files_with_metadata.is_empty() {
                continue;
            }

            // Update last_updates with the latest timestamps
            {
                let mut updates = last_updates.write().unwrap();
                let data_type_updates = updates.get_mut(&data_type).unwrap();

                for (_, symbol, timestamp) in &files_with_metadata {
                    data_type_updates
                        .entry(symbol.clone())
                        .and_modify(|t| {
                            if timestamp > t {
                                *t = *timestamp;
                            }
                        })
                        .or_insert(*timestamp);
                }
            }

            // Second pass: flush files that are older than the latest update
            let last_updates_snapshot = {
                let updates = last_updates.read().unwrap();
                updates.get(&data_type).unwrap().clone()
            };
            let mut flushed_file_count = 0;
            let mut flushed_record_count = 0;

            for (filepath, symbol, timestamp) in files_with_metadata {
                if let Some(latest_time) = last_updates_snapshot.get(&symbol) {
                    if timestamp < *latest_time {
                        // This file is older than the latest, safe to flush
                        match Self::flush_tmp_file(&filepath, &target_dir).await {
                            Err(e) => {
                                eprintln!("Failed to flush {}: {}", filepath, e);
                            }
                            Ok(record_count) => {
                                flushed_file_count += 1;
                                flushed_record_count = flushed_record_count + record_count;
                            }
                        }
                    }
                }
            }
            eprintln!(
                "Data type {}: flushed {} records across {} files",
                &data_type, flushed_record_count, flushed_file_count
            );
        }
    }

    /// Parses a temporary filepath to extract data type, symbol, and timestamp
    /// Expected format: {src_dir}/{data_type}/{symbol}_{YY-MM-DD HH:MM:SS}.pq
    fn parse_tmp_filepath(filepath: &str) -> OrError<(String, TradingSymbol, DateTime<Local>)> {
        let path = Path::new(filepath);

        // Get the parent directory name (data_type)
        let data_type = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("Cannot extract data type from path: {}", filepath))?
            .to_string();

        // Get the filename
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("Cannot extract filename from path: {}", filepath))?;

        // Remove .pq extension
        if !filename.ends_with(".pq") {
            return Err(format!("File does not have .pq extension: {}", filename));
        }
        let without_ext = &filename[..filename.len() - 3];

        // The format is {symbol}_{YY-MM-DD HH:MM:SS}
        // The timestamp part is always 17 characters: "25-01-15 10:30:45"
        // Plus the underscore makes it 18 characters from the end
        if without_ext.len() < 18 {
            return Err(format!(
                "Filename too short to contain timestamp: {}",
                filename
            ));
        }

        let split_pos = without_ext.len() - 17;
        if &without_ext[split_pos - 1..split_pos] != "_" {
            return Err(format!(
                "Expected underscore before timestamp in: {}",
                filename
            ));
        }

        let symbol_str = &without_ext[..split_pos - 1];
        let timestamp_str = &without_ext[split_pos..];

        // Parse symbol
        let symbol = TradingSymbol::from_str(symbol_str)?;

        // Parse timestamp using the format from file.rs: %y-%m-%d %H:%M:%S
        let naive_dt = NaiveDateTime::parse_from_str(timestamp_str, "%y-%m-%d %H:%M:%S")
            .map_err(|e| format!("Failed to parse timestamp '{}': {}", timestamp_str, e))?;

        let dt = Local
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| format!("Ambiguous or invalid local datetime: {}", timestamp_str))?;

        Ok((data_type, symbol, dt))
    }

    /// Validates that a source directory exists and has the expected structure
    fn is_valid_src_dir(src_dir: &str) -> OrError<()> {
        let path = Path::new(src_dir);

        if !path.exists() {
            return Err(format!("Source directory does not exist: {}", src_dir));
        }

        if !path.is_dir() {
            return Err(format!("Source path is not a directory: {}", src_dir));
        }

        // Check that it contains at least one subdirectory
        let has_subdirs = fs::read_dir(path)
            .map_err(|e| format!("Cannot read source directory {}: {}", src_dir, e))?
            .any(|entry| {
                entry
                    .ok()
                    .and_then(|e| e.file_type().ok())
                    .map(|ft| ft.is_dir())
                    .unwrap_or(false)
            });

        if !has_subdirs {
            return Err(format!(
                "Source directory {} does not contain any subdirectories",
                src_dir
            ));
        }

        Ok(())
    }

    /// Atomically flushes a temporary file to the target hive-partitioned structure
    async fn flush_tmp_file(filepath: &str, target_dir: &str) -> OrError<usize> {
        let filepath_clone = filepath.to_string();
        let target_dir_clone = target_dir.to_string();

        // Use spawn_blocking for heavy I/O operations
        let flushed_record_count = tokio::task::spawn_blocking(move || {
            Self::flush_tmp_file_blocking(&filepath_clone, &target_dir_clone)
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))??;

        Ok(flushed_record_count)
    }

    /// Blocking implementation of flush_tmp_file
    fn flush_tmp_file_blocking(filepath: &str, target_dir: &str) -> OrError<usize> {
        // Parse the filepath
        let (data_type, symbol, datetime) = Self::parse_tmp_filepath(filepath)?;

        // Extract date in YYYY-MM-DD format
        let date_str = datetime.format("%Y-%m-%d").to_string();

        // Build target path: {target_dir}/{data_type}/date={date}/symbol={symbol}/data.parquet
        let target_subdir = format!(
            "{}/{}/date={}/symbol={}",
            target_dir,
            data_type,
            date_str,
            symbol.to_string()
        );
        fs::create_dir_all(&target_subdir)
            .map_err(|e| format!("Failed to create target directory {}: {}", target_subdir, e))?;

        let target_path = format!("{}/data.parquet", target_subdir);

        // Check if target file exists
        if !Path::new(&target_path).exists() {
            // Recompress with ZSTD for optimal storage
            let record_count = Self::recompress_parquet_file(filepath, &target_path)?;
            // Delete the source file
            fs::remove_file(filepath)
                .map_err(|e| format!("Failed to remove source file {}: {}", filepath, e))?;
            Ok(record_count)
        } else {
            // Complex case: merge with existing file
            let record_count = Self::merge_parquet_files(filepath, &target_path)?;
            // Delete the source file after successful merge
            fs::remove_file(filepath)
                .map_err(|e| format!("Failed to remove source file {}: {}", filepath, e))?;
            Ok(record_count)
        }
    }

    /// Recompresses a parquet file with ZSTD compression
    fn recompress_parquet_file(src_file: &str, dest_file: &str) -> OrError<usize> {
        // Read source file
        let src_file_handle = fs::File::open(src_file)
            .map_err(|e| format!("Failed to open source file {}: {}", src_file, e))?;

        let src_builder = ParquetRecordBatchReaderBuilder::try_new(src_file_handle)
            .map_err(|e| format!("Failed to create reader for source file: {}", e))?;

        let schema = src_builder.schema().clone();

        let mut batches = Vec::new();
        let mut src_reader = src_builder
            .build()
            .map_err(|e| format!("Failed to build reader for source file: {}", e))?;

        for batch_result in src_reader.by_ref() {
            let batch = batch_result
                .map_err(|e| format!("Failed to read batch from source file: {}", e))?;
            batches.push(batch);
        }

        // Write with ZSTD compression
        let dest_file_handle = fs::File::create(dest_file)
            .map_err(|e| format!("Failed to create destination file {}: {}", dest_file, e))?;

        // ZSTD level 3: optimal balance of compression ratio and speed for time-series data
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = ArrowWriter::try_new(dest_file_handle, schema, Some(props))
            .map_err(|e| format!("Failed to create ArrowWriter: {}", e))?;

        let mut total_records = 0;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
            total_records += batch.num_rows();
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(total_records)
    }

    /// Merges a new parquet file into an existing one with ZSTD compression
    fn merge_parquet_files(new_file: &str, existing_file: &str) -> OrError<usize> {
        // Read existing file
        let existing_file_handle = fs::File::open(existing_file)
            .map_err(|e| format!("Failed to open existing file {}: {}", existing_file, e))?;

        let existing_builder = ParquetRecordBatchReaderBuilder::try_new(existing_file_handle)
            .map_err(|e| format!("Failed to create reader for existing file: {}", e))?;

        let mut existing_batches = Vec::new();
        let mut existing_reader = existing_builder
            .build()
            .map_err(|e| format!("Failed to build reader for existing file: {}", e))?;

        for batch_result in existing_reader.by_ref() {
            let batch = batch_result
                .map_err(|e| format!("Failed to read batch from existing file: {}", e))?;
            existing_batches.push(batch);
        }

        // Read new file
        let new_file_handle = fs::File::open(new_file)
            .map_err(|e| format!("Failed to open new file {}: {}", new_file, e))?;

        let new_builder = ParquetRecordBatchReaderBuilder::try_new(new_file_handle)
            .map_err(|e| format!("Failed to create reader for new file: {}", e))?;

        let schema = new_builder.schema().clone();

        let mut new_batches = Vec::new();
        let mut new_reader = new_builder
            .build()
            .map_err(|e| format!("Failed to build reader for new file: {}", e))?;

        for batch_result in new_reader.by_ref() {
            let batch =
                batch_result.map_err(|e| format!("Failed to read batch from new file: {}", e))?;
            new_batches.push(batch);
        }

        // Concatenate all batches
        let mut all_batches = existing_batches;
        all_batches.extend(new_batches);

        // Write to a temporary file, then atomically replace
        let temp_path = format!("{}.tmp", existing_file);
        let temp_file = fs::File::create(&temp_path)
            .map_err(|e| format!("Failed to create temp file {}: {}", temp_path, e))?;

        // ZSTD level 3: optimal balance of compression ratio and speed for time-series data
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = ArrowWriter::try_new(temp_file, schema, Some(props))
            .map_err(|e| format!("Failed to create ArrowWriter: {}", e))?;

        let mut total_records = 0;
        for batch in &all_batches {
            writer
                .write(batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
            total_records += batch.num_rows();
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        // Atomically replace the existing file
        fs::rename(&temp_path, existing_file)
            .map_err(|e| format!("Failed to rename {} to {}: {}", temp_path, existing_file, e))?;

        Ok(total_records)
    }

    /// Swaps the source directory to a new one
    pub fn swap_on(&self, new_src_dir: &str) -> OrError<()> {
        Self::is_valid_src_dir(new_src_dir)?;

        let mut guard = self.src_dir.write().unwrap();
        *guard = new_src_dir.to_string();

        println!("Swapped source directory to: {}", new_src_dir);
        Ok(())
    }

    /// Gets the current source directory
    pub fn src_dir(&self) -> String {
        let guard = self.src_dir.read().unwrap();
        guard.clone()
    }

    /// Returns time since last update for each data type and symbol
    pub fn time_since_last_update(&self) -> HashMap<String, HashMap<TradingSymbol, Duration>> {
        let updates = self.last_updates.read().unwrap();
        let now = Local::now();

        let mut result = HashMap::new();

        for (data_type, symbols) in updates.iter() {
            let mut symbol_durations = HashMap::new();
            for (symbol, last_time) in symbols {
                let duration = now.signed_duration_since(*last_time);
                symbol_durations.insert(symbol.clone(), duration);
            }
            result.insert(data_type.clone(), symbol_durations);
        }

        result
    }

    /// Gracefully shutdown the archiver
    pub async fn shutdown(&mut self) -> OrError<()> {
        println!("Shutting down Archiver...");

        let handles = std::mem::take(&mut self.bg_handles);
        for handle in handles {
            handle.abort();
        }

        println!("Archiver shutdown complete");
        Ok(())
    }

    pub fn target_dir(&self) -> &str {
        &self.target_dir
    }

    pub fn data_types(&self) -> Vec<&str> {
        self.data_types.iter().map(|s| s.as_str()).collect()
    }
}

impl Drop for Archiver {
    fn drop(&mut self) {
        for handle in &self.bg_handles {
            handle.abort();
        }
        eprintln!(
            "Warning: Archiver dropped without calling shutdown(). {} background tasks aborted.",
            self.bg_handles.len()
        );
    }
}
