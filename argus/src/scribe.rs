use agora::utils::OrError;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
mod file;
pub use file::{AgoraDirScribe, SinglePathScribe};

/// Trait for writing Rust structs to Parquet files with DuckDB compatibility
///
/// Uses Apache Arrow for schema definition and efficient columnar storage.
/// Implements datetime handling with UTC timestamps and supports nested data structures.
pub trait ArgusParquetable: Sized {
    /// Returns the Arrow schema for this type.
    ///
    /// Use:
    /// - `DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))` for DateTime<Utc>
    /// - `DataType::Float64` for numeric types (Price, TradeSize)
    /// - `DataType::Utf8` for strings (TradingSymbol)
    /// - `DataType::List(Arc::new(Field::new("item", DataType::Float64, false)))` for Vec<f64>
    fn arrow_schema() -> std::sync::Arc<arrow::datatypes::Schema>;

    /// Converts a vector of this type into an Arrow RecordBatch.
    ///
    /// Implementation should:
    /// 1. Convert each field to appropriate Arrow arrays (StringArray, Float64Array, etc.)
    /// 2. For custom types, convert to primitives (Price -> f64, TradingSymbol -> String)
    /// 3. For nested vectors, use ListBuilder
    /// 4. Create RecordBatch with schema and arrays
    fn to_record_batch(data: Vec<Self>) -> OrError<arrow::record_batch::RecordBatch>;

    /// Writes a vector of this type to a Parquet file at the given path.
    ///
    /// Default implementation uses Snappy compression and writes a single RecordBatch.
    /// Override for custom behavior (e.g., batching, different compression).
    fn write_to_parquet(data: Vec<Self>, output_path: &std::path::Path) -> OrError<()> {
        let schema = Self::arrow_schema();
        let batch = Self::to_record_batch(data)?;

        let file = std::fs::File::create(output_path)
            .map_err(|e| format!("Failed to create file {:?}: {}", output_path, e))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| format!("Failed to create ArrowWriter: {}", e))?;

        writer
            .write(&batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        Ok(())
    }
}
