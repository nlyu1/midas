use anyhow::{Context, Result};
use chrono::NaiveDate;
use mnemosyne::binance::{BinanceDataInterface, BinanceSpotTradeBook};
use mnemosyne::constants::{BINANCE_DATA_PATH, home_path};
use polars::prelude::*;
use std::sync::Arc;
use walkdir::WalkDir;

#[tokio::main]
async fn main() -> Result<()> {
    let base_path = home_path().join(BINANCE_DATA_PATH);
    let raw_data_path = base_path.join("raw/spot/usdt/last_trade");
    let hive_data_path = base_path.join("spot/last_trade/peg_symbol=USDT");
    let hive_data_path_clone = hive_data_path.clone();
    let data_base_url = "https://data.binance.vision/data/spot/daily";
    let prefix = "spot/daily/trades";
    let binance_data_suffix = "trades";
    let peg_symbol = "USDT";
    let earliest_date = NaiveDate::from_ymd_opt(2022, 1, 1);

    let tb = Arc::new(BinanceSpotTradeBook::new(
        hive_data_path,
        raw_data_path,
        data_base_url.to_string(),
        binance_data_suffix.to_string(),
        prefix.to_string(),
        peg_symbol.to_string(),
        earliest_date,
        None,
    )?);

    tb.initialize_universe(false).await?;
    let universe_df: DataFrame = tb.get_universe_df().await?;
    println!("{}", universe_df.head(Some(10)));

    // Get symbol date for BTC, 2025-10-05
    let test_date = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    let df = tb.clone().symbol_date_df("BTC", test_date).await?;
    let df_clone = df.clone();
    let collected = tokio::task::spawn_blocking(move || df_clone.collect()).await??;
    println!("{}", collected.head(Some(10)));
    println!("{:?}", collected.schema());

    let exprs = cols(["date", "symbol"]).as_expr().first();
    let first_rows = df.select([exprs]);
    let collected = tokio::task::spawn_blocking(move || first_rows.collect()).await??;
    println!("{:#?}", collected);

    fn lazyframe_from_path(
        entry_result: Result<walkdir::DirEntry, walkdir::Error>,
    ) -> Result<LazyFrame> {
        // Handle directory entry error with context
        let entry = entry_result.context("Failed to read directory entry")?;

        // Get the path from the DirEntry
        let path = entry.path();
        let path_str = path.display().to_string();

        // Try to scan the parquet file, adding context with the specific path that failed
        LazyFrame::scan_parquet(PlPath::new(&path_str), ScanArgsParquet::default())
            .with_context(|| format!("Failed to scan parquet file: {}", path_str))
    }

    println!("Hive data path: {:?}", &hive_data_path_clone);

    // Transpose Vec<Result<LazyFrame>> into Result<Vec<LazyFrame>>
    // This will short-circuit on the first error encountered
    let lazyframes: Vec<LazyFrame> = WalkDir::new(&hive_data_path_clone)
        .min_depth(0)
        .into_iter()
        .filter(|p| p.as_ref().unwrap().path().ends_with("data.parquet"))
        .map(lazyframe_from_path)
        .collect::<Result<Vec<_>>>()?; // Transpose Vec<Result<T>> -> Result<Vec<T>>

    println!("Found {} parquet files", lazyframes.len());

    // Concatenate all LazyFrames into a single LazyFrame
    let concatenated = concat(
        lazyframes,
        UnionArgs {
            parallel: true,
            rechunk: false,
            to_supertypes: false,
            ..Default::default()
        },
    )
    .context("Failed to concatenate LazyFrames")?;
    let file_symboldates = tokio::task::spawn_blocking(move || concatenated.collect()).await??;

    println!("Successfully concatenated all files into a single LazyFrame");
    println!("{}", file_symboldates);
    Ok(())
}
