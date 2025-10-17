use anyhow::Result;
use chrono::NaiveDate;
use clap::{ArgAction, Parser};
use mnemosyne::crypto::CryptoDataInterface;
use mnemosyne::crypto::binance::BinanceUmFuturesTradeBook;
use mnemosyne::datasets::DatasetType;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about = "Binance UM Futures Trades data manager", long_about = None)]
struct Args {
    /// Peg symbol (currency unit)
    #[arg(long, default_value = "USDT")]
    peg_symbol: String,

    /// Earliest date to fetch (YYYY-MM-DD)
    #[arg(long, default_value = "2022-01-01")]
    earliest_date: String,

    /// Recompute universe from S3. Pass this flag to toggle recomputation
    #[arg(long, action=ArgAction::SetTrue)]
    recompute_universe: bool,

    /// Recompute on-hive symbol dates
    #[arg(long, action=ArgAction::SetTrue)]
    recompute_onhive: bool,

    /// Test symbol for demo query
    #[arg(long, default_value = "BTC")]
    test_symbol: String,

    /// Test date for demo query (YYYY-MM-DD)
    #[arg(long, default_value = "2025-10-05")]
    test_date: String,

    /// Parallelism for update_universe
    #[arg(long, default_value_t = 32)]
    parallelism: usize,

    /// Skip confirmation prompt
    #[arg(long, default_value_t = false)]
    yes: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("Args:\n{:?}", args);

    let peg_symbol = &args.peg_symbol;
    let dataset_type = DatasetType::BinanceUmPerpTrades;
    let data_base_url = "https://data.binance.vision/data/futures/um/daily";
    let prefix = "futures/um/daily/trades";
    let binance_data_suffix = "trades";

    let raw_data_path = PathBuf::from(dataset_type.raw_data_path(peg_symbol));
    let hive_data_path = PathBuf::from(dataset_type.hive_path(peg_symbol));
    let earliest_date = NaiveDate::parse_from_str(&args.earliest_date, "%Y-%m-%d")
        .expect("Invalid earliest_date format. Use YYYY-MM-DD");

    // Initialize BinanceTradeBook
    let tb = Arc::new(BinanceUmFuturesTradeBook::new(
        hive_data_path,
        raw_data_path,
        data_base_url.to_string(),
        binance_data_suffix.to_string(),
        prefix.to_string(),
        peg_symbol.to_string(),
        Some(earliest_date),
        None,
    )?);

    // Initialize universe
    tb.initialize_universe(args.recompute_universe).await?;

    // Get universe_df and print head
    let universe_df = tb.get_universe_df().await?;
    println!("Universe shape: {:?}", universe_df.shape());
    println!("\nUniverse head:");
    println!("{}", universe_df.head(Some(5)));

    // Get symbol date for test query
    let test_date = NaiveDate::parse_from_str(&args.test_date, "%Y-%m-%d")
        .expect("Invalid test_date format. Use YYYY-MM-DD");
    let df = tb
        .clone()
        .symbol_date_df(&args.test_symbol, test_date)
        .await?;

    // Wrap .collect() in spawn_blocking
    let collected = tokio::task::spawn_blocking(move || df.collect()).await??;
    println!(
        "\n{} {} shape: {:?}",
        args.test_symbol,
        args.test_date,
        collected.shape()
    );
    println!("{}", collected.head(Some(5)));

    let hive_df = tb.hive_symbol_date_pairs(false).await?;
    println!("\nOnhive\n{}", hive_df.head(Some(5)));

    let nohive_pairs = tb.nohive_symbol_date_pairs(false).await?;
    println!("\nMissing\n{}", nohive_pairs);

    if !args.yes {
        println!("\nProceed with update? (press Enter)");
        let mut dummy_input = String::new();
        let _ = std::io::stdin().read_line(&mut dummy_input);
    }

    let update_stats = tb
        .update_universe(args.parallelism, args.recompute_onhive)
        .await?;
    println!("{:?}", update_stats);
    Ok(())
}
