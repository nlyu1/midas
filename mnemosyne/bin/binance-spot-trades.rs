use anyhow::Result;
use chrono::NaiveDate;
use mnemosyne::constants::{BINANCE_DATA_PATH, home_path};
use mnemosyne::crypto::binance::BinanceDataInterface;
use mnemosyne::crypto::binance::BinanceSpotTradeBook;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let base_path = home_path().join(BINANCE_DATA_PATH);
    let raw_data_path = base_path.join("raw/spot/usdt/last_trade");
    let hive_data_path = base_path.join("spot/last_trade/peg_symbol=USDT");
    let data_base_url = "https://data.binance.vision/data/spot/daily";
    let prefix = "spot/daily/trades";
    let binance_data_suffix = "trades";
    let peg_symbol = "USDT";
    let earliest_date = NaiveDate::from_ymd_opt(2022, 1, 1);

    // Initialize BinanceTradeBook
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

    // Initialize universe (no refresh)
    tb.initialize_universe(true).await?;

    // Get universe_df and print head
    let universe_df = tb.get_universe_df().await?;
    println!("Universe shape: {:?}", universe_df.shape());
    println!("\nUniverse head:");
    println!("{}", universe_df.head(Some(10)));

    // Get symbol date for BTC, 2025-10-05
    let test_date = NaiveDate::from_ymd_opt(2025, 10, 5).unwrap();
    let df = tb.clone().symbol_date_df("BTC", test_date).await?;

    // Wrap .collect() in spawn_blocking
    let collected = tokio::task::spawn_blocking(move || df.collect()).await??;
    println!("\nBTC 2025-10-05 shape: {:?}", collected.shape());
    println!("{}", collected.head(Some(5)));

    let update_stats = tb.update_universe(16).await?;
    println!("{:?}", update_stats);
    Ok(())
}
