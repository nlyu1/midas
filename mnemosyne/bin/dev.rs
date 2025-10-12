use anyhow::Result;
use chrono::NaiveDate;
use mnemosynme::constants::{BINANCE_DATA_PATH, home_path};
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

    tb.initialize_universe(true).await?;
    let universe_df = tb.get_universe_df().await?;
    println!("{}", universe_df.head(Some(10)));
}
