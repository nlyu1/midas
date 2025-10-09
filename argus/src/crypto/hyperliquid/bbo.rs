use super::HyperliquidStreamable;
use crate::scribe::ArgusParquetable;
use crate::types::{Price, TradeSize, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BboUpdate {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub time: DateTime<Utc>,
    pub bid_price: Price,
    pub bid_size: TradeSize,
    pub bid_orders: u32,
    pub ask_price: Price,
    pub ask_size: TradeSize,
    pub ask_orders: u32,
}

impl fmt::Display for BboUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Hyperliquid BboUpdate
                Symbol: {}
                Received: {}
                Time: {}
                {:?} x {:?} ({} orders) | {:?} x {:?} ({} orders)
            )",
            self.symbol.to_string(),
            self.received_time,
            self.time,
            self.bid_size,
            self.bid_price,
            self.bid_orders,
            self.ask_price,
            self.ask_size,
            self.ask_orders
        )
    }
}
impl Agorable for BboUpdate {}

/// Intermediate structs for deserializing Hyperliquid BBO data
/// Format:
/// {
///   "coin": "BTC",
///   "time": 1672515782136,
///   "bbo": [
///     {"px": "50000.0", "sz": "1.5", "n": 3},  // bid
///     {"px": "50001.0", "sz": "2.0", "n": 5}   // ask
///   ]
/// }
#[derive(Deserialize)]
struct WsLevel {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Deserialize)]
struct RawBboUpdate {
    #[allow(dead_code)]
    coin: String,
    time: u64,
    bbo: [Option<WsLevel>; 2],
}

impl HyperliquidStreamable for BboUpdate {
    fn of_channel_data(data: serde_json::Value, coin: &str) -> OrError<Self> {
        let received_time = Utc::now();
        let raw: RawBboUpdate = serde_json::from_value(data).map_err(|e| {
            format!(
                "Argus Hyperliquid BboUpdate conversion error: cannot convert data into RawBboUpdate struct. Check schema. {}",
                e
            )
        })?;

        // Extract bid and ask from the bbo array
        let bid = raw.bbo[0]
            .as_ref()
            .ok_or("Argus Hyperliquid BboUpdate: missing bid level")?;
        let ask = raw.bbo[1]
            .as_ref()
            .ok_or("Argus Hyperliquid BboUpdate: missing ask level")?;

        let bbo_update = BboUpdate {
            symbol: TradingSymbol::from_str(coin)?,
            received_time,
            time: DateTime::from_timestamp_millis(raw.time as i64).ok_or("Invalid timestamp")?,
            bid_price: Price::from_string(bid.px.clone())?,
            bid_size: TradeSize::from_string(bid.sz.clone())?,
            bid_orders: bid.n,
            ask_price: Price::from_string(ask.px.clone())?,
            ask_size: TradeSize::from_string(ask.sz.clone())?,
            ask_orders: ask.n,
        };
        Ok(bbo_update)
    }

    fn subscription_type() -> String {
        String::from("bbo")
    }

    fn payload_identifier() -> String {
        String::from("bbo")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

impl ArgusParquetable for BboUpdate {
    fn arrow_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new(
                "received_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("bid_price", DataType::Float64, false),
            Field::new("bid_size", DataType::Float64, false),
            Field::new("bid_orders", DataType::UInt32, false),
            Field::new("ask_price", DataType::Float64, false),
            Field::new("ask_size", DataType::Float64, false),
            Field::new("ask_orders", DataType::UInt32, false),
        ]))
    }

    fn to_record_batch(data: Vec<Self>) -> OrError<arrow::record_batch::RecordBatch> {
        use arrow::array::{
            ArrayRef, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
        };
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Self::arrow_schema();

        let symbols: ArrayRef = Arc::new(StringArray::from(
            data.iter()
                .map(|d| d.symbol.to_string())
                .collect::<Vec<_>>(),
        ));

        let received_times: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(
                data.iter()
                    .map(|d| d.received_time.timestamp_millis())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );

        let times: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(
                data.iter()
                    .map(|d| d.time.timestamp_millis())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );

        let bid_prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.bid_price.to_f64())
                .collect::<Vec<_>>(),
        ));

        let bid_sizes: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.bid_size.to_f64()).collect::<Vec<_>>(),
        ));

        let bid_orders: ArrayRef = Arc::new(UInt32Array::from(
            data.iter().map(|d| d.bid_orders).collect::<Vec<_>>(),
        ));

        let ask_prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.ask_price.to_f64())
                .collect::<Vec<_>>(),
        ));

        let ask_sizes: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.ask_size.to_f64()).collect::<Vec<_>>(),
        ));

        let ask_orders: ArrayRef = Arc::new(UInt32Array::from(
            data.iter().map(|d| d.ask_orders).collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(
            schema.clone(),
            vec![
                symbols,
                received_times,
                times,
                bid_prices,
                bid_sizes,
                bid_orders,
                ask_prices,
                ask_sizes,
                ask_orders,
            ],
        )
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
