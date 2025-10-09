use super::HyperliquidStreamable;
use crate::scribe::ArgusParquetable;
use crate::types::{Price, TradeSize, TradingSymbol};
use agora::utils::OrError;
use agora::Agorable;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeUpdate {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: Price,
    pub size: TradeSize,
    pub trade_time: DateTime<Utc>,
    pub is_buy: bool, // true if buyer side (B), false if seller side (A)
}

impl fmt::Display for TradeUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Hyperliquid TradeUpdate id {}
                Received: {},
                Trade time: {},
                Symbol: {:?},
                Price-size: {:?} x {:?},
                Side: {}
            )",
            self.trade_id,
            self.received_time,
            self.trade_time,
            self.symbol,
            self.price,
            self.size,
            (if self.is_buy { "Buy" } else { "Sell" })
        )
    }
}
impl Agorable for TradeUpdate {}

/// Intermediate struct for deserializing individual trade from Hyperliquid.
/// Hyperliquid trades come as array of objects with format:
/// {
///   "coin": "BTC",
///   "side": "B",  // "B" for buy, "A" for sell (from taker's perspective)
///   "px": "50000.0",
///   "sz": "0.01",
///   "time": 1672515782136,  // milliseconds
///   "tid": 12345
/// }
#[derive(Deserialize)]
struct RawTradeUpdate {
    #[allow(dead_code)]
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    tid: u64,
}

impl HyperliquidStreamable for TradeUpdate {
    fn of_channel_data(data: serde_json::Value, coin: &str) -> OrError<Self> {
        let received_time = Utc::now();

        // Hyperliquid sends trades as an array, we need to handle multiple trades
        // For now, we'll return error if it's not a single trade or pick the first
        let trades: Vec<RawTradeUpdate> = serde_json::from_value(data).map_err(|e| {
            format!(
                "Argus Hyperliquid tradeUpdate conversion error: cannot convert data into Vec<RawTradeUpdate>. Check schema. {}",
                e
            )
        })?;

        if trades.is_empty() {
            return Err("Argus Hyperliquid tradeUpdate: empty trades array".to_string());
        }

        // Take the first trade (we'll handle multiple trades in the publisher)
        let raw = &trades[0];

        let price: f64 = raw.px.parse().map_err(|e| {
            format!(
                "Argus Hyperliquid tradeUpdate conversion error: parsed price {} cannot be converted to f64. {}",
                raw.px, e
            )
        })?;

        let size: f64 = raw.sz.parse().map_err(|e| {
            format!(
                "Argus Hyperliquid tradeUpdate conversion error: parsed size {} cannot be converted to f64. {}",
                raw.sz, e
            )
        })?;

        let trade_update = TradeUpdate {
            symbol: TradingSymbol::from_str(coin)?,
            received_time,
            trade_id: raw.tid,
            price: Price::from_f64(price)?,
            size: TradeSize::from_f64(size)?,
            trade_time: DateTime::from_timestamp_millis(raw.time as i64)
                .ok_or("Invalid trade time")?,
            is_buy: raw.side == "B",
        };
        Ok(trade_update)
    }

    fn subscription_type() -> String {
        String::from("trades")
    }

    fn payload_identifier() -> String {
        String::from("last_trade")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

impl ArgusParquetable for TradeUpdate {
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
            Field::new("trade_id", DataType::UInt64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
            Field::new(
                "trade_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("is_buy", DataType::Boolean, false),
        ]))
    }

    fn to_record_batch(data: Vec<Self>) -> OrError<arrow::record_batch::RecordBatch> {
        use arrow::array::{
            ArrayRef, BooleanArray, Float64Array, StringArray, TimestampMillisecondArray,
            UInt64Array,
        };
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Self::arrow_schema();

        // Convert each field to Arrow arrays
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

        let trade_ids: ArrayRef = Arc::new(UInt64Array::from(
            data.iter().map(|d| d.trade_id).collect::<Vec<_>>(),
        ));

        let prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.price.to_f64()).collect::<Vec<_>>(),
        ));

        let sizes: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.size.to_f64()).collect::<Vec<_>>(),
        ));

        let trade_times: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(
                data.iter()
                    .map(|d| d.trade_time.timestamp_millis())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );

        let is_buys: ArrayRef = Arc::new(BooleanArray::from(
            data.iter().map(|d| d.is_buy).collect::<Vec<_>>(),
        ));

        // Create RecordBatch
        RecordBatch::try_new(
            schema.clone(),
            vec![
                symbols,
                received_times,
                trade_ids,
                prices,
                sizes,
                trade_times,
                is_buys,
            ],
        )
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
