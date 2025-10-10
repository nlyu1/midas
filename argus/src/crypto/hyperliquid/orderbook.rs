use super::HyperliquidStreamable;
use crate::ArgusParquetable; 
use crate::types::{Price, TradeSize, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use bimap::BiMap;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub time: DateTime<Utc>,
    // Store top levels only to avoid excessive memory
    // Each tuple is (price, size, num_orders)
    pub bid_levels: Vec<(Price, TradeSize, u32)>,
    pub ask_levels: Vec<(Price, TradeSize, u32)>,
}

impl fmt::Display for OrderbookSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Hyperliquid OrderbookSnapshot
                Symbol: {}
                Time: {}
                Received: {}
                Bid levels: {}
                Ask levels: {}
            )",
            self.symbol.to_string(),
            self.time,
            self.received_time,
            self.bid_levels.len(),
            self.ask_levels.len()
        )
    }
}

impl Agorable for OrderbookSnapshot {}

/// Intermediate structs for deserializing Hyperliquid L2 orderbook
/// Format:
/// {
///   "coin": "BTC",
///   "time": 1672515782136,
///   "levels": [
///     [{"px": "50000.0", "sz": "1.5", "n": 3}, ...],  // bids
///     [{"px": "50001.0", "sz": "2.0", "n": 5}, ...]   // asks
///   ]
/// }
#[derive(Deserialize)]
struct WsLevel {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Deserialize)]
struct RawOrderbookSnapshot {
    coin: String, // Used for symbol extraction and mapping
    time: u64,
    levels: [Vec<WsLevel>; 2],
}

impl HyperliquidStreamable for OrderbookSnapshot {
    fn of_channel_data(
        data: serde_json::Value,
        symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Vec<Self>> {
        let received_time = Utc::now();
        let raw: RawOrderbookSnapshot = serde_json::from_value(data).map_err(|e| {
            format!(
                "Argus Hyperliquid OrderbookSnapshot conversion error: cannot convert data into RawOrderbookSnapshot struct. Check schema. {}",
                e
            )
        })?;

        // Extract coin from data and normalize
        let hyperliquid_coin = TradingSymbol::from_str(&raw.coin)?;
        let normalized_symbol = symbol_map
            .get_by_right(&hyperliquid_coin)
            .cloned()
            .unwrap_or(hyperliquid_coin);

        // Parse bid levels
        let mut bid_levels = Vec::new();
        for level in &raw.levels[0] {
            let price = Price::from_string(level.px.clone())?;
            let size = TradeSize::from_string(level.sz.clone())?;
            bid_levels.push((price, size, level.n));
        }

        // Parse ask levels
        let mut ask_levels = Vec::new();
        for level in &raw.levels[1] {
            let price = Price::from_string(level.px.clone())?;
            let size = TradeSize::from_string(level.sz.clone())?;
            ask_levels.push((price, size, level.n));
        }

        let orderbook = OrderbookSnapshot {
            symbol: normalized_symbol,
            received_time,
            time: DateTime::from_timestamp_millis(raw.time as i64).ok_or("Invalid timestamp")?,
            bid_levels,
            ask_levels,
        };

        // Return single item in a vector
        Ok(vec![orderbook])
    }

    fn subscription_type() -> String {
        String::from("l2Book")
    }

    fn payload_identifier() -> String {
        String::from("orderbook")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

impl ArgusParquetable for OrderbookSnapshot {
    fn arrow_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        // Define struct type for price levels: (price, size, n_orders)
        let level_struct = DataType::Struct(
            vec![
                Field::new("price", DataType::Float64, false),
                Field::new("size", DataType::Float64, false),
                Field::new("n_orders", DataType::UInt32, false),
            ]
            .into(),
        );

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
            Field::new(
                "bid_levels",
                DataType::List(Arc::new(Field::new("item", level_struct.clone(), false))),
                false,
            ),
            Field::new(
                "ask_levels",
                DataType::List(Arc::new(Field::new("item", level_struct, false))),
                false,
            ),
        ]))
    }

    fn to_record_batch(data: Vec<Self>) -> OrError<arrow::record_batch::RecordBatch> {
        use arrow::array::{
            ArrayRef, Float64Array, StringArray, StructArray, TimestampMillisecondArray,
            UInt32Array,
        };
        use arrow::datatypes::{DataType, Field};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Self::arrow_schema();

        // Basic fields
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

        // Build bid_levels as List<Struct>
        let level_fields = vec![
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
            Field::new("n_orders", DataType::UInt32, false),
        ];

        let mut bid_prices_all = Vec::new();
        let mut bid_sizes_all = Vec::new();
        let mut bid_orders_all = Vec::new();
        let mut bid_offsets = vec![0i32];

        for record in &data {
            for (price, size, n_orders) in &record.bid_levels {
                bid_prices_all.push(price.to_f64());
                bid_sizes_all.push(size.to_f64());
                bid_orders_all.push(*n_orders);
            }
            bid_offsets.push(bid_prices_all.len() as i32);
        }

        let bid_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("price", DataType::Float64, false)),
                Arc::new(Float64Array::from(bid_prices_all)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("size", DataType::Float64, false)),
                Arc::new(Float64Array::from(bid_sizes_all)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("n_orders", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(bid_orders_all)) as ArrayRef,
            ),
        ]);

        let bid_levels: ArrayRef = Arc::new(
            arrow::array::ListArray::try_new(
                Arc::new(Field::new(
                    "item",
                    DataType::Struct(level_fields.clone().into()),
                    false,
                )),
                arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(bid_offsets)),
                Arc::new(bid_struct),
                None,
            )
            .map_err(|e| format!("Failed to create bid_levels ListArray: {}", e))?,
        );

        // Build ask_levels as List<Struct>
        let mut ask_prices_all = Vec::new();
        let mut ask_sizes_all = Vec::new();
        let mut ask_orders_all = Vec::new();
        let mut ask_offsets = vec![0i32];

        for record in &data {
            for (price, size, n_orders) in &record.ask_levels {
                ask_prices_all.push(price.to_f64());
                ask_sizes_all.push(size.to_f64());
                ask_orders_all.push(*n_orders);
            }
            ask_offsets.push(ask_prices_all.len() as i32);
        }

        let ask_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("price", DataType::Float64, false)),
                Arc::new(Float64Array::from(ask_prices_all)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("size", DataType::Float64, false)),
                Arc::new(Float64Array::from(ask_sizes_all)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("n_orders", DataType::UInt32, false)),
                Arc::new(UInt32Array::from(ask_orders_all)) as ArrayRef,
            ),
        ]);

        let ask_levels: ArrayRef = Arc::new(
            arrow::array::ListArray::try_new(
                Arc::new(Field::new(
                    "item",
                    DataType::Struct(level_fields.into()),
                    false,
                )),
                arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(ask_offsets)),
                Arc::new(ask_struct),
                None,
            )
            .map_err(|e| format!("Failed to create ask_levels ListArray: {}", e))?,
        );

        RecordBatch::try_new(
            schema.clone(),
            vec![symbols, received_times, times, bid_levels, ask_levels],
        )
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
