use super::HyperliquidStreamable;
use crate::ArgusParquetable;
use crate::types::{Price, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use bimap::BiMap;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpotAssetContext {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub mark_price: Price,
    pub mid_price: Option<Price>,
    pub volume_24h: Option<f64>,         // In USD
    pub circulating_supply: Option<f64>, // Spot-specific
    pub total_supply: Option<f64>,       // Spot-specific
}

impl fmt::Display for SpotAssetContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Hyperliquid SpotAssetContext
                Symbol: {}
                Received: {}
                Mark Price: {:?}
                Mid Price: {:?}
                24h Volume: {:?}
                Circulating Supply: {:?}
                Total Supply: {:?}
            )",
            self.symbol.to_string(),
            self.received_time,
            self.mark_price,
            self.mid_price,
            self.volume_24h,
            self.circulating_supply,
            self.total_supply
        )
    }
}

impl Agorable for SpotAssetContext {}

/// Intermediate structs for deserializing Hyperliquid spot asset context
/// Format:
/// {
///   "coin": "PURR/USDC",
///   "ctx": {
///     "markPx": "0.05",
///     "midPx": "0.051",
///     "dayNtlVlm": "500000.0",
///     "circulatingSupply": "1000000000",
///     "totalSupply": "10000000000",
///     ...
///   }
/// }
#[derive(Deserialize)]
struct RawSpotAssetCtx {
    #[serde(rename = "markPx")]
    mark_px: String,
    #[serde(rename = "midPx")]
    mid_px: Option<String>,
    #[serde(rename = "dayNtlVlm")]
    day_ntl_vlm: Option<String>,
    #[serde(rename = "circulatingSupply")]
    circulating_supply: Option<String>,
    #[serde(rename = "totalSupply")]
    total_supply: Option<String>,
}

// Context, bbo, orderbook, last-trade

#[derive(Deserialize)]
struct RawSpotAssetContext {
    coin: String, // Used for symbol extraction and mapping
    ctx: RawSpotAssetCtx,
}

impl HyperliquidStreamable for SpotAssetContext {
    fn of_channel_data(
        data: serde_json::Value,
        symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Vec<Self>> {
        let received_time = Utc::now();
        let raw: RawSpotAssetContext = serde_json::from_value(data).map_err(|e| {
            format!(
                "Argus Hyperliquid SpotAssetContext conversion error: cannot convert data into RawSpotAssetContext struct. Check schema. {}",
                e
            )
        })?;

        // Extract coin from data and normalize
        let hyperliquid_coin = TradingSymbol::from_str(&raw.coin)?;
        let normalized_symbol = symbol_map
            .get_by_right(&hyperliquid_coin)
            .cloned()
            .unwrap_or(hyperliquid_coin);

        let mark_price = Price::from_string(raw.ctx.mark_px)?;

        let mid_price = if let Some(mid_px_str) = raw.ctx.mid_px {
            Some(Price::from_string(mid_px_str)?)
        } else {
            None
        };

        let volume_24h = if let Some(vol_str) = raw.ctx.day_ntl_vlm {
            Some(vol_str.parse().map_err(|e| {
                format!(
                    "Argus Hyperliquid SpotAssetContext conversion error: cannot parse volume {}: {}",
                    vol_str, e
                )
            })?)
        } else {
            None
        };

        let circulating_supply = if let Some(supply_str) = raw.ctx.circulating_supply {
            Some(supply_str.parse().map_err(|e| {
                format!(
                    "Argus Hyperliquid SpotAssetContext conversion error: cannot parse circulating supply {}: {}",
                    supply_str, e
                )
            })?)
        } else {
            None
        };

        let total_supply = if let Some(supply_str) = raw.ctx.total_supply {
            Some(supply_str.parse().map_err(|e| {
                format!(
                    "Argus Hyperliquid SpotAssetContext conversion error: cannot parse total supply {}: {}",
                    supply_str, e
                )
            })?)
        } else {
            None
        };

        let context = SpotAssetContext {
            symbol: normalized_symbol,
            received_time,
            mark_price,
            mid_price,
            volume_24h,
            circulating_supply,
            total_supply,
        };

        // Return single item in a vector
        Ok(vec![context])
    }

    fn subscription_type() -> String {
        String::from("activeAssetCtx")
    }

    fn payload_identifier() -> String {
        String::from("spot_context")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

impl ArgusParquetable for SpotAssetContext {
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
            Field::new("mark_price", DataType::Float64, false),
            Field::new("mid_price", DataType::Float64, true),
            Field::new("volume_24h", DataType::Float64, true),
            Field::new("circulating_supply", DataType::Float64, true),
            Field::new("total_supply", DataType::Float64, true),
        ]))
    }

    fn to_record_batch(data: Vec<Self>) -> OrError<arrow::record_batch::RecordBatch> {
        use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
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

        let mark_prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.mark_price.to_f64())
                .collect::<Vec<_>>(),
        ));

        let mid_prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.mid_price.as_ref().map(|p| p.to_f64()))
                .collect::<Vec<_>>(),
        ));

        let volumes_24h: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.volume_24h).collect::<Vec<_>>(),
        ));

        let circulating_supplies: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.circulating_supply)
                .collect::<Vec<_>>(),
        ));

        let total_supplies: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.total_supply).collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(
            schema.clone(),
            vec![
                symbols,
                received_times,
                mark_prices,
                mid_prices,
                volumes_24h,
                circulating_supplies,
                total_supplies,
            ],
        )
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
