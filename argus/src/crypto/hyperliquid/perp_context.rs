use super::HyperliquidStreamable;
use crate::ArgusParquetable;
use crate::types::{Price, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use anyhow::Context;
use bimap::BiMap;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerpAssetContext {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub mark_price: Price,
    pub mid_price: Option<Price>,
    pub funding_rate: f64,          // As decimal (e.g., 0.0001 = 0.01%)
    pub open_interest: Option<f64>, // In USD
    pub volume_24h: Option<f64>,    // In USD
    pub oracle_price: Option<Price>,
}

impl fmt::Display for PerpAssetContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Hyperliquid PerpAssetContext
                Symbol: {}
                Received: {}
                Mark Price: {:?}
                Mid Price: {:?}
                Funding Rate: {:.6}%
                Open Interest: {:?}
                24h Volume: {:?}
                Oracle Price: {:?}
            )",
            self.symbol.to_string(),
            self.received_time,
            self.mark_price,
            self.mid_price,
            self.funding_rate * 100.0,
            self.open_interest,
            self.volume_24h,
            self.oracle_price
        )
    }
}

impl Agorable for PerpAssetContext {}

/// Intermediate structs for deserializing Hyperliquid active asset context
/// Format:
/// {
///   "coin": "BTC",
///   "ctx": {
///     "markPx": "50000.0",
///     "midPx": "50000.5",
///     "funding": "0.0001",
///     "openInterest": "1000000.0",
///     "dayNtlVlm": "5000000.0",
///     "oraclePx": "50000.2",
///     ...
///   }
/// }
#[derive(Deserialize)]
struct RawAssetCtx {
    #[serde(rename = "markPx")]
    mark_px: String,
    #[serde(rename = "midPx")]
    mid_px: Option<String>,
    funding: String,
    #[serde(rename = "openInterest")]
    open_interest: Option<String>,
    #[serde(rename = "dayNtlVlm")]
    day_ntl_vlm: Option<String>,
    #[serde(rename = "oraclePx")]
    oracle_px: Option<String>,
}

#[derive(Deserialize)]
struct RawAssetContext {
    coin: String, // Used for symbol extraction and mapping
    ctx: RawAssetCtx,
}

impl HyperliquidStreamable for PerpAssetContext {
    fn of_channel_data(
        data: serde_json::Value,
        symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Vec<Self>> {
        let received_time = Utc::now();
        let raw: RawAssetContext = serde_json::from_value(data).map_err(|e| {
            anyhow::anyhow!(
                "Argus Hyperliquid PerpAssetContext conversion error: cannot convert data into RawAssetContext struct. Check schema. {}",
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

        let funding_rate: f64 = raw.ctx.funding.parse().map_err(|e| {
            anyhow::anyhow!(
                "Argus Hyperliquid PerpAssetContext conversion error: cannot parse funding rate {}: {}",
                raw.ctx.funding, e
            )
        })?;

        let open_interest = if let Some(oi_str) = raw.ctx.open_interest {
            Some(oi_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "Argus Hyperliquid PerpAssetContext conversion error: cannot parse open interest {}: {}",
                    oi_str, e
                )
            })?)
        } else {
            None
        };

        let volume_24h = if let Some(vol_str) = raw.ctx.day_ntl_vlm {
            Some(vol_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "Argus Hyperliquid PerpAssetContext conversion error: cannot parse volume {}: {}",
                    vol_str, e
                )
            })?)
        } else {
            None
        };

        let oracle_price = if let Some(oracle_str) = raw.ctx.oracle_px {
            Some(Price::from_string(oracle_str)?)
        } else {
            None
        };

        let context = PerpAssetContext {
            symbol: normalized_symbol,
            received_time,
            mark_price,
            mid_price,
            funding_rate,
            open_interest,
            volume_24h,
            oracle_price,
        };

        // Return single item in a vector
        Ok(vec![context])
    }

    fn subscription_type() -> String {
        String::from("activeAssetCtx")
    }

    fn payload_identifier() -> String {
        String::from("perp_context")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

impl ArgusParquetable for PerpAssetContext {
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
            Field::new("funding_rate", DataType::Float64, false),
            Field::new("open_interest", DataType::Float64, true),
            Field::new("volume_24h", DataType::Float64, true),
            Field::new("oracle_price", DataType::Float64, true),
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

        let funding_rates: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.funding_rate).collect::<Vec<_>>(),
        ));

        let open_interests: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.open_interest).collect::<Vec<_>>(),
        ));

        let volumes_24h: ArrayRef = Arc::new(Float64Array::from(
            data.iter().map(|d| d.volume_24h).collect::<Vec<_>>(),
        ));

        let oracle_prices: ArrayRef = Arc::new(Float64Array::from(
            data.iter()
                .map(|d| d.oracle_price.as_ref().map(|p| p.to_f64()))
                .collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(
            schema.clone(),
            vec![
                symbols,
                received_times,
                mark_prices,
                mid_prices,
                funding_rates,
                open_interests,
                volumes_24h,
                oracle_prices,
            ],
        )
        .context("Failed to create RecordBatch")
    }
}
