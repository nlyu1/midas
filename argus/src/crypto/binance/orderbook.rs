use super::{BinanceRest, BinanceStreamable};
use crate::types::{Price, PriceLevel, TradeSize, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;
use tungstenite::Utf8Bytes;

// "e": "depthUpdate", // Event type
// "E": 1675216573749,     // Event time
// "s": "BNBBTC",      // Symbol
// "U": 157,           // First update ID in event
// "u": 160,           // Final update ID in event
// "b": [              // Bids to be updated
//     [
//     "0.0024",       // Price level to be updated
//     "10"            // Quantity
//     ]
// ],
// "a": [              // Asks to be updated
//     [
//     "0.0026",       // Price level to be updated
//     "100"           // Quantity
//     ]
// ]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderbookDiffUpdate {
    pub symbol: TradingSymbol,
    pub event_time: DateTime<Utc>,
    pub received_time: DateTime<Utc>,
    pub first_update_id: u64,
    pub final_update_id: u64,
    // We don't use TradeSize here because diff updates can have zero size (indicating cleared level).
    pub bids: Vec<(Price, f64)>,
    pub asks: Vec<(Price, f64)>,
}

impl fmt::Display for OrderbookDiffUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Binance OrderbookDiffUpdate
                Symbol: {}
                Event time: {}
                Received: {}
                Update IDs: {} - {}
                Bids: {} levels
                Asks: {} levels
            )",
            self.symbol.to_string(),
            self.event_time,
            self.received_time,
            self.first_update_id,
            self.final_update_id,
            self.bids.len(),
            self.asks.len()
        )
    }
}

impl Agorable for OrderbookDiffUpdate {}

#[derive(Deserialize)]
struct RawOrderbookDiffUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

fn parse_float_from_string(s: String) -> OrError<f64> {
    s.parse().map_err(|e| {
        anyhow::anyhow!(
            "Argus float parsing error: cannot interpret string {} as price: {}",
            s, e
        )
    })
}

impl BinanceStreamable for OrderbookDiffUpdate {
    fn of_json_bytes(msg: Utf8Bytes) -> OrError<Self> {
        let received_time = Utc::now();
        let msg_ref: &[u8] = msg.as_ref();
        let raw: RawOrderbookDiffUpdate = serde_json::from_slice(msg_ref).map_err(
            |e| {anyhow::anyhow!(
                "Argus Binance OrderbookDiffUpdate conversion error: cannot convert {} into RawOrderbookDiffUpdate struct. Check schema. {}",
                msg, e
            )}
        )?;

        // Parse bid levels
        let mut bids = Vec::new();
        for level in raw.bids {
            let price = Price::from_string(level[0].clone())?;
            let size = parse_float_from_string(level[1].clone())?;
            bids.push((price, size));
        }

        // Parse ask levels
        let mut asks = Vec::new();
        for level in raw.asks {
            let price = Price::from_string(level[0].clone())?;
            let size = parse_float_from_string(level[1].clone())?;
            asks.push((price, size));
        }

        let orderbook_diff = OrderbookDiffUpdate {
            symbol: TradingSymbol::from_str(&raw.symbol)?,
            event_time: DateTime::from_timestamp_millis(raw.event_time_ms as i64)
                .ok_or_else(|| anyhow::anyhow!("Invalid event time"))?,
            received_time,
            first_update_id: raw.first_update_id,
            final_update_id: raw.final_update_id,
            bids,
            asks,
        };
        Ok(orderbook_diff)
    }

    fn websocket_suffix() -> String {
        String::from("@depth@100ms")
    }

    fn payload_identifier() -> String {
        String::from("book_diff")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}

// https://docs.binance.us/#get-order-book-depth
// Sample command: curl -X "GET" "https://api.binance.us/api/v3/depth?symbol=BTCUSDT"
// Periodically getting orderbook depth is REST endpoint operation
// "lastUpdateId": 1027024,
// "bids": [
//   [
//     "0.00379200",
//     "31.26000000"
//   ]
// ],
// "asks": [
//   [
//     "0.00380100",
//     "32.37000000"
//   ]
// ]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderbookDepthUpdate {
    pub symbol: TradingSymbol,
    pub request_time: DateTime<Utc>, // Time the request is sent
    pub received_time: DateTime<Utc>,
    pub last_update_id: u64,
    pub bid_levels: Vec<PriceLevel>,
    pub ask_levels: Vec<PriceLevel>,
}

#[derive(Deserialize)]
struct RawOrderbookDepthUpdate {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

impl BinanceRest for OrderbookDepthUpdate {
    fn of_json_bytes(msg: Utf8Bytes, symbol: &str, request_time: DateTime<Utc>) -> OrError<Self> {
        let received_time = Utc::now();
        let msg_ref: &[u8] = msg.as_ref();
        let raw: RawOrderbookDepthUpdate = serde_json::from_slice(msg_ref).map_err(|e| {
            anyhow::anyhow!(
                "Argus Binance OrderbookDepthUpdate conversion error: cannot convert {} into RawOrderbookDepthUpdate struct. Check schema. {}",
                msg, e
            )
        })?;

        // Parse bid levels
        let mut bid_levels = Vec::new();
        for level in raw.bids {
            let price = Price::from_string(level[0].clone())?;
            let size = TradeSize::from_string(level[1].clone())?;
            bid_levels.push((price, size));
        }

        // Parse ask levels
        let mut ask_levels = Vec::new();
        for level in raw.asks {
            let price = Price::from_string(level[0].clone())?;
            let size = TradeSize::from_string(level[1].clone())?;
            ask_levels.push((price, size));
        }

        Ok(OrderbookDepthUpdate {
            symbol: TradingSymbol::from_str(symbol)?,
            request_time,
            received_time,
            last_update_id: raw.last_update_id,
            bid_levels,
            ask_levels,
        })
    }

    fn rest_suffix() -> String {
        "depth".to_string()
    }
}

impl Agorable for OrderbookDepthUpdate {}

impl fmt::Display for OrderbookDepthUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Binance OrderbookDepthUpdate")?;
        writeln!(f, "Symbol: {}", self.symbol.to_string())?;
        writeln!(f, "Last Update ID: {}", self.last_update_id)?;
        writeln!(f, "Request time: {}", self.request_time)?;
        writeln!(f, "Received: {}", self.received_time)?;
        writeln!(f)?;

        // Header
        writeln!(
            f,
            "{:<20} {:<20} {:<20} {:<20}",
            "Bid Size", "Bid Price", "Ask Price", "Ask Size"
        )?;
        writeln!(f, "{}", "-".repeat(80))?;

        // Determine the max number of rows to display
        let max_rows = self.bid_levels.len().max(self.ask_levels.len());

        // Display rows
        for i in 0..max_rows {
            let bid_size = self
                .bid_levels
                .get(i)
                .map(|(_, size)| format!("{:.8}", size.to_f64()))
                .unwrap_or_default();
            let bid_price = self
                .bid_levels
                .get(i)
                .map(|(price, _)| format!("{:.8}", price.to_f64()))
                .unwrap_or_default();
            let ask_price = self
                .ask_levels
                .get(i)
                .map(|(price, _)| format!("{:.8}", price.to_f64()))
                .unwrap_or_default();
            let ask_size = self
                .ask_levels
                .get(i)
                .map(|(_, size)| format!("{:.8}", size.to_f64()))
                .unwrap_or_default();

            writeln!(
                f,
                "{:<20} {:<20} {:<20} {:<20}",
                bid_size, bid_price, ask_price, ask_size
            )?;
        }

        Ok(())
    }
}
