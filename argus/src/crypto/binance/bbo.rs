use super::BinanceStreamable;
use crate::types::{Price, TradeSize, TradingSymbol};
use agora::Agorable;
use agora::utils::OrError;
use chrono::prelude::{DateTime, Utc};
use indoc::writedoc;
use serde::{Deserialize, Serialize};
use std::fmt;
use tungstenite::Utf8Bytes;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BboUpdate {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub update_id: u64,
    pub bid_price: Price,
    pub bid_size: TradeSize,
    pub ask_price: Price,
    pub ask_size: TradeSize,
}

impl fmt::Display for BboUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Binance BboUpdate id {}
                Symbol: {}
                Received: {}
                {:?}, {:?} | {:?}, {:?} 
            )",
            self.update_id,
            self.symbol.to_string(),
            self.received_time,
            self.bid_size,
            self.bid_price,
            self.ask_price,
            self.ask_size
        )
    }
}
impl Agorable for BboUpdate {}

//   "u":400900217,     // order book updateId
//   "s":"BNBUSDT",     // symbol
//   "b":"25.35190000", // best bid price
//   "B":"31.21000000", // best bid qty
//   "a":"25.36520000", // best ask price
//   "A":"40.66000000"  // best ask qty
#[derive(Deserialize)]
struct RawBboUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "u")]
    orderbook_update_id: u64,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "B")]
    bid_size: String,
    #[serde(rename = "a")]
    ask_price: String,
    #[serde(rename = "A")]
    ask_size: String,
}

impl BinanceStreamable for BboUpdate {
    fn of_json_bytes(msg: Utf8Bytes) -> OrError<Self> {
        let received_time = Utc::now();
        let msg_ref: &[u8] = msg.as_ref();
        let raw: RawBboUpdate = serde_json::from_slice(msg_ref).map_err(
            |e| {format!(
                "Argus Binance tradeUpdate conversion error: cannot convert {} into RawTradeUpdate struct. Check schema. {}", 
                msg, e
            )}
        )?;
        let bbo_update = BboUpdate {
            symbol: TradingSymbol::from_str(&raw.symbol)?,
            received_time: received_time,
            update_id: raw.orderbook_update_id,
            bid_price: Price::from_string(raw.bid_price)?,
            bid_size: TradeSize::from_string(raw.bid_size)?,
            ask_price: Price::from_string(raw.ask_price)?,
            ask_size: TradeSize::from_string(raw.ask_size)?,
        };
        Ok(bbo_update)
    }

    fn websocket_suffix() -> String {
        String::from("@bookTicker")
    }

    fn payload_identifier() -> String {
        String::from("bbo")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}
