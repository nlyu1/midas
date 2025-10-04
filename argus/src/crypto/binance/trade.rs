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
pub struct TradeUpdate {
    pub symbol: TradingSymbol,
    pub event_time: DateTime<Utc>,
    pub received_time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: Price,
    pub size: TradeSize,
    pub buyer_order_id: u64,
    pub seller_order_id: u64,
    pub trade_time: DateTime<Utc>,
    pub is_bid_quote: bool,
}

impl fmt::Display for TradeUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writedoc!(
            f,
            "(
                Binance TradeUpdate id {}
                Event time: {},
                Received: {}, 
                Trade time: {}, 
                Symbol: {:?}, 
                Price-size: {:?} x {:?}, 
                (buyer, seller) order id: ({}, {}),
                Quote side: {}
            )",
            self.trade_id,
            self.event_time,
            self.received_time,
            self.trade_time,
            self.symbol,
            self.price,
            self.size,
            self.buyer_order_id,
            self.seller_order_id,
            (if self.is_bid_quote { "Bid" } else { "Offer" })
        )
    }
}
impl Agorable for TradeUpdate {}

/// Intermediate struct for deserializing the raw JSON payload from Binance.
/// Field names match the JSON keys using serde attributes.
//     "e": "trade",     // Event type
//     "E": 1672515782136,   // Event time
//     "s": "BNBBTC",    // Symbol
//     "t": 12345,       // Trade ID
//     "p": "0.001",     // Price
//     "q": "100",       // Quantity
//     "b": 88,          // Buyer order ID
//     "a": 50,          // Seller order ID
//     "T": 1672515782136,   // Trade time
//     "m": true,        // Is the buyer the market maker?
//     "M": true         // Ignore
#[derive(Deserialize)]
struct RawTradeUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "b")]
    buyer_order_id: u64,
    #[serde(rename = "a")]
    seller_order_id: u64,
    #[serde(rename = "T")]
    trade_time_ms: u64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

impl BinanceStreamable for TradeUpdate {
    fn of_json_bytes(msg: Utf8Bytes) -> OrError<Self> {
        let received_time = Utc::now(); 
        let msg_ref: &[u8] = msg.as_ref(); 
        let raw: RawTradeUpdate = serde_json::from_slice(msg_ref).map_err(
            |e| {format!(
                "Argus Binance tradeUpdate conversion error: cannot convert {} into RawTradeUpdate struct. Check schema. {}", 
                msg, e
            )}
        )?; 
        let price: f64 = raw.price.parse().map_err(|e| {
            format!(
                "Argus Binance tradeUpdate conversion error: parsed price {} cannot be converted to f64. {}",
                raw.price, e 
            )
        })?; 
        let size: f64 = raw.quantity.parse().map_err(|e| {
            format!(
                "Argus Binance tradeUpdate conversion error: parsed size {} cannot be cnoverted to f64. {}",
                raw.price, e 
            )
        })?; 
        let trade_update = TradeUpdate {
            symbol: TradingSymbol::from_str(&raw.symbol)?, 
            event_time: DateTime::from_timestamp_millis(raw.event_time_ms as i64).expect("Invalid event time"), 
            received_time,
            trade_id: raw.trade_id,
            price: Price::from_f64(price)?, 
            size: TradeSize::from_f64(size)?, 
            buyer_order_id: raw.buyer_order_id,
            seller_order_id: raw.seller_order_id,
            trade_time: DateTime::from_timestamp_millis(raw.event_time_ms as i64).expect("Invalid trade time"), 
            // If the buyer is the maker, they had a resting bid order on the book.
            // Therefore, the trade occurred against the bid side.
            is_bid_quote: raw.is_buyer_maker,
        };
        Ok(trade_update)
    }

    fn websocket_suffix() -> String {
        String::from("@trade")
    }

    fn payload_identifier() -> String {
        String::from("last_trade")
    }

    fn symbol(&self) -> TradingSymbol {
        self.symbol.clone()
    }
}
