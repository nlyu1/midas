use crate::tokio_tungstenite::protocol::Utf8Bytes;
use crate::types::{TradingSymbol, UtcNanoTimestamp};

// See docs.binance.us/#all-market-24h-change-stream

//   "u":400900217,     // order book updateId
//   "s":"BNBUSDT",     // symbol
//   "b":"25.35190000", // best bid price
//   "B":"31.21000000", // best bid qty
//   "a":"25.36520000", // best ask price
//   "A":"40.66000000"  // best ask qty
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BboUpdate {
    pub symbol: TradingSymbol,
    pub received_time: UtcNanoTimestamp,
    pub update_id: UtcNanoTimestamp,
    pub bid_price: Price,
    pub bid_sz: TradeSize,
    pub ask_price: Price,
    pub ask_sz: TradeSize,
}

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeUpdate {
    pub symbol: TradingSymbol,
    pub event_time: UtcNanoTimestamp,
    pub received_time: UtcNanoTimestamp,
    pub trade_id: u64,
    pub price: Price,
    pub size: TradeSize,
    pub buyer_order_id: u64,
    pub seller_order_id: u64,
    pub trade_time: UtcNanoTimestamp,
    pub is_market_maker: bool,
}

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
#[derive(Debug, Clone, PartialEq)]
pub struct OrderbookDiffUpdate {
    pub symbol: TradingSymbol,
    pub event_time: UtcNanoTimestamp,
    pub received_time: UtcNanoTimestamp,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

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
pub struct OrderbookDepthUpdate {
    pub symbol: TradingSymbol,
    pub request_time: UtcNanoTimestamp, // Time the request is sent
    pub received_time: UtcNanoTimestamp,
    pub bid_levels: Vec<PriceLevel>,
    pub ask_levels: Vec<PriceLevel>,
}

pub trait Streamable: Agorable + Sized {
    pub fn of_json_bytes(msg: Utf8Bytes) -> OrError<Self>;
    pub fn prefix() -> String;
}
