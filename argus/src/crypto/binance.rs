mod bbo;
mod orderbook;
mod publisher;
mod trades;

use crate::types::TradingSymbol;
use agora::Agorable;
use agora::utils::OrError;
use tungstenite::Utf8Bytes;

// Data from websocket endpoints
pub trait BinanceStreamable: Agorable + Sized {
    fn of_json_bytes(msg: Utf8Bytes) -> OrError<Self>;
    fn websocket_suffix() -> String;
    fn payload_identifier() -> String;
    fn symbol(&self) -> TradingSymbol; // Returns the symbol of current payload
}

// Data from rest api
pub trait BinanceRest: Agorable + Sized {
    fn of_json_bytes(
        msg: Utf8Bytes,
        symbol: &str,
        request_time: chrono::DateTime<chrono::Utc>,
    ) -> OrError<Self>;
    fn rest_suffix() -> String;
}

pub use bbo::BboUpdate;
pub use orderbook::{OrderbookDepthUpdate, OrderbookDiffUpdate};
pub use publisher::{BinanceWebstreamSymbols, BinanceWebstreamWorker};
pub use trades::TradeUpdate;
