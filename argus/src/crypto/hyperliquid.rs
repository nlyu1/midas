mod bbo;
mod orderbook;
mod perp_context;
mod publisher;
mod scribe {
    pub use super::writer::*;
}
mod writer;
mod spot_context;
mod trades;
mod universe;
pub mod webstream;

use crate::types::TradingSymbol;
use agora::Agorable;
use agora::utils::OrError;
use serde::Deserialize;

// Data from websocket endpoints
pub trait HyperliquidStreamable: Agorable + Sized {
    fn of_channel_data(data: serde_json::Value, coin: &str) -> OrError<Self>;
    fn subscription_type() -> String;
    fn payload_identifier() -> String;
    fn symbol(&self) -> TradingSymbol; // Returns the symbol of current payload
}

/// Wrapper for Hyperliquid WebSocket messages
/// All messages come in format: {"channel": "...", "data": ...}
#[derive(Deserialize)]
pub struct ChannelMessage {
    pub channel: String,
    pub data: serde_json::Value,
}

pub use bbo::BboUpdate;
pub use orderbook::OrderbookSnapshot;
pub use perp_context::PerpAssetContext;
pub use publisher::HyperliquidPublisher;
pub use scribe::{HyperliquidArchiver, HyperliquidScribe};
pub use spot_context::SpotAssetContext;
pub use trades::TradeUpdate;
pub use universe::UniverseManager;
pub use webstream::HyperliquidWebstreamWorker;
