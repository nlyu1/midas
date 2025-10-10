mod bbo;
mod orderbook;
mod perp_context;
mod publisher;
mod scribe; 
mod spot_context;
mod trades;
mod universe;
pub mod webstream;

use crate::types::TradingSymbol;
use agora::Agorable;
use agora::utils::OrError;
use bimap::BiMap;
use serde::Deserialize;

pub trait HyperliquidStreamable: Agorable + Sized {
    /// Parse channel data into one or more instances.
    /// Each implementation extracts symbol from its data structure, translates via symbol_map,
    /// and embeds the normalized symbol in returned items.
    /// Returns Vec<Self>: single-object types return vec![item], array types return all items.
    fn of_channel_data(
        data: serde_json::Value,
        symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Vec<Self>>;

    fn subscription_type() -> String;
    fn payload_identifier() -> String;
    fn symbol(&self) -> TradingSymbol;
}

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
