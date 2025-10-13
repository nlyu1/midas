use std::path::PathBuf;

/// Home directory - to be expanded at runtime
pub fn home_path() -> PathBuf {
    PathBuf::from("/")
}

/// Assumes that there'll be 'asset_ctxs', 'market_data', 'trades' beneath
pub const LIB_CACHE_PATH: &str = "Data/mnemosyne/cache";

/// BINANCE_DATA_PATH / {spot | perp} / {data_type} will be
pub const BINANCE_DATA_PATH: &str = "Data/mnemosyne/binance";
