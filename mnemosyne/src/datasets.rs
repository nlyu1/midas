use pyo3::prelude::*;

pub fn lib_cache_path() -> String {
    String::from("/bigdata/mnemosyne/cache")
}

pub fn binance_data_path() -> String {
    // Where binance grid & lossless data are stored
    String::from("/bigdata/mnemosyne/binance")
}

pub fn binance_study_data_path() -> String {
    // Where binance grid data should be stored
    String::from("/data/mnemosyne/binance")
}

pub fn hyperliquid_data_path() -> String {
    String::from("/bigdata/mnemosyne/hyperliquid")
}

pub fn hyperliquid_study_data_path() -> String {
    String::from("/data/mnemosyne/hyperliquid")
}

#[pyclass(eq, eq_int)]
#[derive(PartialEq, Debug)]
pub enum DatasetType {
    BinanceSpotTrades,
    BinanceUmPerpTrades,
    HyperliquidPerpL2,
    // HyperliquidAssetCtx,
    HyperliquidPerpTrades,
}

#[pymethods]
impl DatasetType {
    pub fn hive_path(&self, peg_symbol: &str) -> String {
        // Where "lossless" processed data is stored
        // assert!((peg_symbol == "USDT") || (peg_symbol == "USDC"));
        match self {
            Self::BinanceSpotTrades => format!(
                "{}/lossless/spot/last_trade/peg_symbol={}",
                binance_data_path(),
                peg_symbol
            ),
            Self::BinanceUmPerpTrades => format!(
                "{}/lossless/futures/um/last_trade/peg_symbol={}",
                binance_data_path(),
                peg_symbol
            ),
            Self::HyperliquidPerpL2 => format!("{}/lossless/l2", hyperliquid_data_path()),
            Self::HyperliquidPerpTrades => {
                format!("{}/lossless/last_trade", hyperliquid_data_path())
            }
        }
    }

    pub fn raw_data_path(&self, peg_symbol: &str) -> String {
        // Where raw, downloaded data is stored
        // assert!((peg_symbol == "USDT") || (peg_symbol == "USDC"));
        match self {
            Self::BinanceSpotTrades => format!(
                "{}/raw/spot/last_trade/peg_symbol={}",
                binance_data_path(),
                peg_symbol
            ),
            Self::BinanceUmPerpTrades => format!("{}/raw/trades", binance_data_path(),),
            Self::HyperliquidPerpL2 => {
                format!("{}/raw/futures/market_data", hyperliquid_data_path(),)
            }
            Self::HyperliquidPerpTrades => {
                format!("{}/raw/futures/trades", hyperliquid_data_path(),)
            }
        }
    }

    pub fn grid_hive_path(&self, peg_symbol: &str, grid_interval: &str) -> String {
        // Where gridded data should be stored
        match self {
            Self::BinanceSpotTrades => format!(
                "{}/grids/spot/last_trade/{}/peg_symbol={}",
                binance_study_data_path(),
                grid_interval,
                peg_symbol
            ),
            Self::BinanceUmPerpTrades => format!(
                "{}/grids/futures/um/last_trade/{}/peg_symbol={}",
                binance_study_data_path(),
                grid_interval,
                peg_symbol
            ),
            Self::HyperliquidPerpL2 => format!(
                "{}/grids/l2/{}",
                hyperliquid_study_data_path(),
                grid_interval,
            ),
            Self::HyperliquidPerpTrades => format!(
                "{}/grids/last_trade/{}",
                hyperliquid_study_data_path(),
                grid_interval,
            ),
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}
