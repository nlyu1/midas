use pyo3::prelude::*;

pub fn lib_cache_path() -> String {
    String::from("/bigdata/mnemosyne/cache")
}

pub fn binance_data_path() -> String {
    String::from("/bigdata/mnemosyne/binance")
}

pub fn binance_study_data_path() -> String {
    String::from("/data/mnemosyne/binance")
}

#[pyclass(eq, eq_int)]
#[derive(PartialEq, Debug)]
pub enum DatasetType {
    BinanceSpotTrades,
    BinanceUmPerpTrades,
    HyperliquidPerpL2,
    HyperliquidPerpTrades,
}

#[pymethods]
impl DatasetType {
    pub fn hive_path(&self, peg_symbol: &str) -> String {
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
            Self::HyperliquidPerpL2 => panic!("Not implemented"),
            Self::HyperliquidPerpTrades => panic!("Not implemented"),
        }
    }

    pub fn raw_data_path(&self, peg_symbol: &str) -> String {
        // assert!((peg_symbol == "USDT") || (peg_symbol == "USDC"));
        match self {
            Self::BinanceSpotTrades => format!(
                "{}/raw/spot/last_trade/peg_symbol={}",
                binance_data_path(),
                peg_symbol
            ),
            Self::BinanceUmPerpTrades => format!(
                "{}/raw/futures/um/last_trade/peg_symbol={}",
                binance_data_path(),
                peg_symbol
            ),
            Self::HyperliquidPerpL2 => panic!("Not implemented"),
            Self::HyperliquidPerpTrades => panic!("Not implemented"),
        }
    }

    // Raw data & lossless data lives in /bigdata
    // Gridded data lives in /data
    pub fn grid_hive_path(&self, peg_symbol: &str, grid_interval: &str) -> String {
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
            Self::HyperliquidPerpL2 => panic!("Not implemented"),
            Self::HyperliquidPerpTrades => panic!("Not implemented"),
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}
