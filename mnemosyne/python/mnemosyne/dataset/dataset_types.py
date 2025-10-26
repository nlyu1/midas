"""
Pure Python implementation of DatasetType enum.
This replaces the Rust version to enable pickling for parallel computation.
"""
from enum import Enum


def lib_cache_path() -> str:
    return "/bigdata/mnemosyne/cache"


def binance_data_path() -> str:
    """Where binance grid & lossless data are stored"""
    return "/bigdata/mnemosyne/binance"


def binance_study_data_path() -> str:
    """Where binance grid data should be stored"""
    return "/data/mnemosyne/binance"


def hyperliquid_data_path() -> str:
    return "/bigdata/mnemosyne/hyperliquid"


def hyperliquid_study_data_path() -> str:
    return "/data/mnemosyne/hyperliquid"


class DatasetType(Enum):
    """
    Dataset type enumeration matching the Rust implementation.
    This pure Python version is picklable for parallel computation.
    """
    BinanceSpotTrades = "BinanceSpotTrades"
    BinanceUmPerpTrades = "BinanceUmPerpTrades"
    HyperliquidPerpL2 = "HyperliquidPerpL2"
    HyperliquidPerpTrades = "HyperliquidPerpTrades"

    def hive_path(self, peg_symbol: str) -> str:
        """
        Where "lossless" processed data is stored.
        Matches Rust implementation in src/datasets.rs
        """
        if self == DatasetType.BinanceSpotTrades:
            return f"{binance_data_path()}/lossless/spot/last_trade/peg_symbol={peg_symbol}"
        elif self == DatasetType.BinanceUmPerpTrades:
            return f"{binance_data_path()}/lossless/futures/um/last_trade/peg_symbol={peg_symbol}"
        elif self == DatasetType.HyperliquidPerpL2:
            return f"{hyperliquid_data_path()}/lossless/l2"
        elif self == DatasetType.HyperliquidPerpTrades:
            return f"{hyperliquid_data_path()}/lossless/last_trade"
        else:
            raise ValueError(f"Unknown DatasetType: {self}")

    def raw_data_path(self, peg_symbol: str) -> str:
        """
        Where raw, downloaded data is stored.
        Matches Rust implementation in src/datasets.rs
        """
        if self == DatasetType.BinanceSpotTrades:
            return f"{binance_data_path()}/raw/spot/last_trade/peg_symbol={peg_symbol}"
        elif self == DatasetType.BinanceUmPerpTrades:
            return f"{binance_data_path()}/raw/trades"
        elif self == DatasetType.HyperliquidPerpL2:
            return f"{hyperliquid_data_path()}/raw/futures/market_data"
        elif self == DatasetType.HyperliquidPerpTrades:
            return f"{hyperliquid_data_path()}/raw/futures/trades"
        else:
            raise ValueError(f"Unknown DatasetType: {self}")

    def grid_hive_path(self, peg_symbol: str, grid_interval: str) -> str:
        """
        Where gridded data should be stored.
        Matches Rust implementation in src/datasets.rs
        """
        if self == DatasetType.BinanceSpotTrades:
            return f"{binance_study_data_path()}/grids/spot/last_trade/{grid_interval}/peg_symbol={peg_symbol}"
        elif self == DatasetType.BinanceUmPerpTrades:
            return f"{binance_study_data_path()}/grids/futures/um/last_trade/{grid_interval}/peg_symbol={peg_symbol}"
        elif self == DatasetType.HyperliquidPerpL2:
            return f"{hyperliquid_study_data_path()}/grids/l2/{grid_interval}"
        elif self == DatasetType.HyperliquidPerpTrades:
            return f"{hyperliquid_study_data_path()}/grids/last_trade/{grid_interval}"
        else:
            raise ValueError(f"Unknown DatasetType: {self}")

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name
