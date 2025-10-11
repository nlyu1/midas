# %%

import polars as pl
import matplotlib.pyplot as plt
import requests
from pathlib import Path

# %% Spot data

from mnemosyne import printv
from mnemosyne.crypto.binance.s3_helpers import get_all_trade_pairs, get_all_keys

# %%


# ----------------------
# Helper functions for symbol file retrieval
# (These use the helper functions above.)
# ----------------------
def get_binance_usdt_symbols(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
    prefix="spot/daily/trades",
):
    """
    Returns a list of universe symbols (tickers) by processing the S3 trade pairs.
    (Strips 'USDT' from trade pairs.)
    """
    trade_pairs = get_all_trade_pairs(base_url, prefix)
    # Filter for trade pairs that end with 'USDT' and remove 'USDT'
    return list(
        map(
            lambda x: x.replace("USDT", ""),
            filter(lambda x: x.endswith("USDT"), trade_pairs),
        )
    )


def get_binance_symbol_files(
    symbol="BTC", base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
):
    """
    Returns a list of object keys for a given symbol (e.g., "BTC") by filtering out .CHECKSUM files.

    Args:
        symbol (str): Ticker symbol (without "USDT")
        base_url (str): S3 endpoint.

    Returns:
        list[str]: List of object keys for that symbol.
    """
    prefix = f"spot/daily/trades/{symbol}USDT"
    keys = get_all_keys(base_url, prefix)
    return list(filter(lambda x: not x.endswith(".CHECKSUM"), keys))


# %%

spot_last_trade_symbols = get_binance_usdt_symbols()
futures_last_trade_symbols = get_binance_usdt_symbols(prefix="futures/um/daily/trades")
