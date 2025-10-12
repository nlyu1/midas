from pathlib import Path

HOME_PATH = Path.home()

# Assumes that there'll be 'asset_ctxs', 'market_data', 'trades' beneath
LIB_CACHE_PATH = HOME_PATH / "Data/mnemosyne/cache"

# BINANCE_DATA_PATH / {spot | perp} / {data_type} will be
BINANCE_DATA_PATH = HOME_PATH / "Data/mnemosyne/binance"
