# %%

from pathlib import Path 
import polars as pl 
# %%

q = pl.scan_parquet('/home/nlyu/Data/argus/hyperliquid/spot/last_trade/**/*.parquet', hive_partitioning=True)

df = q.collect()
df
# %%
!ls /home/nlyu/Data/argus/hyperliquid/perp/orderbook
# %%
