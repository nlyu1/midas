from ..dataset import ByDateDataset
from dataclasses import dataclass
from datetime import date as Date
from timedelta_isoformat import timedelta as Timedelta
from typing import Dict, Optional, List
import polars as pl
from pathlib import Path
from mnemosyne import DatasetType
import logging

logger = logging.getLogger(__name__)

def weighted_mean(price_expr: pl.Expr, volume_expr: pl.Expr, final_name: str) -> pl.Expr :
    """Calculates the volume-weighted average price."""
    return (price_expr.dot(volume_expr) / volume_expr.sum()).alias(final_name)

def grid_columns():
    """Streaming-friendly aggregations"""
    
    # Booleans cast to 0/1, so multiply directly. Don't use filter here, since that will cause memory fallback
    is_taker_buy = ~pl.col('is_buyer_maker')  # Cast to int: True=1, False=0
    is_taker_sell = pl.col('is_buyer_maker')
    
    # --- Metadata ---
    yield pl.col('peg_symbol').last()
    
    # --- OHLCV ---
    yield pl.first('price').alias('open')
    yield pl.max('price').alias('high')
    yield pl.min('price').alias('low')
    yield pl.last('price').alias('close')
    yield pl.sum('quantity').alias('volume')
    yield pl.len().alias('trade_count')
    yield pl.last('time').alias('last_event_time')
    
    # --- Taker Volumes (using multiplication) ---
    # When is_taker_buy=True (1), include quote_quantity; when False (0), contribute 0
    yield (pl.col('quantity') * is_taker_buy).sum().alias('taker_buy_volume')
    yield (pl.col('quantity') * is_taker_sell).sum().alias('taker_sell_volume')
    
    # --- VWAP (adjusted for multiplication) ---
    yield weighted_mean(
        pl.col('price'),
        pl.col('quantity') * is_taker_buy,
        'vwap_taker_buy'
    )
    yield weighted_mean(
        pl.col('price'),
        pl.col('quantity') * is_taker_sell,
        'vwap_taker_sell'
    )
    yield weighted_mean(
        pl.col('price'),
        pl.col('quantity'),
        'vwap_price'
    )

def grid_query(lf: pl.LazyFrame, grid_interval: Timedelta) -> pl.LazyFrame:
    return (
        # Note: it is very important to offset grid-time by grid_interval!
        # truncate does backward rounding
        lf
        .group_by(
            'symbol', 'date',
            (pl.col('time').dt.truncate(every=grid_interval) + grid_interval).alias('time_grid')
        )
        .agg(
            grid_columns()
        ).rename({
            'time_grid': 'time'
        })
    )

@dataclass(kw_only=True)
class BinanceLastTradesGrid(ByDateDataset):
    peg_symbol: str
    grid_interval: Timedelta
    dataset_type: Optional[DatasetType] = None
    src_path: Optional[Path] = None
    parquet_names: str = '*.parquet'

    def __post_init__(self):
        # Convert string to Timedelta if needed (for worker deserialization)
        if isinstance(self.grid_interval, str):
            self.grid_interval = self.grid_interval.isoformat()

        if self.dataset_type is not None:
            # User initialization - compute paths from dataset_type
            self.src_path = Path(self.dataset_type.hive_path(self.peg_symbol))
            self.path = Path(self.dataset_type.grid_hive_path(self.peg_symbol, self.grid_interval.isoformat()))
        else:
            # Worker initialization - paths provided as strings from kwargs
            self.src_path = Path(self.src_path)
            self.path = Path(self.path)
        # Hardcoded: the rust lossless dataset stores in data.parquet
        self.db = pl.scan_parquet(self.src_path / '**/data.parquet', hive_partitioning=True)
        universe_path = self.src_path / 'hive_symbol_date_pairs.parquet'
        if not universe_path.is_file():
            raise RuntimeError(f'Expected lossless dataset at {universe_path}')
        self.universe_df = pl.read_parquet(universe_path)
        # logger.info(f'Reading from {self.src_path}\n writing to {self.path}')
        super().__post_init__()

    def _get_self_kwargs(self) -> Dict:
        """Return this class's kwargs and aggregate parent's."""
        return {
            'src_path': str(self.src_path),
            'peg_symbol': self.peg_symbol,
            'grid_interval': self.grid_interval,
        } | super()._get_self_kwargs()

    def universe(self) -> pl.DataFrame:
        return self.universe_df

    def _compute_partitions(self, dates: List[Date]) -> pl.LazyFrame:
        """Compute grid data for multiple dates. Returns single DataFrame with all dates."""
        # date_filepaths = [
        #     self.path / f'date={str(date)}/*.parquet' 
        #     for date in dates 
        # ]
        # for date in dates:
        #     date_path = self.path / f'date={str(date)}/0.parquet' 
        #     assert date_path.is_file(), f'{date_path} should exist'
        # return pl.concat([
        #     pl.scan_parquet(date_path).with_columns(pl.col('symbol').cast(pl.String)) for date_path in date_filepaths
        # ])

        unified_lf = (
            self.db.filter(pl.col('date').is_in(dates))
            .rename({'qty': 'quantity', 'quote_qty': 'quote_quantity', 'id': 'trade_id'}, strict=False)
        )
        return grid_query(unified_lf, self.grid_interval)