"""
CTDP (Cumulative Total-Dependence Plot) Module

A flexible, modular library for creating cumulative total-dependence plots.

Three-level API:

1. Functional (stateless):
   >>> from atlas.ctdp import plot_ctdp
   >>> import polars as pl
   >>> plots = plot_ctdp(
   ...     df,
   ...     accum_cols=[pl.col('y')],
   ...     feature_cols=[pl.col('x'), pl.col('x').log().alias('log_x')],
   ...     weight_col=pl.col('w'),
   ... )

2. Stateful (persistent cache):
   >>> from atlas.ctdp import create_ctdp_cache, CTDPCache
   >>> path = create_ctdp_cache(df, accum_cols=[pl.col('y')], ...)
   >>> # Returns: '/data/atlas/ctdp_20251021_143022'
   >>> cache = CTDPCache(path)
   >>> cache.update_filter('x', 0, 10)
   >>> plots = cache.generate_plots()

3. Interactive app:
   >>> # streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path {path}
"""

from .config import DEFAULT_PLOT_PATH, DEFAULT_N_TICKS, DEFAULT_NUM_COLS
from .plotting import plot_ctdp
from .state import (
    create_ctdp_cache,
    delete_cache,
    CTDPCache,
    CTDPMetadata,
)
from .formatting import detect_col_type, format_value, get_col_range
from .app import run_app

__all__ = [
    # Core functional API
    'plot_ctdp',

    # Stateful API
    'create_ctdp_cache',
    'delete_cache',
    'CTDPCache',
    'CTDPMetadata',

    # App API
    'run_app',

    # Configuration
    'DEFAULT_PLOT_PATH',
    'DEFAULT_N_TICKS',
    'DEFAULT_NUM_COLS',

    # Utilities
    'detect_col_type',
    'format_value',
    'get_col_range',
]

__version__ = '0.1.0'
