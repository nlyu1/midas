"""
CTDP (Cumulative Total-Dependence Plot) Module

A flexible, modular library for creating cumulative total-dependence plots.

Three-level API:

1. Functional (stateless):
   >>> from atlas.ctdp import plot_ctdp
   >>> plots = plot_ctdp(df, accum_cols=['y'], feature_cols=['x'], weight_col='w')

2. Stateful (persistent cache):
   >>> from atlas.ctdp import create_ctdp
   >>> path = create_ctdp(df, accum_cols=['y'], feature_cols=['x'], weight_col='w')
   >>> # Returns: PosixPath('/data/atlas/ctdp_20250121_143022')

3. Interactive app:
   >>> # streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path {path}
"""

from .config import DEFAULT_PLOT_PATH, DEFAULT_N_TICKS, DEFAULT_NUM_COLS
from .plotting import plot_ctdp
from .state import (
    create_ctdp,
    load_ctdp_metadata,
    load_ctdp_filters,
    load_ctdp_data,
    save_ctdp_filters,
    save_ctdp_metadata,
    generate_plots,
    CTDPMetadata,
)
from .formatting import detect_col_type, format_value, get_col_range
from .app import run_app

__all__ = [
    # Core functional API
    'plot_ctdp',

    # Stateful API
    'create_ctdp',
    'load_ctdp_metadata',
    'load_ctdp_filters',
    'load_ctdp_data',
    'save_ctdp_filters',
    'save_ctdp_metadata',
    'generate_plots',
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
