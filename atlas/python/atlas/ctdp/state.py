"""State management and persistence for CTDP sessions"""

import json
import shutil
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any, Union
from pathlib import Path
from datetime import datetime

import polars as pl
import plotly.graph_objects as go

from .config import DEFAULT_PLOT_PATH, DEFAULT_N_TICKS, DEFAULT_NUM_COLS


def _normalize_exprs(exprs: Union[List[Union[pl.Expr, str]], pl.Expr, str]) -> List[pl.Expr]:
    """
    Normalize expressions: convert str → pl.col(str), pass through pl.Expr.

    Args:
        exprs: Single expression/string or list of expressions/strings

    Returns:
        List of Polars expressions
    """
    # Handle single expression/string
    if isinstance(exprs, (pl.Expr, str)):
        exprs = [exprs]

    # Convert strings to pl.col()
    return [pl.col(e) if isinstance(e, str) else e for e in exprs]


@dataclass
class CTDPMetadata:
    """
    Metadata for a CTDP session.

    All settings except filters (which are stored separately in filters.parquet).
    Column names are stored as strings (expressions already computed at cache creation).
    """

    accum_cols: List[str]
    feature_cols: List[str]
    weight_col: str
    n_ticks: int = DEFAULT_N_TICKS
    num_cols: int = DEFAULT_NUM_COLS
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    version: str = "0.1.0"

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "CTDPMetadata":
        """Deserialize from dict"""
        return cls(**data)

    def save(self, path: Path):
        """Save metadata to JSON file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "CTDPMetadata":
        """Load metadata from JSON file"""
        with open(path, "r") as f:
            return cls.from_dict(json.load(f))


def create_ctdp_cache(
    df: pl.LazyFrame,
    accum_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    feature_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    weight_col: Union[pl.Expr, str],
    cache_dir: Optional[Path] = None,
    n_ticks: int = DEFAULT_N_TICKS,
    num_cols: int = DEFAULT_NUM_COLS,
) -> str:
    """
    Create CTDP cache with expression evaluation.

    Evaluates expressions, validates uniqueness, and saves only required columns
    to disk using sink_parquet for efficient streaming execution.

    Args:
        df: Input LazyFrame
        accum_cols: Expressions or column names to accumulate
        feature_cols: Expressions or column names to slice by
        weight_col: Expression or column name for weighting
        cache_dir: Cache directory (auto-generated if None)
        n_ticks: Number of x-axis ticks
        num_cols: Number of grid columns for app

    Returns:
        String path to created cache directory

    Example:
        >>> path = create_ctdp_cache(
        ...     df=my_lazyframe,
        ...     accum_cols=[pl.col('null_frac')],
        ...     feature_cols=[pl.col('return'), pl.col('lag').log().alias('log_lag')],
        ...     weight_col=pl.col('weight'),
        ... )
        >>> print(path)
        '/data/atlas/ctdp_20251021_143022'
    """
    # 1. Normalize to expressions
    accum_exprs = _normalize_exprs(accum_cols)
    feature_exprs = _normalize_exprs(feature_cols)
    weight_expr = _normalize_exprs(weight_col)[0]

    # 2. Extract output names
    accum_names = [e.meta.output_name() for e in accum_exprs]
    feature_names = [e.meta.output_name() for e in feature_exprs]
    weight_name = weight_expr.meta.output_name()

    # 3. Validate uniqueness
    all_names = accum_names + feature_names + [weight_name]
    if len(all_names) != len(set(all_names)):
        duplicates = [n for n in set(all_names) if all_names.count(n) > 1]
        raise ValueError(f"Duplicate output names detected: {duplicates}")

    # 4. Create cache directory
    if cache_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cache_dir = DEFAULT_PLOT_PATH / f"ctdp_{timestamp}"

    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 5. Select and sink (STAYS LAZY - streaming execution!)
        print(f"Caching data to {cache_dir / 'data.parquet'}...")
        df.select([*accum_exprs, *feature_exprs, weight_expr]).sink_parquet(
            cache_dir / "data.parquet", compression="lz4"
        )

        # 6. Save metadata (with string names!)
        metadata = CTDPMetadata(
            accum_cols=accum_names,
            feature_cols=feature_names,
            weight_col=weight_name,
            n_ticks=n_ticks,
            num_cols=num_cols,
        )
        metadata.save(cache_dir / "metadata.json")

        # 7. Create empty filters
        pl.DataFrame().write_parquet(cache_dir / "filters.parquet")

        print(f"✓ CTDP cache created at: {cache_dir}")
        return str(cache_dir)

    except Exception as e:
        # Cleanup on error
        print(f"✗ Error creating cache, cleaning up {cache_dir}...")
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        raise ValueError(f"Failed to create CTDP cache: {e}") from e


def delete_cache(path: Union[str, Path], verbose: bool = True):
    """
    Delete CTDP cache directory.

    Args:
        path: Path to cache directory
        verbose: Print status messages

    Example:
        >>> delete_cache('/data/atlas/ctdp_20251021_143022')
        Deleting cache: /data/atlas/ctdp_20251021_143022
        ✓ Deleted: /data/atlas/ctdp_20251021_143022
    """
    path = Path(path)
    if not path.exists():
        if verbose:
            print(f"Cache not found: {path}")
        return

    if verbose:
        print(f"Deleting cache: {path}")
    shutil.rmtree(path)
    if verbose:
        print(f"✓ Deleted: {path}")


class CTDPCache:
    """
    Stateful CTDP cache manager.

    Primary interface for app usage. Works with column name strings
    (expressions already computed during cache creation).

    Example:
        >>> cache = CTDPCache('/data/atlas/ctdp_20251021_143022')
        >>> cache.update_filter('return', -0.1, 0.1)
        >>> plots = cache.generate_plots()
    """

    def __init__(self, path: Union[str, Path]):
        """Load existing cache from path"""
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"Cache not found: {self.path}")
        if not (self.path / "metadata.json").exists():
            raise ValueError(f"Invalid cache (no metadata.json): {self.path}")

        self.metadata = CTDPMetadata.load(self.path / "metadata.json")
        self._filters = pl.read_parquet(self.path / "filters.parquet")

    @property
    def filters(self) -> pl.DataFrame:
        """Current filters DataFrame"""
        return self._filters

    def load_data(self) -> pl.LazyFrame:
        """Load cached data as LazyFrame"""
        return pl.scan_parquet(self.path / "data.parquet")

    def update_filter(self, col_name: str, min_val: Any, max_val: Any):
        """Update filter for column (auto-saves)"""
        self._filters = _update_filter_df(self._filters, col_name, min_val, max_val)
        self.save_filters()

    def remove_filter(self, col_name: str):
        """Remove filter for column (auto-saves)"""
        self._filters = _remove_filter_df(self._filters, col_name)
        self.save_filters()

    def save_filters(self):
        """Persist current filters to disk"""
        self._filters.write_parquet(self.path / "filters.parquet")

    def reload_filters(self):
        """Reload filters from disk (discard in-memory changes)"""
        self._filters = pl.read_parquet(self.path / "filters.parquet")

    def update_settings(
        self, n_ticks: Optional[int] = None, num_cols: Optional[int] = None
    ):
        """Update settings and save to disk"""
        if n_ticks is not None:
            self.metadata.n_ticks = n_ticks
        if num_cols is not None:
            self.metadata.num_cols = num_cols
        self.metadata.save(self.path / "metadata.json")

    def generate_plots(self, format_fns: Optional[dict] = None) -> List[go.Figure]:
        """Generate plots using current state"""
        # Import here to avoid circular dependency
        from .plotting import plot_ctdp

        df = self.load_data()

        # Use strings (expressions already computed!)
        return plot_ctdp(
            df=df,
            accum_cols=self.metadata.accum_cols,  # List[str]
            feature_cols=self.metadata.feature_cols,  # List[str]
            weight_col=self.metadata.weight_col,  # str
            n_ticks=self.metadata.n_ticks,
            filters=self._filters if self._filters.width > 0 else None,
            format_fns=format_fns,
        )


# Private helper functions


def _update_filter_df(
    filters: pl.DataFrame, col_name: str, min_val: Any, max_val: Any
) -> pl.DataFrame:
    """
    Update or add a filter column (pure function).

    Args:
        filters: Current filters DataFrame
        col_name: Column to filter
        min_val: Minimum value (None for no lower bound)
        max_val: Maximum value (None for no upper bound)

    Returns:
        New filters DataFrame with updated column
    """
    # Create new filter column
    new_filter = pl.DataFrame({col_name: [min_val, max_val]})

    # If filters is empty, return new filter
    if filters.width == 0:
        return new_filter

    # If column already exists, drop it first
    if col_name in filters.columns:
        filters = filters.drop(col_name)

    # Add new filter column (horizontal concat)
    return pl.concat([filters, new_filter], how="horizontal")


def _remove_filter_df(filters: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Remove a filter column (pure function).

    Args:
        filters: Current filters DataFrame
        col_name: Column to remove

    Returns:
        New filters DataFrame without specified column
    """
    if col_name in filters.columns:
        return filters.drop(col_name)
    return filters
