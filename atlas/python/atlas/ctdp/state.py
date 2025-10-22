"""State management and persistence for CTDP sessions"""

import json
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime

import polars as pl

from .config import DEFAULT_PLOT_PATH, DEFAULT_N_TICKS, DEFAULT_NUM_COLS
from .plotting import plot_ctdp


@dataclass
class CTDPMetadata:
    """
    Metadata for a CTDP session.

    All settings except filters (which are stored separately in filters.parquet).
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


def create_ctdp(
    df: pl.LazyFrame,
    accum_cols: List[str],
    feature_cols: List[str],
    weight_col: str,
    cache_dir: Optional[Path] = None,
    n_ticks: int = DEFAULT_N_TICKS,
    num_cols: int = DEFAULT_NUM_COLS,
) -> Path:
    """
    Create CTDP state directory with initialized state.

    This collects the LazyFrame and caches it to disk, then initializes
    metadata and empty filters.

    Args:
        df: Input LazyFrame (will be collected and cached)
        accum_cols: Column names to accumulate
        feature_cols: Feature column names to slice by
        weight_col: Weight column name
        cache_dir: Cache directory (auto-generated if None)
        n_ticks: Number of x-axis ticks
        num_cols: Number of grid columns for app

    Returns:
        Path to created cache directory

    Example:
        >>> path = create_ctdp(
        ...     df=my_lazyframe,
        ...     accum_cols=['null_frac'],
        ...     feature_cols=['return', 'lag'],
        ...     weight_col='weight',
        ... )
        >>> print(path)
        PosixPath('/data/atlas/ctdp_20250121_143022')
    """
    # Auto-generate cache directory if not provided
    if cache_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cache_dir = DEFAULT_PLOT_PATH / f"ctdp_{timestamp}"

    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Collect and cache data
    print(f"Collecting and caching data to {cache_dir / 'data.parquet'}...")
    df.collect().write_parquet(cache_dir / "data.parquet", compression="lz4")

    # Create and save metadata
    metadata = CTDPMetadata(
        accum_cols=accum_cols,
        feature_cols=feature_cols,
        weight_col=weight_col,
        n_ticks=n_ticks,
        num_cols=num_cols,
    )
    metadata.save(cache_dir / "metadata.json")

    # Create empty filters DataFrame (0 columns, 2 rows)
    empty_filters = pl.DataFrame()
    empty_filters.write_parquet(cache_dir / "filters.parquet")

    print(f"✓ CTDP state created at: {cache_dir}")
    return cache_dir


def load_ctdp_metadata(cache_dir: Path) -> CTDPMetadata:
    """
    Load CTDP metadata from cache directory.

    Args:
        cache_dir: Path to CTDP cache directory

    Returns:
        CTDPMetadata object
    """
    return CTDPMetadata.load(Path(cache_dir) / "metadata.json")


def load_ctdp_filters(cache_dir: Path) -> pl.DataFrame:
    """
    Load CTDP filters from cache directory.

    Returns empty DataFrame if no filters exist.

    Args:
        cache_dir: Path to CTDP cache directory

    Returns:
        2-row DataFrame with filter bounds (or empty DataFrame)
    """
    filters_path = Path(cache_dir) / "filters.parquet"
    if filters_path.exists():
        return pl.read_parquet(filters_path)
    else:
        return pl.DataFrame()


def load_ctdp_data(cache_dir: Path) -> pl.LazyFrame:
    """
    Load CTDP data as LazyFrame.

    Args:
        cache_dir: Path to CTDP cache directory

    Returns:
        LazyFrame reading from cached data
    """
    return pl.scan_parquet(Path(cache_dir) / "data.parquet")


def save_ctdp_filters(cache_dir: Path, filters: pl.DataFrame):
    """
    Save filters to cache directory.

    Args:
        cache_dir: Path to CTDP cache directory
        filters: 2-row DataFrame with filter bounds
    """
    filters.write_parquet(Path(cache_dir) / "filters.parquet")


def save_ctdp_metadata(cache_dir: Path, metadata: CTDPMetadata):
    """
    Save metadata to cache directory.

    Args:
        cache_dir: Path to CTDP cache directory
        metadata: CTDPMetadata object
    """
    metadata.save(Path(cache_dir) / "metadata.json")


def update_filter(
    filters: pl.DataFrame,
    col_name: str,
    min_val: Any,
    max_val: Any,
) -> pl.DataFrame:
    """
    Update or add a filter column.

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


def remove_filter(filters: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Remove a filter column.

    Args:
        filters: Current filters DataFrame
        col_name: Column to remove

    Returns:
        New filters DataFrame without specified column
    """
    if col_name in filters.columns:
        return filters.drop(col_name)
    return filters


def get_filter_range(
    filters: pl.DataFrame,
    col_name: str,
) -> Optional[Tuple[Any, Any]]:
    """
    Get filter range for a column.

    Args:
        filters: Filters DataFrame
        col_name: Column name

    Returns:
        (min_val, max_val) tuple, or None if column not filtered
    """
    if col_name not in filters.columns:
        return None
    return (filters[col_name][0], filters[col_name][1])


def generate_plots(
    cache_dir: Path,
    format_fns: Optional[dict] = None,
) -> List[Any]:
    """
    Generate plots using saved state.

    Convenience function that loads metadata, filters, and data,
    then calls plot_ctdp.

    Args:
        cache_dir: Path to CTDP cache directory
        format_fns: Optional custom format functions

    Returns:
        List of plotly Figures
    """
    metadata = load_ctdp_metadata(cache_dir)
    filters = load_ctdp_filters(cache_dir)
    df = load_ctdp_data(cache_dir)

    return plot_ctdp(
        df=df,
        accum_cols=metadata.accum_cols,
        feature_cols=metadata.feature_cols,
        weight_col=metadata.weight_col,
        n_ticks=metadata.n_ticks,
        filters=filters if filters.width > 0 else None,
        format_fns=format_fns,
    )
