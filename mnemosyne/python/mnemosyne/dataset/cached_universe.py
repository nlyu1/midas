"""
Abstract dataset with cached universe support.

Architecture parallel to Rust CryptoDataInterface:
1. Universe Initialization: Fetch (symbol, date) pairs from source ’ cache to universe.parquet
2. Partition Inference: Extract unique dates from universe ’ self.partitions
3. Computation: Subclasses implement _compute_partitions using universe info
4. Validation: Inherited from ByDateDataset (check parquet files exist and readable)

Key differences from Rust version:
- Partitions by date only (not symbol), multiple symbols stored per date partition
- Uses Python parallelism machinery from ByDateDataset
- Flexible universe schema (supports additional columns like 'hour' for intraday data)
"""

from abc import abstractmethod
from datetime import date as Date
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import logging
import polars as pl

from .interface import ByDateDataset

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class CachedUniverseDataset(ByDateDataset):
    """
    Abstract dataset with cached universe support.

    Universe defines all available (date, symbol) pairs from source (e.g., S3, API).
    Caches universe to avoid expensive API calls on every instantiation.

    Workflow:
        1. Call initialize_universe(refresh=True) to fetch from source
        2. Universe cached to {path}/universe.parquet
        3. Subsequent instantiations read from cache (fast)
        4. Call compute() to download/process missing partitions
        5. Validation inherited from ByDateDataset
    """

    _universe_cache_path: Path = field(init=False, repr=False)
    _universe_df: Optional[pl.DataFrame] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        """Initialize universe cache path and load cached universe if exists."""
        self.path = Path(self.path)
        self._universe_cache_path = self.path / "universe.parquet"

        # Load cached universe if available (needed before super().__post_init__)
        # Super calls self.universe() to infer partitions
        if self._universe_cache_path.exists():
            self._universe_df = pl.read_parquet(self._universe_cache_path)

        # Call parent __post_init__ which calls self.universe() to get partitions
        super().__post_init__()

    @abstractmethod
    def _fetch_new_universe(self) -> pl.DataFrame:
        """
        Fetch fresh universe from source (e.g., S3 API, REST API, database query).

        Must return DataFrame with columns:
        - "date": Date dtype (trading dates)
        - "symbol": String dtype (trading symbols/tickers)

        May include additional columns for granular tracking (e.g., "hour" for intraday data).

        Returns:
            DataFrame with "date" and "symbol" columns (minimum required schema)

        Raises:
            Exception on fetch failure (network error, invalid response, etc.)
        """
        pass

    def initialize_universe(self, refresh: bool = False):
        """
        Initialize universe with caching (parallel to Rust initialize_universe).

        Fetches new universe from source if:
        - refresh=True (force refresh, bypass cache)
        - Cache file missing at {path}/universe.parquet

        Args:
            refresh: If True, fetch from source even if cache exists

        Raises:
            RuntimeError: If universe schema invalid (missing date/symbol columns)
        """
        if refresh or not self._universe_cache_path.exists():
            logger.info("Fetching new universe from source...")
            universe_df = self._fetch_new_universe()

            # Validate schema
            schema = universe_df.schema
            required_cols = {'date', 'symbol'}
            missing_cols = required_cols - set(schema.keys())
            if missing_cols:
                raise RuntimeError(
                    f"Universe must have 'date' and 'symbol' columns. "
                    f"Missing: {missing_cols}. Got schema: {list(schema.keys())}"
                )

            # Ensure date column is Date dtype
            if schema['date'] != pl.Date:
                logger.info("Converting 'date' column to Date dtype")
                universe_df = universe_df.with_columns(pl.col('date').cast(pl.Date))

            # Create parent directory if needed
            self._universe_cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to cache
            universe_df.write_parquet(self._universe_cache_path, compression='lz4')
            logger.info(
                f"Wrote universe ({len(universe_df):,} rows, "
                f"{universe_df['symbol'].n_unique()} symbols, "
                f"{universe_df['date'].n_unique()} dates) "
                f"to {self._universe_cache_path}"
            )

            # Update in-memory cache
            self._universe_df = universe_df
        else:
            logger.info(f"Universe cache exists at {self._universe_cache_path}, skipping fetch")

    def universe(self) -> pl.DataFrame:
        """
        Get universe DataFrame (implements ByDateDataview abstract method).

        Returns cached universe. Partitions are inferred from unique dates
        via ByDateDataview.__post_init__.

        Minimum schema:
        - "date": Date dtype
        - "symbol": String dtype

        May include additional columns (e.g., "hour" for intraday data).

        Returns:
            DataFrame with "date" and "symbol" columns

        Raises:
            RuntimeError: If universe cache doesn't exist (call initialize_universe first)
        """
        if self._universe_df is not None:
            return self._universe_df

        if not self._universe_cache_path.exists():
            raise RuntimeError(
                f"Universe not initialized. Cache file not found at {self._universe_cache_path}.\n"
                "Call initialize_universe() to fetch and cache universe from source."
            )

        # Load and cache
        self._universe_df = pl.read_parquet(self._universe_cache_path)
        return self._universe_df
