"""
Date-partitioned dataset abstractions with validation caching and parallel computation.

## ByDateDataview (Read-only)
Hive-partitioned data by date: `{path}/date={date}/**/*.parquet`

**Validation Workflow:**
- Partitions inferred from `universe()['date'].unique()`
- Default validation: attempt to read parquet files via `_valid_partition(date)`
- Results cached to `{path}/validated_partitions.json` (persistent)
- Parallel validation using process pool (`num_workers`)
- Access via `lazyframe()` or `__getitem__(dates)` (validates before loading)

**Subclass Override:**
- `universe()`: Return DataFrame with "date", "symbol" columns (required)
- `_valid_partition(date)`: Custom validation logic (optional, default checks readability)
- `_get_self_kwargs()`: Worker reconstruction params (optional, for custom attributes)

## ByDateDataset (Computation)
Extends ByDateDataview with parallel computation.

**Computation Workflow:**
- `compute(recompute=False)`: Computes missing (uncached) partitions in parallel batches
- Workers call `_compute_partitions(dates) -> LazyFrame` for each batch
- Auto-partitioning: `sink_parquet(PartitionByKey(by=['date']))`
- Successful partitions marked valid and cached
- Raises RuntimeError if any batch fails

**Subclass Override:**
- `_compute_partitions(dates)`: Compute data for date batch (required)
"""

from abc import ABC, abstractmethod
from datetime import date as Date
from dataclasses import dataclass, field
from pathlib import Path
from atlas import ParallelMap
from atlas.multiprocessing import chunk_list
from typing import List, Set, Optional, Dict
import json
import logging
from functools import partial
import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass(kw_only=True)
class ByDateDataview(ABC):
    path: Path = Path('')
    num_workers: int = 16
    parquet_names: str = "*.parquet"
    _valid_partitions: Set[Date] = field(default_factory=set, init=False, repr=False)
    _validation_file: Path = field(init=False, repr=False)
    _parallel_map: ParallelMap = field(init=False, repr=False)
    _symbol_enum: pl.Enum = field(init=False, repr=False)

    def __post_init__(self):
        """Initialize paths, parallel executor, and load cached validations."""
        self.path = Path(self.path)
        self._validation_file = self.path / "validated_partitions.json"
        self._parallel_map = ParallelMap(max_workers=self.num_workers, pbar=True, use_thread=False)
        universe_df = self.universe()
        universe_schema = universe_df.schema
        if not (
            'date' in universe_schema and
            'symbol' in universe_schema
        ):
            raise RuntimeError(f'Dataview expects symbol, date columns. Schema: {universe_schema}')
        self.partitions = sorted(universe_df['date'].unique().to_list())
        # Create symbol enum from universe for efficient categorical operations
        self._symbol_enum = pl.Enum(universe_df['symbol'].unique().sort())
        cached = self._load_validation_cache()
        if cached:
            self.update_validations(new_partitions=list(cached), outdated_partitions=[], memory=True, file=False)
            # logger.info(f"Loaded {len(cached)} validated partitions from cache")

    def _load_validation_cache(self) -> Set[Date]:
        """Load validation cache from JSON file. Returns empty set on error."""
        if not self._validation_file.exists():
            logger.warning(f'No validation cache-file exists at {self._validation_file}\nCheck whether {self.path} exists.')
            return set()
        try:
            with open(self._validation_file, "r") as f:
                data = json.load(f)
                return {Date.fromisoformat(d) for d in data.get("valid_partitions", [])}
        except (json.JSONDecodeError, OSError, ValueError) as e:
            logger.warning(f"Could not load validation cache: {e}")
            return set()

    def _save_validation_cache(self) -> bool:
        """Persist in-memory validation cache to JSON file. Returns success status."""
        try:
            data = {"valid_partitions": sorted(d.isoformat() for d in self._valid_partitions)}
            temp_file = self._validation_file.with_suffix(".json.tmp")
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)
            temp_file.replace(self._validation_file)
            return True
        except OSError as e:
            logger.error(f"Could not save validation cache: {e}")
            return False

    def _valid_partition(self, date: Date) -> bool:
        """
        Validate a single partition (called in worker process during parallel validation).

        Default: Checks partition exists and parquet files are readable.
        Override for custom logic (e.g., schema validation, row count checks, data integrity).

        Called during:
        - `validate()` / `invalid_partitions()`: Parallel batch validation
        - `valid_partition(date)`: Single partition check
        - `__getitem__(dates)` / `lazyframe()`: Pre-load validation

        Args:
            date: Partition date to validate

        Returns:
            True if valid, False otherwise
        """
        # Uncomment this line to monkey-patch: force recomputation. Override _compute_partitions as well. 
        # return False 
        partition_path = self.path / f'date={date}/{self.parquet_names}'
        try:
            pl.scan_parquet(partition_path).head(1).collect()
            return True
        except Exception as e:
            logger.debug(f'Date {date} validation failed: {e}')
            return False

    def _check_partitions_batch(self, dates: List[Date]) -> Dict[Date, bool]:
        """Validate multiple partitions in parallel using process pool. Returns dict mapping date to validity."""
        if not dates:
            return {}
        # logger.info(f"Validating {len(dates)} partitions with {self.num_workers} workers")
        validate_fn = partial(
            _validate_partition_worker,
            path=self.path,
            parquet_names=self.parquet_names,
            validator_class=self.__class__,
            validator_kwargs=self._get_self_kwargs(),
        )
        def on_validation_error(exception: Exception, date: Date, _index: int) -> bool:
            logger.error(f"Exception validating {date}: {exception}")
            return False
        results_list = self._parallel_map(validate_fn, dates, on_error=on_validation_error)
        results = dict(zip(dates, results_list))
        # valid_count = sum(1 for v in results.values() if v)
        # logger.info(f"Validation complete: {valid_count}/{len(dates)} valid")
        return results

    def _get_self_kwargs(self) -> Dict:
        """
        Return kwargs needed to reconstruct instance in worker process.
        Override to pass custom attributes to workers. Call super() and merge results.

        Example:
            def _get_self_kwargs(self):
                return {**super()._get_self_kwargs(), 'api_key': self.api_key}
        """
        return {}

    def _postprocess_lf(self, lf) -> pl.LazyFrame:
        """
        Common postprocessing steps. Cast symbol column to enum for categorical efficiency if present.
        This operation is **idempotent**, as it is performed both during sinking & later.
        Subclasses can override to add additional processing while calling super()._postprocess_lf(lf).
        """
        schema = lf.collect_schema()
        if 'symbol' in schema:
            return lf.with_columns(pl.col('symbol').cast(self._symbol_enum))
        return lf 

    @abstractmethod
    def universe(self) -> pl.DataFrame:
        """
        Return universe DataFrame defining available data.

        Required columns:
        - "date": Date dtype (partitions inferred from unique dates)
        - "symbol": String dtype (trading symbols)

        May include additional columns (e.g., "hour" for intraday data).
        Called during __post_init__ to infer partitions and build symbol enum.
        """
        pass

    def update_validations(self, new_partitions: List[Date], outdated_partitions: List[Date], memory: bool = True, file: bool = False):
        """
        Single point of state mutation for validation cache.
        Adds new_partitions and removes outdated_partitions using efficient set operations.
        Updates in-memory cache if memory=True, persists to file if file=True.
        """
        if not memory and not file:
            return
        if memory:
            if new_partitions:
                self._valid_partitions.update(new_partitions)
            if outdated_partitions:
                self._valid_partitions.difference_update(outdated_partitions)
        if file:
            self._save_validation_cache()

    def valid_partition(self, date: Date, recompute: bool = False) -> bool:
        """
        Check if partition is valid. Uses cache unless recompute=True.
        Updates cache and persists result. Returns True if valid, False otherwise.
        """
        if not recompute and date in self._valid_partitions:
            return True
        try:
            is_valid = self._valid_partition(date)
        except Exception as e:
            logger.error(f"Validation error for {date}: {e}")
            is_valid = False
        self.update_validations(
            new_partitions=[date] if is_valid else [],
            outdated_partitions=[] if is_valid else [date],
            memory=True,
            file=True,
        )
        return is_valid

    def validate_partition(self, date: Date, recompute: bool = False):
        """Validate partition or raise RuntimeError if invalid."""
        if not self.valid_partition(date, recompute):
            raise RuntimeError(f"Invalid partition {date}")

    def invalid_partitions(self, recompute: bool = False) -> Set[Date]:
        """
        Find all invalid partitions using parallel validation.
        If recompute=False, only validates uncached partitions and returns all known invalid.
        If recompute=True, revalidates everything and returns only newly found invalid.
        """
        if recompute:
            to_validate = list(self.partitions)
        else:
            to_validate = [d for d in self.partitions if d not in self._valid_partitions]
            if not to_validate:
                logger.info("All partitions already validated (cached)")
                return set()
        validation_results = self._check_partitions_batch(to_validate)
        valid = [date for date, is_valid in validation_results.items() if is_valid]
        invalid = [date for date, is_valid in validation_results.items() if not is_valid]
        self.update_validations(new_partitions=valid, outdated_partitions=invalid, memory=True, file=True)
        if not recompute:
            return {d for d in self.partitions if d not in self._valid_partitions}
        return set(invalid)

    def validate(self, recompute: bool = False):
        """Validate all partitions or raise RuntimeError with list of invalid partitions."""
        invalid = self.invalid_partitions(recompute=recompute)
        if invalid:
            raise RuntimeError(f"DatasetView failed validation. {len(invalid)} invalid partitions:\n{sorted(invalid)}")

    def __getitem__(self, dates: Optional[List[Date]] = None) -> pl.LazyFrame:
        """
        Get LazyFrame for specified partitions or all partitions.
        Validates partitions before loading. Returns polars LazyFrame.
        """
        if dates is None:
            self.validate()
            return pl.scan_parquet(self.path / f"date=*/**/{self.parquet_names}")
        for date in dates:
            self.validate_partition(date)
        return self._postprocess_lf(
            pl.concat([pl.scan_parquet(self.path / f"date={date}/**/{self.parquet_names}") for date in dates])
        )

    def lazyframe(self, validate=True) -> pl.LazyFrame:
        """
        Returns a single lazyframe for the whole dataset
        """
        lf_path: Path = self.path / f'date=*/**/{self.parquet_names}'
        lf = self._postprocess_lf(pl.scan_parquet(lf_path))
        if validate:
            try:
                _ = lf.head(1).collect()
            except Exception as e:
                raise RuntimeError(f'Failed to fetch lazyframe at {lf_path}\n{e}')
        return lf

    def num_partitions(self) -> int:
        """Return total number of partitions."""
        return len(self.partitions)

    def num_validated(self) -> int:
        """Return number of cached valid partitions."""
        return len(self._valid_partitions)

    def clear_validation_cache(self, memory: bool = True, file: bool = False):
        """Clear validation cache in memory and/or delete cache file."""
        if memory:
            self._valid_partitions.clear()
        if file and self._validation_file.exists():
            try:
                self._validation_file.unlink()
                # logger.info("Deleted validation cache file")
            except OSError as e:
                logger.error(f"Could not delete cache file: {e}")


def _validate_partition_worker(date: Date, path: Path, parquet_names: str, validator_class: type, validator_kwargs: Dict) -> bool:
    """Worker function for parallel validation in separate process. Recreates validator and calls _valid_partition."""
    try:
        validator = validator_class(path=path, parquet_names=parquet_names, **validator_kwargs)
        return validator._valid_partition(date)
    except Exception as e:
        logger.error(f"Worker validation failed for {date}: {e}")
        return False


@dataclass(kw_only=True)
class ByDateDataset(ByDateDataview):
    """
    Dataset with computation capabilities. Computed partitions are automatically validated and cached.
    Inherits default _valid_partition implementation from ByDateDataview (attempts to read parquet files).
    """

    @abstractmethod
    def _compute_partitions(self, dates: List[Date]) -> pl.LazyFrame:
        """
        Compute data for a batch of dates (called in worker process).

        Implementation pattern:
        1. Query universe for symbols available on these dates
        2. Download/fetch/compute data for (date, symbol) pairs
        3. Return single LazyFrame containing all dates

        Worker automatically:
        - Partitions by date: `sink_parquet(PartitionByKey(by=['date']))`
        - Applies `_postprocess_lf()` (symbol enum casting, etc.)
        - Validates and caches successful partitions

        Args:
            dates: Batch of dates to compute (typically 30 days via days_per_batch)

        Returns:
            LazyFrame with all dates (must include 'date' column for partitioning)

        Raises:
            Exception on failure (marks batch as failed, logged to user)
        """
        pass

    def _compute_partitions_batch(self, dates: List[Date], days_per_batch: int) -> Dict[Date, bool]:
        """Compute multiple partitions in parallel batches. Returns dict mapping date to success status."""
        if not dates:
            return {}
        date_chunks = chunk_list(sorted(dates), days_per_batch, assert_div=False)
        logger.info(f"Computing {len(dates)} partitions in {len(date_chunks)} batches ({days_per_batch} days/batch) with {self.num_workers} workers")
        compute_fn = partial(
            _compute_partitions_worker,
            path=self.path,
            parquet_names=self.parquet_names,
            dataset_class=self.__class__,
            dataset_kwargs=self._get_self_kwargs(),
        )
        def on_compute_error(exception: Exception, dates_batch: List[Date], _index: int) -> List[bool]:
            logger.error(f"Computation failed for batch {dates_batch[0]} to {dates_batch[-1]}: {exception}")
            return [False] * len(dates_batch)
        batch_results = self._parallel_map(compute_fn, date_chunks, on_error=on_compute_error)
        # Flatten batch results back to per-date dict
        results = {}
        for dates_batch, batch_success in zip(date_chunks, batch_results):
            if isinstance(batch_success, list):
                for date, success in zip(dates_batch, batch_success):
                    results[date] = success
            else:
                for date in dates_batch:
                    results[date] = batch_success
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Computation complete: {success_count}/{len(dates)} successful")
        return results

    def compute(self, recompute: bool = False, days_per_batch: int = 30):
        """
        Compute all partitions in parallel batches. Marks successful computations as valid in cache.
        If recompute=False, only computes uncached partitions. Raises RuntimeError if any fail.
        days_per_batch: Number of dates to process per worker batch (default 30, matching notebook pattern).
        """
        if recompute:
            to_compute = list(self.partitions)
        else:
            to_compute = [d for d in self.partitions if d not in self._valid_partitions]
            if not to_compute:
                return
        computation_results = self._compute_partitions_batch(to_compute, days_per_batch)
        successful = [date for date, success in computation_results.items() if success]
        failed = [date for date, success in computation_results.items() if not success]
        self.update_validations(new_partitions=successful, outdated_partitions=failed, memory=True, file=True)
        if failed:
            raise RuntimeError(f"Computation failed for {len(failed)} partitions:\n{sorted(failed)}")

def _compute_partitions_worker(dates: List[Date], path: Path, parquet_names: str, dataset_class: type, dataset_kwargs: Dict) -> bool:
    """Worker function for parallel batch computation. Processes multiple dates, partitions and saves. Returns success bool."""
    try:
        dataset = dataset_class(path=path, parquet_names=parquet_names, **dataset_kwargs)
        lf = dataset._compute_partitions(dates)
        lf.sink_parquet(pl.PartitionByKey(path, by=['date'], 
                        per_partition_sort_by=pl.col('time')), compression='brotli', mkdir=True)
        return True
    except Exception as e:
        logger.error(f"Worker computation failed for batch {dates[0]} to {dates[-1]}: {e}")
        return False