from ..engines import ReturnsEngine
from dataclasses import dataclass, field
from timedelta_isoformat import timedelta as Timedelta
from datetime import date as Date
import polars as pl
from typing import Dict, Any, List
from ..dataset import ByDateDataset
from atlas import printv_lazy


def _default_metadata(
    returns_interval: Timedelta,
) -> Dict[str, Dict[Timedelta, List[pl.Expr]]]:
    liquidity = (pl.col("vwap_price") * pl.col("volume")).sum()
    sqrtliq = liquidity.pow(0.5)
    excess_buy_ratio = (
        pl.col("taker_buy_volume") - pl.col("taker_sell_volume")
    ).sum() / pl.col("volume").sum()
    trade_count = pl.col("trade_count").sum()

    num_intervals_in_day = Timedelta(days=1) / returns_interval
    returns_drift = pl.col("return").mean() * num_intervals_in_day
    volatility = pl.col("return").std() * num_intervals_in_day**0.5
    vol_ssize = pl.col("return").count().alias("vol_ssize")
    return {
        # These are calculated
        "by_symbol_index": {
            Timedelta(days=1): [
                liquidity.alias("liquidity_1d"),
                sqrtliq.alias("sqrtliq_1d"),
                excess_buy_ratio.alias("excess_buy_ratio_1d"),
                trade_count.alias("trade_count_1d"),
            ],
            Timedelta(days=7): [
                liquidity.alias("liquidity_7d"),
                sqrtliq.alias("sqrtliq_7d"),
                excess_buy_ratio.alias("excess_buy_ratio_7d"),
                trade_count.alias("trade_count_7d"),
            ],
        },
        "accum_returns": {
            Timedelta(days=7): [
                returns_drift.alias("daily_returns_drift_7d_lookback"),
                volatility.alias("daily_vol_7d_lookback"),
                vol_ssize.alias("vol_ssize_7d_lookback"),
            ],
            Timedelta(days=30): [
                returns_drift.alias("daily_returns_drift_30d_lookback"),
                volatility.alias("daily_vol_30d_lookback"),
                vol_ssize.alias("vol_ssize_30d_lookback"),
            ],
        },
    }


@dataclass(kw_only=True)
class MetadataEngine(ByDateDataset):
    # Should support initializing a ReturnsEngine
    # Supplies both (1) symbol-date universe (via .universe()) and (2) source for metadata computation
    backend_dataset: ByDateDataset

    # Column used in `backend_dataset` to identify causal point-in-time timestamp
    last_event_time_expr: pl.Expr = field(
        default_factory=lambda: pl.col("last_event_time")
    )

    # Additional arguments for returns engine & query commands
    returns_engine_kwargs: Dict[str, Any] = field(default_factory=dict)
    returns_query_kwargs: Dict[str, Any] = field(
        default_factory=lambda: {"filter_by_query_dates": True}
    )

    # Interval at which metadata should exist.
    # This will be the interval for e.g. volatility. Final volatility will be normalized to day-range
    returns_interval: Timedelta = Timedelta(minutes=10)
    metadata_exprs: Dict[str, Dict[Timedelta, List[pl.Expr]]] = field(
        default_factory=lambda: _default_metadata(Timedelta(minutes=10))
    )

    # Polars selector for metadata which should be quantile-expanded
    quantile_expand_exprs: pl.Expr = field(
        default_factory=lambda: pl.col("^daily_vol.*$", "^liquidity.*$")
    )

    # Grid-interval: interval at which final return result will be gridded
    grid_interval: Timedelta = Timedelta(hours=1)

    # Enable verbose debug output (prints shape and schema at each computation step)
    verbose_debug: bool = False

    def __post_init__(self):
        self.backend_db = self.backend_dataset.lazyframe()
        self.returns_engine = ReturnsEngine(
            self.backend_db, **self.returns_engine_kwargs
        )

        # Compute maximum lookbacks for returns & metadata
        self.max_returns_lookback = max(self.metadata_exprs["accum_returns"].keys())
        self.max_metadata_lookback = max(self.metadata_exprs["by_symbol_index"].keys())

        # Filter laxly around given date ranges: returns & index grids symbol-date pairs are constructed off this dataframe
        backend_universe_df = self.backend_dataset.cast_symbol_col_to_enum(
            self.backend_dataset.universe()
        )
        self.returns_grid = (
            backend_universe_df.with_columns(
                returns_grid_time=pl.datetime_ranges(
                    pl.col("date"),
                    pl.col("date").dt.offset_by("1d"),
                    interval=self.returns_interval,
                    closed="left",
                )
            )
            .explode("returns_grid_time")
            .sort("symbol", "returns_grid_time")
            .lazy()
        )

        super().__post_init__()

    def universe(self) -> pl.DataFrame:
        return self.backend_dataset.universe()

    def _get_self_kwargs(self) -> Dict:
        """Return kwargs needed to reconstruct instance in worker process."""
        return {
            "backend_dataset": self.backend_dataset,
            "returns_engine_kwargs": self.returns_engine_kwargs,
            "returns_query_kwargs": self.returns_query_kwargs,
            "returns_interval": self.returns_interval,
            "grid_interval": self.grid_interval,
            "verbose_debug": self.verbose_debug,
            # Note: Skipping polars expression fields (last_event_time_expr, metadata_exprs,
            # quantile_expand_exprs) as they use default_factory. If you customize them,
            # override this method and include them.
        } | super()._get_self_kwargs()

    def append_metadata(
        self,
        lf: pl.LazyFrame,
        time_expr: pl.Expr = pl.col("time"),
        symbol_expr: pl.Expr = pl.col("symbol"),
    ) -> pl.LazyFrame:
        """
        Append metadata to the input lazyframe using asof join.

        Performs a backward asof join to match each row in lf with the most recent
        metadata entry where metadata.time <= lf.time and metadata.symbol == lf.symbol.

        Args:
            lf: Input lazyframe to append metadata to
            time_expr: Expression identifying the time column in lf (default: pl.col('time'))
            symbol_expr: Expression identifying the symbol column in lf (default: pl.col('symbol'))

        Returns:
            LazyFrame with metadata columns appended
        """
        # Get metadata lazyframe
        metadata_lf = self.lazyframe()

        # Extract column names from expressions
        time_col = time_expr.meta.output_name()
        symbol_col = symbol_expr.meta.output_name()

        # Perform asof join with backward strategy
        # This finds the most recent metadata entry where metadata.time <= query.time
        result = lf.sort(symbol_col, time_col).join_asof(
            metadata_lf.sort("symbol", "time"),
            left_on=time_col,
            right_on="time",
            by_left=symbol_col,
            by_right="symbol",
            strategy="backward",
        )

        return result

    def _compute_partitions(
        self, dates=List[Date], verbose_debug: bool | None = None
    ) -> pl.LazyFrame:
        """
        Users should make sure that dates are contiguous

        Args:
            dates: List of contiguous dates to compute
            verbose_debug: Enable verbose debug output showing schemas at each step.
                          If None, uses self.verbose_debug class attribute.
        """
        if verbose_debug is None:
            verbose_debug = self.verbose_debug

        start_date, end_date = min(dates), max(dates)
        printv_lazy(
            lambda: f"Computing metadata for date range: {start_date} to {end_date}",
            verbose_debug,
        )
        printv_lazy(
            lambda: f"  max_returns_lookback: {self.max_returns_lookback}, max_metadata_lookback: {self.max_metadata_lookback}",
            verbose_debug,
        )

        ## Step 1: compute returns_interval gridded returns
        # [symbol, date, time] with `returns_interval` interspaced "time"
        returns_query = self.returns_grid.filter(
            (pl.col("date") >= start_date - self.max_returns_lookback)
            & (pl.col("date") <= pl.lit(end_date))
        ).sort("symbol", "returns_grid_time")
        printv_lazy(
            lambda: f"Step 1a - returns_query schema: {returns_query.collect_schema()}",
            verbose_debug,
        )

        index_with_returns = self.returns_engine.query(
            returns_query,
            start_time_expr=pl.col("returns_grid_time"),
            mark_duration=self.returns_interval,
            tick_lag_tolerance=self.returns_interval,
            append_lag=True,
            **self.returns_query_kwargs,
        ).sort("symbol", "returns_grid_time")
        printv_lazy(
            lambda: f"Step 1b - index_with_returns schema: {index_with_returns.collect_schema()}",
            verbose_debug,
        )

        ## Step 2: compute returns_metadata from returns
        returns_metadata = (
            pl.concat(
                [index_with_returns.select("symbol", "returns_grid_time")]
                + [
                    index_with_returns.rolling(
                        pl.col("returns_grid_time"),
                        period=interval,
                        closed="left",
                        group_by="symbol",
                    )
                    .agg(cols)
                    .sort("symbol", "returns_grid_time")
                    .drop("symbol", "returns_grid_time")
                    for interval, cols in self.metadata_exprs["accum_returns"].items()
                ],
                how="horizontal",
            )
            .with_columns(
                # Add grid_interval to make grid_time point-in-time as well
                grid_time=pl.col("returns_grid_time").dt.truncate(self.grid_interval)
                + self.grid_interval
            )
            .drop("returns_grid_time")
            .filter(
                # We can filter early here since here's a direct join to the final result
                pl.col("grid_time").is_between(start_date, end_date, closed="left")
            )
            .group_by("symbol", "grid_time")
            .agg(pl.all().last())
            .sort("symbol", "grid_time")
        )
        printv_lazy(
            lambda: f"Step 2 - returns_metadata schema: {returns_metadata.collect_schema()}",
            verbose_debug,
        )

        ## Step 3: collect rolling database of metadata.
        # Only collect index-gridded entries
        inrange_db = self.backend_db.filter(
            (pl.col("date") >= start_date - self.max_metadata_lookback)
            & (pl.col("date") <= end_date)
        ).sort("symbol", self.last_event_time_expr)
        printv_lazy(
            lambda: f"Step 3a - inrange_db schema: {inrange_db.collect_schema()}",
            verbose_debug,
        )

        rolling_metadata = (
            pl.concat(
                [inrange_db.select("symbol", "date", self.last_event_time_expr)]
                + [
                    inrange_db.rolling(
                        self.last_event_time_expr,
                        period=interval,
                        closed="left",
                        group_by="symbol",
                    )
                    .agg(cols)
                    .sort("symbol", self.last_event_time_expr)
                    .drop("symbol", self.last_event_time_expr)
                    for interval, cols in self.metadata_exprs["by_symbol_index"].items()
                ],
                how="horizontal",
            )
            .with_columns(
                # Add grid_interval to make grid_time point-in-time as well
                grid_time=self.last_event_time_expr.dt.truncate(self.grid_interval)
                + self.grid_interval
            )
            .filter(
                # We can filter early here since here's a direct join to the final result
                pl.col("grid_time").is_between(start_date, end_date, closed="left")
            )
            .group_by("symbol", "grid_time")
            .agg(pl.all().last())
            .sort("symbol", "grid_time")
        )
        printv_lazy(
            lambda: f"Step 3b - rolling_metadata schema: {rolling_metadata.collect_schema()}",
            verbose_debug,
        )

        final_metadata = (
            rolling_metadata.join(returns_metadata, on=["symbol", "grid_time"])
            .with_columns(
                (
                    self.quantile_expand_exprs.rank("average")
                    / self.quantile_expand_exprs.count()
                )
                .name.suffix("_q")
                .over("grid_time")
            )
            .sort("symbol", self.last_event_time_expr)
            .rename({"grid_time": "time"})
        )
        printv_lazy(
            lambda: f"Step 4 - final_metadata schema: {final_metadata.collect_schema()}",
            verbose_debug,
        )

        return final_metadata
