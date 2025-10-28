import polars as pl
from pathlib import Path
from dataclasses import dataclass, field
from timedelta_isoformat import timedelta as Timedelta
from mnemosyne.engines import ReturnsEngine, MetadataEngine
from mnemosyne.dataset import ByDateDataset
from datetime import date as Date
from typing import Dict, Any, List, Tuple
from ..dataset.dataset_types import DatasetType
from .last_trade import BinanceLastTradesGrid

default_returns_mark_exprs = {
    "m10m_to_now": (pl.col("time") - Timedelta(minutes=10), Timedelta(minutes=10)),
    "m12h_to_now": (pl.col("time") - Timedelta(hours=12), Timedelta(hours=12)),
    "m1d_to_now": (pl.col("time") - Timedelta(days=1), Timedelta(days=1)),
    "m1h_to_now": (pl.col("time") - Timedelta(hours=1), Timedelta(hours=1)),
    "m1m_to_now": (pl.col("time") - Timedelta(minutes=1), Timedelta(minutes=1)),
    "m20m_to_now": (pl.col("time") - Timedelta(minutes=20), Timedelta(minutes=20)),
    "m2h_to_now": (pl.col("time") - Timedelta(hours=2), Timedelta(hours=2)),
    "m2m_to_now": (pl.col("time") - Timedelta(minutes=2), Timedelta(minutes=2)),
    "m30m_to_now": (pl.col("time") - Timedelta(minutes=30), Timedelta(minutes=30)),
    "m3h_to_now": (pl.col("time") - Timedelta(hours=3), Timedelta(hours=3)),
    "m5m_to_now": (pl.col("time") - Timedelta(minutes=5), Timedelta(minutes=5)),
    "m6h_to_now": (pl.col("time") - Timedelta(hours=6), Timedelta(hours=6)),
    "now_to_p10m": (pl.col("time"), Timedelta(minutes=10)),
    "now_to_p1m": (pl.col("time"), Timedelta(minutes=1)),
    "now_to_p2m": (pl.col("time"), Timedelta(minutes=2)),
    "now_to_p5m": (pl.col("time"), Timedelta(minutes=5)),
    "p1m_to_p11m": (pl.col("time") + Timedelta(minutes=1), Timedelta(minutes=10)),
    "p1m_to_p2m": (pl.col("time") + Timedelta(minutes=1), Timedelta(minutes=1)),
    "p1m_to_p3m": (pl.col("time") + Timedelta(minutes=1), Timedelta(minutes=2)),
    "p1m_to_p6m": (pl.col("time") + Timedelta(minutes=1), Timedelta(minutes=5)),
}


@dataclass(kw_only=True)
class BinanceResearchDataset(ByDateDataset):
    dataset_type: DatasetType
    backend_grid_interval: Timedelta = Timedelta(seconds=5)
    index_grid_interval: Timedelta = Timedelta(minutes=10)
    mark_exprs: Dict[str, Tuple[pl.Expr, Timedelta]] = field(
        default_factory=lambda: default_returns_mark_exprs
    )
    peg_symbol: str = "USDT"

    save_root: Path = Path("/data/mnemosyne/cache")
    returns_mark_exprs: Dict[str, Any] = field(
        default_factory=lambda: default_returns_mark_exprs
    )

    returns_engine_kwargs: Dict[str, Any] = field(
        default_factory=lambda: {
            "backend_fair_expr": pl.col("vwap_price"),
            "backend_time_expr": pl.col("last_event_time"),
        }
    )

    returns_query_kwargs: Dict[str, Any] = field(
        default_factory=lambda: {
            "tick_lag_tolerance": Timedelta(minutes=10),
            "append_lag": False,
        }
    )

    metadata_engine_kwargs: Dict[str, Any] = field(
        default_factory=lambda: {"num_workers": 1}
    )

    metadata_query_kwargs: Dict[str, Any] = field(
        default_factory=lambda: {
            "time_expr": pl.col("time"),
            "symbol_expr": pl.col("symbol"),
        }
    )

    def __post_init__(self):
        self.path = Path(self.save_root) / "".join(
            [
                f"{self.peg_symbol}_{self.dataset_type}_",
                f"index={self.index_grid_interval.isoformat()}_",
                f"backend={self.backend_grid_interval.isoformat()}",
            ]
        )
        self.path.mkdir(exist_ok=True)

        self.backend_dataset = BinanceLastTradesGrid(
            peg_symbol=self.peg_symbol,
            grid_interval=self.backend_grid_interval,
            dataset_type=self.dataset_type,
        )

        self.index_dataset = BinanceLastTradesGrid(
            peg_symbol=self.peg_symbol,
            grid_interval=self.index_grid_interval,
            dataset_type=self.dataset_type,
        )

        self.query_lf = self.index_dataset.lazyframe()
        self.returns_engine = ReturnsEngine(
            db=self.backend_dataset.lazyframe(), **self.returns_engine_kwargs
        )
        self.metadata_engine = MetadataEngine(
            path=self.path / "metadata_backend",
            backend_dataset=self.backend_dataset,
            returns_engine_kwargs=self.returns_engine_kwargs,
            # metadata_engine has its own definition of tick_lag_tolerance
            returns_query_kwargs={
                k: v
                for k, v in self.returns_query_kwargs.items()
                if k not in ["append_lag", "tick_lag_tolerance"]
            },
            **self.metadata_engine_kwargs,
        )

        super().__post_init__()

    def universe(self) -> pl.DataFrame:
        return self.backend_dataset.universe()

    def _get_self_kwargs(self):
        return {
            "dataset_type": self.dataset_type,
            "backend_grid_interval": self.backend_grid_interval,
            "index_grid_interval": self.index_grid_interval,
            "peg_symbol": self.peg_symbol,
            "returns_mark_exprs": self.returns_mark_exprs,
            "returns_engine_kwargs": self.returns_engine_kwargs,
            "returns_query_kwargs": self.returns_query_kwargs,
            "metadata_engine_kwargs": self.metadata_engine_kwargs,
            "metadata_query_kwargs": self.metadata_query_kwargs,
        } | super()._get_self_kwargs()

    def _compute_partitions(self, dates: List[Date]) -> pl.LazyFrame:
        min_date, max_date = min(dates), max(dates)

        query_slice = self.query_lf.filter(
            pl.col("date").is_between(min_date, max_date)
        ).sort("symbol", "last_event_time")
        query_with_returns = self.returns_engine.query_batch(
            query_slice, mark_exprs=self.mark_exprs, **self.returns_query_kwargs
        )
        query_with_metadata = self.metadata_engine.append_metadata(
            query_with_returns, **self.metadata_query_kwargs
        )
        return query_with_metadata
