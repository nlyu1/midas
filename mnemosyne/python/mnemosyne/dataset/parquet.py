from dataclasses import dataclass 
from .interface import ByDateDataview, ByDateDataset
from typing import Dict, Optional, List 
from abc import ABC, abstractmethod
from datetime import date as Date 
import polars as pl 

@dataclass(kw_only=True)
class ParquetDataview(ByDateDataview):
    """
    Example implementation that validates:
    1. Partition directory exists
    2. At least one parquet file exists
    3. Files are readable by Polars
    4. Expected columns are present
    """
    expected_columns: Optional[List[str]] = None

    def _get_self_kwargs(self) -> Dict:
        """Return this class's kwargs and aggregate parent's."""
        return {'expected_columns': self.expected_columns} | super()._get_self_kwargs()

    def _valid_partition(self, date: Date) -> bool:
        """
        Validate a single partition.
        PURE: No side effects, just returns validation result.
        """
        partition_dir = self.path / f"date={date}"
        try:
            lf = pl.scan_parquet(partition_dir / f'**/{self.parquet_names}')
            cols = lf.collect_schema().names()
            lf.head(1).collect()
            if self.expected_columns is None:
                return True 
            return set(cols) >= set(self.expected_columns) 
        except Exception as e:
            print(f'Error reading {date} partition: {e}')
            return False 