"""Type detection and formatting utilities for CTDP plots"""

from typing import Any, Literal, Tuple, Optional
from datetime import datetime, date, timedelta
import polars as pl

# Type alias for supported column types
ColType = Literal['timedelta', 'datetime', 'date', 'numeric']


def detect_col_type(df: pl.DataFrame, col_name: str) -> ColType:
    """
    Detect column type from polars dtype.

    Args:
        df: Polars DataFrame
        col_name: Column name to check

    Returns:
        One of: 'timedelta', 'datetime', 'date', 'numeric'

    Examples:
        >>> detect_col_type(df, 'timestamp')
        'datetime'
        >>> detect_col_type(df, 'duration')
        'timedelta'
    """
    dtype = df.schema[col_name]

    if dtype == pl.Duration:
        return 'timedelta'
    elif dtype == pl.Datetime:
        return 'datetime'
    elif dtype == pl.Date:
        return 'date'
    else:
        # Assume numeric (Int*, UInt*, Float*, Decimal, etc.)
        return 'numeric'


def format_value(value: Any, col_type: Optional[ColType] = None) -> str:
    """
    Format value for display in plot labels.

    Auto-detects type if col_type not provided.

    Args:
        value: Value to format
        col_type: Optional type hint

    Returns:
        Formatted string representation

    Examples:
        >>> format_value(3.14159, 'numeric')
        '3.14'
        >>> format_value(timedelta(seconds=123.456), 'timedelta')
        '123.456s'
        >>> format_value(datetime(2025, 1, 21, 14, 30), 'datetime')
        '2025-01-21 14:30:00'
    """
    # Auto-detect if not provided
    if col_type is None:
        if isinstance(value, timedelta):
            col_type = 'timedelta'
        elif isinstance(value, datetime):
            col_type = 'datetime'
        elif isinstance(value, date):
            col_type = 'date'
        else:
            col_type = 'numeric'

    # Format based on type
    if col_type == 'timedelta':
        if isinstance(value, timedelta):
            return f"{value.total_seconds():.3f}s"
        else:
            # Polars Duration as nanoseconds
            return f"{value / 1_000_000_000:.3f}s"

    elif col_type == 'datetime':
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Convert from polars datetime if needed
            return str(value)

    elif col_type == 'date':
        if isinstance(value, (date, datetime)):
            return value.strftime('%Y-%m-%d')
        else:
            return str(value)

    else:  # numeric
        if isinstance(value, float):
            return f"{value:.2f}"
        else:
            return str(value)


def get_col_range(df: pl.DataFrame, col_name: str) -> Tuple[Any, Any]:
    """
    Get (min, max) range for a column.

    Handles all column types uniformly via polars expressions.

    Args:
        df: Polars DataFrame
        col_name: Column name

    Returns:
        (min_value, max_value) tuple

    Examples:
        >>> get_col_range(df, 'price')
        (10.5, 99.9)
        >>> get_col_range(df, 'timestamp')
        (datetime(2025, 1, 1), datetime(2025, 1, 21))
    """
    min_val = df[col_name].min()
    max_val = df[col_name].max()
    return (min_val, max_val)
