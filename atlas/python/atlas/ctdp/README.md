# CTDP: Cumulative Total-Dependence Plots

A modular, type-aware library for creating cumulative total-dependence plots with interactive exploration.

## Overview

CTDP provides a three-tier API for analyzing feature distributions through cumulative plots:
1. **Functional** - Pure stateless plotting with Polars expressions
2. **Stateful** - Persistent cache with filter manipulation via `CTDPCache` class
3. **Interactive** - Streamlit app for exploration

## Installation

The module is part of the `atlas` package. Ensure you have:
- `polars` - DataFrame operations
- `plotly` - Interactive plotting
- `streamlit` - Web app interface

## Quick Start

### 1. Functional API (Stateless)

Pure functional plotting with Polars expressions:

```python
import polars as pl
from atlas.ctdp import plot_ctdp

# Prepare data
df = pl.read_parquet("data.parquet").lazy()

# Generate plots with expressions
plots = plot_ctdp(
    df=df,
    accum_cols=[pl.col('null_frac')],                      # Metrics to accumulate
    feature_cols=[
        pl.col('return'),                                   # Raw column
        pl.col('lag').log().alias('log_lag'),              # Computed feature!
    ],
    weight_col=pl.col('weight'),
    n_ticks=5,
)

# Display
plots[0].show()
plots[0].write_html("plot.html")
```

**String shortcuts** (converted to `pl.col(str)` internally):
```python
plots = plot_ctdp(df, accum_cols=['y'], feature_cols=['x'], weight_col='w')
```

**With filters:**
```python
# 2-row DataFrame: row 0 = min, row 1 = max
filters = pl.DataFrame({
    'return': [-0.1, 0.1],
    'lag': [None, 10.0],  # None = no bound
})

plots = plot_ctdp(df, ..., filters=filters)
```

### 2. Stateful API (Persistent Cache)

Create persistent state using `CT DPCache`:

```python
from atlas.ctdp import create_ctdp_cache, CTDPCache
import polars as pl

# Create cache (evaluates expressions, saves compressed parquet)
path = create_ctdp_cache(
    df=my_lazyframe,
    accum_cols=[pl.col('null_frac')],
    feature_cols=[
        pl.col('return'),
        pl.col('max_tick_to_query_lag'),
    ],
    weight_col=pl.col('weight'),
    n_ticks=7,
)
# Returns: '/data/atlas/ctdp_20251021_143022'

# Load cache and manipulate
cache = CTDPCache(path)
cache.update_filter('return', -0.05, 0.05)
cache.update_filter('max_tick_to_query_lag', None, 5.0)  # No lower bound

plots = cache.generate_plots()
plots[0].show()

# Later: reload
cache = CTDPCache('/data/atlas/ctdp_20251021_143022')
cache.remove_filter('return')
plots = cache.generate_plots()
```

### 3. Interactive App

Launch Streamlit app:

```bash
# Create cache first
python -c "
from atlas.ctdp import create_ctdp_cache
import polars as pl

df = pl.read_parquet('data.parquet').lazy()
path = create_ctdp_cache(
    df,
    accum_cols=[pl.col('y')],
    feature_cols=[pl.col('x')],
    weight_col=pl.col('w')
)
print(f'Created: {path}')
"

# Launch app
streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_20251021_143022
```

Or programmatically:
```python
from atlas.ctdp import run_app
run_app('/data/atlas/ctdp_20251021_143022')
```

## Complete Workflow Examples

### Example 1: Trading Analysis

```python
import polars as pl
from atlas.ctdp import plot_ctdp, create_ctdp_cache, CTDPCache

# Load and aggregate trading data
df = pl.read_parquet("./data.parquet")
agg_df = (
    df.group_by("symbol", "date")
    .agg(
        pl.col("max_tick_to_query_lag").mean(),
        pl.col("return").sum(),
        (pl.col("return").null_count() / pl.len()).alias("null_frac"),
    )
    .with_columns(
        pl.col("symbol").cast(pl.String).str.len_chars().alias("weight")
    )
)

# Quick exploration: How does null_frac accumulate across return distribution?
plots = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=[pl.col('null_frac')],
    feature_cols=[pl.col('return')],
    weight_col=pl.col('weight'),
    n_ticks=5,
)
plots[0].show()  # In Jupyter
# Insight: Check if null data concentrates in extreme returns

# Advanced: Add computed features
plots_detailed = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=[pl.col('null_frac')],
    feature_cols=[
        pl.col('return'),
        pl.col('return').abs().alias('abs_return'),  # Magnitude regardless of direction
        pl.col('max_tick_to_query_lag').log().alias('log_lag'),  # Log-scale lag
    ],
    weight_col=pl.col('weight'),
)
# Now we can see null_frac across 3 different feature views

# Create persistent cache for deeper analysis
path = create_ctdp_cache(
    df=agg_df.lazy(),
    accum_cols=[pl.col('null_frac')],
    feature_cols=[
        pl.col('return'),
        pl.col('max_tick_to_query_lag'),
    ],
    weight_col=pl.col('weight'),
    n_ticks=7,
)
print(f"Cache created: {path}")

# Programmatic filtering
cache = CTDPCache(path)
cache.update_filter('return', -0.05, 0.05)  # Focus on small returns
cache.update_filter('max_tick_to_query_lag', None, 10.0)  # Cap lag at 10
plots = cache.generate_plots()
plots[0].write_html("filtered_analysis.html")

# Launch interactive app for stakeholders
# streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path {path}
```

### Example 2: Financial Metrics Analysis

```python
import polars as pl
from atlas.ctdp import plot_ctdp

# Load financial data
df = pl.scan_parquet("financial_data.parquet")

# Analyze revenue distribution with multiple metrics
plots = plot_ctdp(
    df=df,
    accum_cols=[
        pl.col('profit_margin'),
        pl.col('customer_count'),
        (pl.col('marketing_spend') / pl.col('revenue')).alias('marketing_ratio'),
    ],
    feature_cols=[
        pl.col('revenue'),
        pl.col('revenue').log().alias('log_revenue'),
        (pl.col('revenue') / pl.col('employee_count')).alias('revenue_per_employee'),
    ],
    weight_col=pl.col('transaction_count'),
)

# Interpretation:
# - How does profit margin accumulate as revenue grows?
# - Where do most customers concentrate in the revenue distribution?
# - Is marketing spend efficient across revenue segments?
```

### Example 3: A/B Test Analysis

```python
import polars as pl
from atlas.ctdp import plot_ctdp

df = pl.scan_parquet("ab_test_results.parquet")

# Compare control vs treatment
for variant in ['control', 'treatment']:
    variant_df = df.filter(pl.col('variant') == variant)

    plots = plot_ctdp(
        df=variant_df,
        accum_cols=[
            pl.col('conversion_rate'),
            pl.col('revenue_per_user'),
        ],
        feature_cols=[
            pl.col('session_duration'),
            pl.col('pages_viewed'),
            (pl.col('clicks') / pl.col('impressions')).alias('ctr'),
        ],
        weight_col=pl.col('user_count'),
    )

    plots[0].write_html(f"{variant}_analysis.html")

# Insight: Compare how metrics accumulate differently between variants
```

### Example 4: Time-Series Lag Analysis

```python
import polars as pl
from atlas.ctdp import create_ctdp_cache, CTDPCache

df = pl.scan_parquet("time_series_data.parquet")

# Create cache with time-based features
path = create_ctdp_cache(
    df=df,
    accum_cols=[
        pl.col('error_rate'),
        pl.col('timeout_count'),
    ],
    feature_cols=[
        pl.col('processing_lag'),
        pl.col('processing_lag').log().alias('log_lag'),
        pl.col('queue_depth'),
    ],
    weight_col=pl.col('request_count'),
)

# Interactive exploration in app
# Users can filter by lag ranges to identify problematic segments
cache = CTDPCache(path)
cache.update_filter('processing_lag', 0, 100)  # Focus on reasonable lags
plots = cache.generate_plots()

# Answer questions like:
# - At what lag threshold does error rate spike?
# - How much of our traffic operates under 50ms lag?
```

### Example 5: Minimal Quickstart

```python
import polars as pl
from atlas.ctdp import plot_ctdp

# Simplest possible usage
df = pl.scan_parquet("data.parquet")

plots = plot_ctdp(
    df=df,
    accum_cols=['metric_y'],      # String shortcut
    feature_cols=['feature_x'],    # Automatically converted to pl.col()
    weight_col='sample_weight',
)

plots[0].show()
```

## Key Design Features

### Expression Evaluation

**At cache creation:**
- Expressions are evaluated once via `sink_parquet` (streaming, lazy)
- Only required columns saved (minimal storage)
- Output names extracted via `.meta.output_name()`
- Stored as strings in `metadata.json`

**After cache creation:**
- All operations work with column name strings
- Expressions already computed and saved

**Benefits:**
- ✅ Flexible feature engineering without intermediate files
- ✅ Minimal storage (only what you need)
- ✅ Fast streaming execution via `sink_parquet`

### CTDPCache Class

Stateful manager for app usage:

```python
cache = CTDPCache(path)

# Properties
cache.path          # Path object
cache.metadata      # CTDPMetadata object
cache.filters       # pl.DataFrame (2-row format)

# Methods
cache.load_data() → LazyFrame
cache.update_filter(col, min, max)
cache.remove_filter(col)
cache.save_filters()
cache.reload_filters()
cache.update_settings(n_ticks=, num_cols=)
cache.generate_plots() → List[Figure]
```

## State Persistence

CTDP state stored in three files:

```
/data/atlas/ctdp_20251021_143022/
├── data.parquet       # Cached data (lz4 compressed, selected columns only)
├── metadata.json      # Column names (strings), settings
└── filters.parquet    # 2-row DataFrame with filter bounds
```

### Filter Format

```python
filters = pl.DataFrame({
    'return': [-0.1, 0.1],              # -0.1 <= return <= 0.1
    'lag': [None, 5.0],                 # lag <= 5.0 (no lower bound)
})
```

- **Row 0**: minimum values
- **Row 1**: maximum values
- **Missing column**: no filter
- **None value**: no bound on that side

### Metadata Format

```json
{
  "accum_cols": ["null_frac"],
  "feature_cols": ["return", "log_lag"],
  "weight_col": "weight",
  "n_ticks": 7,
  "num_cols": 2,
  "created_at": "2025-10-21T14:25:30.123456",
  "version": "0.1.0"
}
```

Note: `log_lag` is the output name from `pl.col('lag').log().alias('log_lag')`.

## API Reference

### Functional API

```python
plot_ctdp(
    df: pl.LazyFrame,
    accum_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    feature_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    weight_col: Union[pl.Expr, str],
    n_ticks: int = 5,
    filters: Optional[pl.DataFrame] = None,
    format_fns: Optional[Dict[str, Callable]] = None,
) -> List[go.Figure]
```

### Stateful API

```python
# Creation
create_ctdp_cache(
    df: pl.LazyFrame,
    accum_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    feature_cols: Union[List[Union[pl.Expr, str]], pl.Expr, str],
    weight_col: Union[pl.Expr, str],
    cache_dir: Optional[Path] = None,  # Auto-generates
    n_ticks: int = 5,
    num_cols: int = 2,
) -> str  # Returns path string

# Cleanup
delete_cache(path: Union[str, Path], verbose: bool = True)

# CTDPCache class
class CTDPCache:
    def __init__(self, path: Union[str, Path])
    @property def filters() -> pl.DataFrame
    def load_data() -> pl.LazyFrame
    def update_filter(col_name: str, min_val: Any, max_val: Any)
    def remove_filter(col_name: str)
    def save_filters()
    def reload_filters()
    def update_settings(n_ticks: Optional[int], num_cols: Optional[int])
    def generate_plots(format_fns: Optional[dict]) -> List[go.Figure]
```

### App API

```python
run_app(cache_dir: Union[str, Path])
```

## Type Support

| Type | Polars dtype | Widget | Format Example |
|------|--------------|--------|----------------|
| Numeric | Int*, UInt*, Float* | number_input | `3.14` |
| Timedelta | Duration | number_input (seconds) | `123.456s` |
| Datetime | Datetime | date_input | `2025-01-21 14:30:00` |
| Date | Date | date_input | `2025-01-21` |

## Advanced Usage

### Custom Formatting

```python
def price_format(value):
    return f"${value:,.2f}M"

plots = plot_ctdp(
    df=df,
    ...,
    format_fns={'revenue': price_format}
)
```

### Computed Features

```python
plot_ctdp(
    df=df,
    accum_cols=[
        pl.col('volume'),
        (pl.col('high') - pl.col('low')).alias('range'),
    ],
    feature_cols=[
        pl.col('price'),
        pl.col('price').pct_change().alias('returns'),
        pl.col('volume').log().alias('log_volume'),
        (pl.col('close') / pl.col('open') - 1).alias('intraday_return'),
    ],
    weight_col=pl.col('shares'),
)
```

### Validation Example

```python
try:
    path = create_ctdp_cache(
        df=df,
        accum_cols=[pl.col('x'), pl.col('y').alias('x')],  # Duplicate!
        feature_cols=[pl.col('z')],
        weight_col=pl.col('w'),
    )
except ValueError as e:
    print(e)  # "Duplicate output names detected: ['x']"
```

On error, partially created directories are automatically cleaned up.

---

## Design & Architecture

### Module Structure

```
python/atlas/ctdp/
├── config.py          # Constants
├── formatting.py      # Type detection & formatting
├── plotting.py        # Core plot_ctdp() - fully vectorized
├── state.py          # CTDPCache class + persistence
├── app.py            # Streamlit UI
├── run_ctdp_app.py   # CLI entry point
└── __init__.py       # Public API
```

### Expression Flow

**Creation:**
```
User provides pl.Expr
    ↓
Extract output names via .meta.output_name()
    ↓
Validate uniqueness
    ↓
df.select(exprs).sink_parquet()  (lazy, streaming!)
    ↓
Save metadata with string names
```

**Usage:**
```
Load metadata (strings)
    ↓
CTDPCache.generate_plots()
    ↓
plot_ctdp(df, feature_cols=["return", "log_lag"], ...)  (strings!)
    ↓
Normalize: strings → pl.col(str)
    ↓
Plot generation
```

### Performance

- **Lazy evaluation**: Uses `sink_parquet` for streaming execution
- **Minimal storage**: Only selected columns saved
- **Vectorization**: All Polars operations use expressions
- **Single collection**: Data collected once per plot generation

### Error Handling

```python
create_ctdp_cache(...) attempts:
    1. Normalize expressions
    2. Extract names
    3. Validate uniqueness
    4. Create directory
    5. Sink parquet  # <-- Polars validates expressions here
    6. Save metadata
    7. Create filters

On error:
    - Print error message
    - Delete created directory
    - Raise ValueError with context
```

### Future Enhancements

- **Subsampling**: For massive datasets
- **Multiple weights**: Different weighting schemes
- **Categorical features**: Bar chart support
- **Comparison mode**: Overlay multiple caches
- **Export**: Download filtered data from app

### Version History

- **0.1.0** (2025-10-21)
  - Initial implementation
  - Expression-based API (Polars expressions + strings)
  - `CTDPCache` class for stateful operations
  - Type-aware filtering
  - Vectorized plotting
  - `sink_parquet` for lazy caching
