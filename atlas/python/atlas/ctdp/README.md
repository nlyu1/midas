# CTDP: Cumulative Total-Dependence Plots

A modular, type-aware library for creating cumulative total-dependence plots with interactive exploration.

## Overview

CTDP provides three-tier API for analyzing feature distributions through cumulative plots:
1. **Functional** - Pure stateless plotting
2. **Stateful** - Persistent cache with filter manipulation
3. **Interactive** - Streamlit app for exploration

## Installation

The module is part of the `atlas` package. Ensure you have:
- `polars` - DataFrame operations
- `plotly` - Interactive plotting
- `streamlit` - Web app interface

## Quick Start

### 1. Functional API (Stateless)

Pure functional plotting for quick analysis in notebooks:

```python
import polars as pl
from atlas.ctdp import plot_ctdp

# Prepare data
df = pl.read_parquet("data.parquet").lazy()

# Generate plots
plots = plot_ctdp(
    df=df,
    accum_cols=['null_frac'],           # Metrics to accumulate
    feature_cols=['return', 'lag'],     # Features to slice by
    weight_col='weight',                 # Weighting column
    n_ticks=5,                          # Number of x-axis ticks
)

# Display in notebook
plots[0].show()

# Save to HTML
plots[0].write_html("plot.html")
```

**With filters:**
```python
# Create 2-row filter DataFrame
# Row 0 = min values, Row 1 = max values
filters = pl.DataFrame({
    'return': [-0.1, 0.1],
    'lag': [None, 10.0],  # None = no bound
})

plots = plot_ctdp(df, ..., filters=filters)
```

### 2. Stateful API (Persistent Cache)

Create persistent state for reproducible analysis:

```python
from atlas.ctdp import create_ctdp, load_ctdp_filters, save_ctdp_filters, generate_plots
from atlas.ctdp import update_filter, remove_filter
import polars as pl

# Create state (caches data, initializes metadata)
path = create_ctdp(
    df=my_lazyframe,
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
    n_ticks=7,
    num_cols=2,
)
# Returns: PosixPath('/data/atlas/ctdp_20251021_142530')

# Manipulate filters
filters = load_ctdp_filters(path)
filters = update_filter(filters, 'return', -0.05, 0.05)
filters = update_filter(filters, 'max_tick_to_query_lag', None, 5.0)
save_ctdp_filters(path, filters)

# Generate plots with current state
plots = generate_plots(path)
plots[0].show()

# Later: reload and continue
filters = load_ctdp_filters(path)
filters = remove_filter(filters, 'return')
save_ctdp_filters(path, filters)
plots = generate_plots(path)
```

### 3. Interactive App

Launch Streamlit app for interactive exploration:

```bash
# Create state first
python -c "
from atlas.ctdp import create_ctdp
import polars as pl
df = pl.read_parquet('data.parquet').lazy()
path = create_ctdp(df, accum_cols=['y'], feature_cols=['x'], weight_col='w')
print(f'Created: {path}')
"

# Launch app
streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_20251021_142530

# Or with uv
uv run streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/ctdp_20251021_142530
```

Or programmatically:
```python
from atlas.ctdp import run_app
from pathlib import Path

run_app(Path('/data/atlas/ctdp_20251021_142530'))
```

## Complete Workflow Example

```python
import polars as pl
from atlas.ctdp import create_ctdp, plot_ctdp
from pathlib import Path

# 1. Load and prepare data
df = pl.read_parquet("./plotting_dev_parquet.parquet")
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

# 2. Quick exploration (functional)
plots = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
)
plots[0].write_html("quick_exploration.html")

# 3. Create persistent state for deeper analysis
cache_path = create_ctdp(
    df=agg_df.lazy(),
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
    cache_dir=Path('/data/atlas/my_analysis'),  # Optional: auto-generated if None
    n_ticks=7,
)

# 4. Launch interactive app
# streamlit run python/atlas/ctdp/run_ctdp_app.py -- --path /data/atlas/my_analysis
```

In the app:
- Adjust filters using type-aware widgets (numeric, datetime, timedelta, date)
- Filter changes are automatically persisted
- Statistics update in real-time (filtered count, retention %)
- Plots regenerate with current filters
- All state survives app restarts

## State Persistence

CTDP state is stored in three files:

```
/data/atlas/ctdp_20251021_142530/
├── data.parquet       # Cached DataFrame
├── metadata.json      # Column names, settings
└── filters.parquet    # 2-row DataFrame with filter bounds
```

### Filter Format

Filters are stored as a 2-row Polars DataFrame:
- **Row 0**: minimum values
- **Row 1**: maximum values
- **Columns**: filtered features (missing column = no filter)
- **Values**: `None` = no bound on that side

Example:
```python
filters = pl.DataFrame({
    'return': [-0.1, 0.1],              # -0.1 <= return <= 0.1
    'lag': [None, 5.0],                 # lag <= 5.0 (no lower bound)
})
```

This format:
- ✅ Native Polars type preservation (datetime, timedelta, etc.)
- ✅ Efficient parquet serialization
- ✅ Simple manipulation with standard Polars operations
- ✅ No custom type serialization needed

### Metadata Format

```json
{
  "accum_cols": ["null_frac"],
  "feature_cols": ["return", "max_tick_to_query_lag"],
  "weight_col": "weight",
  "n_ticks": 7,
  "num_cols": 2,
  "created_at": "2025-10-21T14:25:30.123456",
  "version": "0.1.0"
}
```

## API Reference

### Functional API

```python
plot_ctdp(
    df: pl.LazyFrame,
    accum_cols: List[str],
    feature_cols: List[str],
    weight_col: str,
    n_ticks: int = 5,
    filters: Optional[pl.DataFrame] = None,
    format_fns: Optional[Dict[str, Callable]] = None,
) -> List[go.Figure]
```

### Stateful API

```python
# Creation
create_ctdp(
    df: pl.LazyFrame,
    accum_cols: List[str],
    feature_cols: List[str],
    weight_col: str,
    cache_dir: Optional[Path] = None,  # Auto-generates: /data/atlas/ctdp_{timestamp}
    n_ticks: int = 5,
    num_cols: int = 2,
) -> Path

# Loading
load_ctdp_metadata(cache_dir: Path) -> CTDPMetadata
load_ctdp_filters(cache_dir: Path) -> pl.DataFrame
load_ctdp_data(cache_dir: Path) -> pl.LazyFrame

# Saving
save_ctdp_filters(cache_dir: Path, filters: pl.DataFrame)
save_ctdp_metadata(cache_dir: Path, metadata: CTDPMetadata)

# Filter manipulation
update_filter(filters: pl.DataFrame, col_name: str, min_val: Any, max_val: Any) -> pl.DataFrame
remove_filter(filters: pl.DataFrame, col_name: str) -> pl.DataFrame
get_filter_range(filters: pl.DataFrame, col_name: str) -> Optional[Tuple[Any, Any]]

# Plotting
generate_plots(cache_dir: Path, format_fns: Optional[dict] = None) -> List[go.Figure]
```

### App API

```python
run_app(cache_dir: Path)
```

## Type Support

CTDP automatically detects and handles:

| Type | Polars dtype | Widget | Format Example |
|------|--------------|--------|----------------|
| Numeric | Int*, UInt*, Float* | number_input | `3.14` |
| Timedelta | Duration | number_input (seconds) | `123.456s` |
| Datetime | Datetime | date_input | `2025-01-21 14:30:00` |
| Date | Date | date_input | `2025-01-21` |

Custom formatting:
```python
def my_formatter(value):
    return f"${value:,.2f}"

plots = plot_ctdp(
    df=df,
    ...,
    format_fns={'price': my_formatter}
)
```

---

## Design & Architecture

### Design Philosophy

**Key Principles:**
1. **Separation of concerns** - Plotting, state, UI are independent
2. **Type-native persistence** - Leverage Polars/Parquet for type safety
3. **Fully vectorized** - No row-by-row iteration, all Polars expressions
4. **Progressive disclosure** - Three API tiers for different use cases
5. **Stateless core** - Functional plotting has no side effects

### Module Structure

```
python/atlas/ctdp/
├── config.py          # Constants (DEFAULT_PLOT_PATH, etc.)
├── formatting.py      # Type detection & formatting
├── plotting.py        # Core plot_ctdp() - pure functional
├── state.py          # Persistence & session management
├── app.py            # Streamlit UI components
├── run_ctdp_app.py   # CLI entry point
└── __init__.py       # Public API exports
```

**Dependency graph:**
```
run_ctdp_app.py
    └── app.py
            └── state.py
                    └── plotting.py
                            └── formatting.py
                                    └── config.py
```

### Core Algorithms

**CTDP Generation** (`plotting.py`):
```
For each feature_col in feature_cols:
    1. Sort DataFrame by feature_col (ascending)
    2. Compute cumulative sums:
       - weight_cumsum = cumsum(weight_col)
       - weight_norm = weight_cumsum / sum(weight_col)  # [0, 1]
       - For each accum_col: accum_cumsum = cumsum(accum_col)
    3. Generate x-ticks at quantiles:
       - Compute feature quantiles [0, 0.25, 0.5, 0.75, 1.0]
       - Find nearest index for each quantile
       - Extract (weight_norm[idx], formatted_feature[idx])
    4. Create Plotly figure:
       - X-axis: weight_norm
       - Y-axes: One trace per accum_col (cumulative)
       - Custom xticks showing feature values
       - Hover: feature value at position
```

**Fully Vectorized:**
- No Python loops over rows
- All operations use Polars expressions
- Single pass for cumsum operations
- Quantile-based tick generation uses `arg_min()` for nearest neighbor

**Filter Application** (`state.py`):
```
For each column in filters DataFrame:
    min_val = filters[col][0]
    max_val = filters[col][1]

    if min_val is not None:
        df = df.filter(pl.col(col) >= min_val)
    if max_val is not None:
        df = df.filter(pl.col(col) <= max_val)
```

Lazy evaluation: Filters are applied to LazyFrame, actual filtering happens during `collect()`.

### State Management Design

**Why 2-row DataFrame for filters?**
- Polars Parquet handles all type serialization automatically
- No custom JSON serialization for datetime/timedelta/date
- Type-safe: impossible to deserialize as wrong type
- Simple manipulation: just DataFrame operations
- Efficient: binary format, compressed

**Alternative considered (rejected):**
```json
{
  "filters": {
    "return": {"min": -0.1, "max": 0.1, "type": "float"},
    "timestamp": {"min": "2025-01-01", "max": "2025-01-21", "type": "datetime"}
  }
}
```
❌ Requires custom type serialization/deserialization
❌ Error-prone type reconstruction
❌ More code to maintain

**Chosen approach:**
```python
filters = pl.DataFrame({
    'return': [-0.1, 0.1],
    'timestamp': [datetime(2025,1,1), datetime(2025,1,21)]
})
filters.write_parquet("filters.parquet")
```
✅ Native Polars types preserved
✅ Zero custom serialization code
✅ Type safety guaranteed by Polars

### App State Flow

```
User opens app
    ↓
Load: metadata.json, filters.parquet, data.parquet (scan)
    ↓
Render filter widgets (type-aware based on column dtype)
    ↓
User adjusts filter
    ↓
Update filters DataFrame → save_ctdp_filters()
    ↓
Apply filters to LazyFrame → collect()
    ↓
Compute statistics (filtered count, retention)
    ↓
Generate plots via plot_ctdp()
    ↓
Render plots in grid
    ↓
[Repeat on interaction]
```

**State persistence triggers:**
- Filter change → Immediate save to `filters.parquet`
- Settings change (n_ticks, num_cols) → Immediate save to `metadata.json`
- Data → Never modified after initial `create_ctdp()`

### Type System

**Type Detection** (`formatting.py:detect_col_type`):
```python
pl.Duration   → 'timedelta'
pl.Datetime   → 'datetime'
pl.Date       → 'date'
*             → 'numeric'
```

**Type Formatting** (`formatting.py:format_value`):
```python
timedelta     → "123.456s"
datetime      → "2025-01-21 14:30:00"
date          → "2025-01-21"
numeric       → "3.14"
```

**Widget Mapping** (`app.py:_create_filter_widget`):
```python
timedelta     → st.number_input (seconds) + conversion
datetime      → st.date_input + datetime.combine()
date          → st.date_input
numeric       → st.number_input
```

### Performance Considerations

**Data Collection:**
- LazyFrame collected **once** per plot generation
- Filters applied lazily (no intermediate materialization)
- App: Full data loaded once at startup (for range calculation)

**Vectorization:**
- All cumulative sums: single `cum_sum()` call
- Quantile tick generation: vectorized via `arg_min()`
- Filter application: Polars expression tree optimization

**Caching:**
- App: Streamlit's `@st.cache_data` can be added for plot generation
- Data: Already cached to disk via `data.parquet`

**Scaling:**
- Large datasets: Consider sampling before `create_ctdp()`
- Many features: Grid layout prevents excessive scrolling
- Plotly HTML size: ~100KB per 10K points (acceptable for most cases)

### Extension Points

**Custom Formatting:**
```python
def custom_format(value):
    return f"${value:,.2f}M"

plot_ctdp(df, ..., format_fns={'revenue': custom_format})
```

**Custom Accumulation:**
Just add more columns to `accum_cols`:
```python
plot_ctdp(
    df=df,
    accum_cols=['null_frac', 'mean_price', 'total_volume'],
    ...
)
```

**Additional Metrics:**
Compute in preprocessing, then include in `accum_cols`:
```python
df = df.with_columns([
    (pl.col('price').std() / pl.col('price').mean()).alias('cv')
])
plot_ctdp(df, accum_cols=['cv'], ...)
```

### Future Enhancements

**Potential additions:**
1. **Subsampling** - For very large datasets, subsample plot data while preserving quantiles
2. **Multiple weights** - Support different weighting schemes
3. **Categorical features** - Extend to categorical slicing (bar charts)
4. **Comparison mode** - Overlay multiple CTDP sessions for A/B testing
5. **Export** - Download filtered data from app
6. **Annotations** - Add text annotations to plots
7. **Theming** - Customizable color schemes

**Backward compatibility:**
- Metadata includes `version` field for migration support
- Future changes should extend, not break existing states

### Testing Strategy

**Unit tests:**
- `formatting.py` - All type conversions
- `plotting.py` - Individual helper functions
- `state.py` - Filter manipulation logic

**Integration tests:**
- Full workflow: create → manipulate → generate
- Round-trip: save → load → verify equality
- App: Streamlit test framework (if needed)

**Test data:**
- Use existing `plotting_dev_parquet.parquet`
- Create minimal synthetic data for edge cases

### Known Limitations

1. **Filter granularity** - Only range filters (no regex, contains, etc.)
2. **Plot customization** - Limited Plotly configuration exposure
3. **Data immutability** - Cached data cannot be updated (must recreate)
4. **Single metric per trace** - Can't plot derived metrics without preprocessing
5. **Memory** - Full data loaded in app (not streaming)

### Troubleshooting

**Common issues:**

1. **Import errors:**
   - Ensure `PYTHONPATH` includes `python/atlas`
   - Or install package: `pip install -e python/atlas`

2. **Streamlit app won't start:**
   - Verify cache directory exists
   - Check `metadata.json` is valid JSON
   - Ensure all three files present

3. **Type errors in filters:**
   - Filters must match column dtypes exactly
   - Use `pl.Duration`, not raw timedelta for duration columns

4. **Performance issues:**
   - Reduce `n_ticks` to decrease tick generation overhead
   - Pre-filter data before `create_ctdp()`
   - Consider sampling large datasets

### Code Navigation

**To understand plotting algorithm:**
1. Start: `plotting.py:plot_ctdp()`
2. Follow: `_create_single_plot()` → `_prepare_plot_data()` → `_generate_xticks()`
3. End: `_create_plotly_figure()`

**To understand state persistence:**
1. Start: `state.py:create_ctdp()`
2. Load: `load_ctdp_*()` functions
3. Save: `save_ctdp_*()` functions
4. Manipulate: `update_filter()`, `remove_filter()`

**To understand app flow:**
1. Entry: `run_ctdp_app.py:main()`
2. Core: `app.py:run_app()`
3. Widgets: `_create_filter_widget()`, `_render_filter_grid()`
4. Display: `_render_plots_grid()`

### Version History

- **0.1.0** (2025-10-21) - Initial implementation
  - Three-tier API (functional, stateful, app)
  - Type-aware filtering (numeric, datetime, date, timedelta)
  - Polars-native state persistence
  - Vectorized plot generation
