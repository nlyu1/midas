"""Test Step 4: State management and persistence"""

import sys
from pathlib import Path
import shutil

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import polars as pl
from atlas.ctdp.state import (
    create_ctdp,
    load_ctdp_metadata,
    load_ctdp_filters,
    load_ctdp_data,
    save_ctdp_filters,
    update_filter,
    remove_filter,
    get_filter_range,
    generate_plots,
)

print("✓ Step 4: Testing state.py")

# Load and prepare test data
print("\n1. Preparing test data...")
df = pl.read_parquet("./plotting_dev_parquet.parquet")
agg_df = (
    df.group_by("symbol", "date")
    .agg(
        pl.col("max_tick_to_query_lag").mean(),
        pl.col("return").sum(),
        (pl.col("return").null_count() / pl.len()).alias("null_frac"),
    )
    .with_columns(pl.col("symbol").cast(pl.String).str.len_chars().alias("weight"))
)
lazy_df = agg_df.lazy()
print(f"  Prepared {len(agg_df):,} rows")

# Create CTDP state
print("\n2. Creating CTDP state...")
test_cache_dir = Path("./test_ctdp_state")
# Clean up if exists
if test_cache_dir.exists():
    shutil.rmtree(test_cache_dir)

cache_path = create_ctdp(
    df=lazy_df,
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
    cache_dir=test_cache_dir,
    n_ticks=7,
    num_cols=2,
)
print(f"  Created at: {cache_path}")

# Verify files were created
print("\n3. Verifying files...")
expected_files = ['data.parquet', 'metadata.json', 'filters.parquet']
for fname in expected_files:
    fpath = cache_path / fname
    if fpath.exists():
        size = fpath.stat().st_size
        print(f"  ✓ {fname:20} ({size:,} bytes)")
    else:
        print(f"  ✗ {fname:20} MISSING!")

# Load metadata
print("\n4. Loading metadata...")
metadata = load_ctdp_metadata(cache_path)
print(f"  accum_cols: {metadata.accum_cols}")
print(f"  feature_cols: {metadata.feature_cols}")
print(f"  weight_col: {metadata.weight_col}")
print(f"  n_ticks: {metadata.n_ticks}")
print(f"  num_cols: {metadata.num_cols}")
print(f"  created_at: {metadata.created_at}")

# Load filters (should be empty)
print("\n5. Loading filters...")
filters = load_ctdp_filters(cache_path)
print(f"  Filters shape: {filters.shape}")
print(f"  Filters columns: {filters.columns}")

# Manipulate filters
print("\n6. Testing filter manipulation...")
# Add filter for 'return'
filters = update_filter(filters, 'return', -0.1, 0.1)
print(f"  After adding 'return' filter: {filters.shape}")
print(f"  Columns: {filters.columns}")
print(f"  Values: min={filters['return'][0]}, max={filters['return'][1]}")

# Add filter for 'max_tick_to_query_lag'
# First get the range from data
data_df = load_ctdp_data(cache_path).collect()
lag_min, lag_max = data_df['max_tick_to_query_lag'].min(), data_df['max_tick_to_query_lag'].max()
filters = update_filter(filters, 'max_tick_to_query_lag', lag_min, lag_max * 0.5)
print(f"  After adding 'lag' filter: {filters.shape}")
print(f"  Columns: {filters.columns}")

# Save filters
save_ctdp_filters(cache_path, filters)
print(f"  Saved filters to disk")

# Reload filters
filters_reloaded = load_ctdp_filters(cache_path)
print(f"  Reloaded filters: {filters_reloaded.shape}")
assert filters_reloaded.width == 2, "Should have 2 filter columns"

# Get specific filter range
return_range = get_filter_range(filters_reloaded, 'return')
print(f"  'return' filter range: {return_range}")

# Remove a filter
filters = remove_filter(filters, 'return')
print(f"  After removing 'return': {filters.shape}")
print(f"  Columns: {filters.columns}")

# Generate plots with current state
print("\n7. Generating plots with filters...")
save_ctdp_filters(cache_path, filters)  # Save updated filters
plots = generate_plots(cache_path)
print(f"  Generated {len(plots)} plots")

# Clean up
print("\n8. Cleanup...")
# Commenting out to allow inspection
# shutil.rmtree(test_cache_dir)
# print(f"  Removed {test_cache_dir}")
print(f"  Test state preserved at: {test_cache_dir}")
print(f"  (Delete manually when done inspecting)")

print("\n✓ All state management tests passed!")
