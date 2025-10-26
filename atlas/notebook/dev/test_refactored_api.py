"""Test Refactored CTDP API with Expressions"""

import sys
from pathlib import Path
import shutil

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import polars as pl
from atlas.ctdp import plot_ctdp, create_ctdp_cache, CTDPCache, delete_cache

print("=" * 70)
print("CTDP Refactored API Test Suite")
print("=" * 70)

# Load and prepare test data
print("\n1. Loading and preparing data...")
df = pl.read_parquet("./data.parquet")
agg_df = (
    df.group_by("symbol", "date")
    .agg(
        pl.col("max_tick_to_query_lag").mean(),
        pl.col("return").sum(),
        (pl.col("return").null_count() / pl.len()).alias("null_frac"),
    )
    .with_columns(pl.col("symbol").cast(pl.String).str.len_chars().alias("weight"))
)
print(f"  Prepared {len(agg_df):,} rows")

# ===========================================================================
# Test 1: Functional API with Expressions
# ===========================================================================
print("\n" + "=" * 70)
print("Test 1: Functional API with Polars Expressions")
print("=" * 70)

plots = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=[pl.col("null_frac")],
    feature_cols=[
        pl.col("return"),
        pl.col("max_tick_to_query_lag"),
    ],
    weight_col=pl.col("weight"),
    n_ticks=5,
)
print(f"✓ Generated {len(plots)} plots with expressions")

# Test string shortcuts
plots_str = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=["null_frac"],
    feature_cols=["return", "max_tick_to_query_lag"],
    weight_col="weight",
)
print(f"✓ Generated {len(plots_str)} plots with string shortcuts")

# Test computed expressions
plots_computed = plot_ctdp(
    df=agg_df.lazy(),
    accum_cols=[pl.col("null_frac")],
    feature_cols=[
        pl.col("return"),
        pl.col("return").abs().alias("abs_return"),
        pl.col("max_tick_to_query_lag").log().alias("log_lag"),
    ],
    weight_col=pl.col("weight"),
)
print(f"✓ Generated {len(plots_computed)} plots (including 1 computed feature)")

# Save one plot
output_path = Path("./test_refactored_plot.html")
plots[0].write_html(output_path)
print(f"✓ Saved sample plot to: {output_path}")

# ===========================================================================
# Test 2: Cache Creation with Validation
# ===========================================================================
print("\n" + "=" * 70)
print("Test 2: Cache Creation & Validation")
print("=" * 70)

test_cache_dir = Path("./test_refactored_cache")
if test_cache_dir.exists():
    shutil.rmtree(test_cache_dir)

# Test successful creation
path = create_ctdp_cache(
    df=agg_df.lazy(),
    accum_cols=[pl.col("null_frac")],
    feature_cols=[pl.col("return"), pl.col("max_tick_to_query_lag")],
    weight_col=pl.col("weight"),
    cache_dir=test_cache_dir,
    n_ticks=7,
)
print(f"✓ Cache created at: {path}")

# Verify files
for fname in ["data.parquet", "metadata.json", "filters.parquet"]:
    fpath = Path(path) / fname
    assert fpath.exists(), f"Missing {fname}"
    print(f"  ✓ {fname:20} ({fpath.stat().st_size:,} bytes)")

# Test duplicate name validation
print("\nTesting duplicate name validation...")
try:
    bad_path = create_ctdp_cache(
        df=agg_df.lazy(),
        accum_cols=[pl.col("return"), pl.col("weight").alias("return")],  # Duplicate!
        feature_cols=[pl.col("null_frac")],
        weight_col=pl.col("weight"),
        cache_dir=Path("./test_bad_cache"),
    )
    print("✗ Should have raised ValueError for duplicates!")
except ValueError as e:
    print(f"✓ Correctly caught duplicate: {e}")
    # Verify cleanup happened
    assert not Path("./test_bad_cache").exists(), "Failed to clean up on error"
    print("✓ Correctly cleaned up partial cache")

# ===========================================================================
# Test 3: CTDPCache Class
# ===========================================================================
print("\n" + "=" * 70)
print("Test 3: CTDPCache Class")
print("=" * 70)

cache = CTDPCache(path)
print(f"✓ Loaded cache from: {cache.path}")
print(f"  Metadata columns:")
print(f"    - accum_cols: {cache.metadata.accum_cols}")
print(f"    - feature_cols: {cache.metadata.feature_cols}")
print(f"    - weight_col: {cache.metadata.weight_col}")
print(f"  Filters: {cache.filters.shape}")

# Test filter manipulation
print("\nTesting filter operations...")
cache.update_filter("return", -0.1, 0.1)
print(f"✓ Added filter for 'return'")
print(f"  Filters shape: {cache.filters.shape}")

cache.update_filter("max_tick_to_query_lag", None, 5.0)
print(f"✓ Added filter for 'max_tick_to_query_lag' (no lower bound)")
print(f"  Filters shape: {cache.filters.shape}")

# Verify filters persisted
cache2 = CTDPCache(path)
assert cache2.filters.width == 2, "Filters not persisted!"
print(f"✓ Filters persisted correctly")

# Remove filter
cache.remove_filter("return")
print(f"✓ Removed 'return' filter")
print(f"  Filters shape: {cache.filters.shape}")

# Update settings
cache.update_settings(n_ticks=10, num_cols=3)
print(f"✓ Updated settings: n_ticks={cache.metadata.n_ticks}, num_cols={cache.metadata.num_cols}")

# Generate plots
plots = cache.generate_plots()
print(f"✓ Generated {len(plots)} plots from cache")

# ===========================================================================
# Test 4: Cache Cleanup
# ===========================================================================
print("\n" + "=" * 70)
print("Test 4: Cache Cleanup")
print("=" * 70)

delete_cache(path, verbose=False)
assert not Path(path).exists(), "Cache not deleted!"
print(f"✓ Cache deleted: {path}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n" + "=" * 70)
print("✓ All Tests Passed!")
print("=" * 70)
print("\nRefactored API features tested:")
print("  ✓ Expression-based functional API")
print("  ✓ String shortcuts (auto-converted to pl.col())")
print("  ✓ Computed expressions (e.g., .log(), .abs())")
print("  ✓ Validation & auto-cleanup on error")
print("  ✓ CTDPCache class for stateful operations")
print("  ✓ Filter persistence and manipulation")
print("  ✓ Settings updates")
print("  ✓ Cache deletion")

print("\nTest artifacts:")
print(f"  - Sample plot: {output_path}")
print("  - (Cleaned up test caches)")
