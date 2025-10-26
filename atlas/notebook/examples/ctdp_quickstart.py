"""
CTDP Quickstart Example

Demonstrates the three core API patterns:
1. Functional direct plotting (stateless)
2. Creating a CTDP cache
3. Using CTDPCache for interactive filtering

This example analyzes how data quality issues (missing values)
distribute across different trading metrics.
"""

import sys
from pathlib import Path

# Add atlas to path (adjust if needed)
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python"))

import polars as pl
from atlas.ctdp import plot_ctdp, create_ctdp_cache, CTDPCache

# ==============================================================================
# Setup: Generate sample data
# ==============================================================================

print("Generating sample trading data...")

# Create realistic sample data
n_samples = 10000
pl.set_random_seed(42)

df = pl.DataFrame({
    'symbol': pl.arange(0, n_samples) % 100,
    'date': pl.date_range(
        pl.date(2024, 1, 1),
        pl.date(2024, 12, 31),
        interval='1d',
        eager=True
    ).head(n_samples),
    'return': pl.Series([None if i % 10 == 0 else v for i, v in enumerate(
        (pl.arange(0, n_samples).cast(pl.Float64) / 1000 - 5).to_list()
    )]),
    'lag': (pl.arange(0, n_samples).cast(pl.Float64) / 100).exp(),
}).with_columns(
    # Add data quality metric: fraction of missing values
    (pl.col('return').is_null().cast(pl.Float64)).alias('is_null'),
    # Weight: treat all samples equally
    pl.lit(1.0).alias('weight'),
)

print(f"Generated {len(df):,} rows")
print(f"Columns: {df.columns}")
print()

# ==============================================================================
# Pattern 1: Functional API - Direct Plotting (Stateless)
# ==============================================================================

print("=" * 70)
print("Pattern 1: Functional API - Direct plotting")
print("=" * 70)

# Generate plots directly without caching
plots = plot_ctdp(
    df=df.lazy(),
    accum_cols=['is_null'],           # Track nulls
    feature_cols=['return', 'lag'],   # Across return and lag distributions
    weight_col='weight',
    n_ticks=5,
)

print(f"✓ Generated {len(plots)} plots")
print("  - Plot 0: Null accumulation vs return")
print("  - Plot 1: Null accumulation vs lag")

# Save first plot
output_file = Path(__file__).parent / "ctdp_example_plot.html"
plots[0].write_html(output_file)
print(f"✓ Saved plot to: {output_file}")
print()

# Advanced: Use computed features with Polars expressions
print("Using computed features...")
plots_computed = plot_ctdp(
    df=df.lazy(),
    accum_cols=[pl.col('is_null')],
    feature_cols=[
        pl.col('return'),
        pl.col('return').abs().alias('abs_return'),  # Magnitude
        pl.col('lag').log().alias('log_lag'),        # Log-scale
    ],
    weight_col=pl.col('weight'),
    n_ticks=5,
)
print(f"✓ Generated {len(plots_computed)} plots (including computed features)")
print()

# ==============================================================================
# Pattern 2: Create CTDP Cache
# ==============================================================================

print("=" * 70)
print("Pattern 2: Creating CTDP cache")
print("=" * 70)

# Create persistent cache for interactive exploration
cache_dir = Path(__file__).parent / "example_cache"

path = create_ctdp_cache(
    df=df.lazy(),
    accum_cols=['is_null'],
    feature_cols=['return', 'lag'],
    weight_col='weight',
    cache_dir=cache_dir,
    n_ticks=7,
    num_cols=2,
)

print(f"✓ Cache created at: {path}")
print("  Cache contents:")
for fname in ['data.parquet', 'metadata.json', 'filters.parquet']:
    fpath = Path(path) / fname
    print(f"    - {fname:20} ({fpath.stat().st_size:,} bytes)")
print()

# ==============================================================================
# Pattern 3: Using CTDPCache - Interactive Filtering
# ==============================================================================

print("=" * 70)
print("Pattern 3: CTDPCache - Interactive filtering")
print("=" * 70)

# Load cache and manipulate filters
cache = CTDPCache(path)
print(f"✓ Loaded cache from: {cache.path}")
print(f"  Metadata:")
print(f"    - accum_cols: {cache.metadata.accum_cols}")
print(f"    - feature_cols: {cache.metadata.feature_cols}")
print(f"    - weight_col: {cache.metadata.weight_col}")
print()

# Apply filters to focus on specific data ranges
print("Applying filters...")
cache.update_filter('return', -0.5, 0.5)       # Focus on small returns
cache.update_filter('lag', None, 100.0)        # Cap lag at 100 (no lower bound)
print(f"✓ Applied 2 filters")
print(f"  - return: [-0.5, 0.5]")
print(f"  - lag: [None, 100.0]")
print()

# Generate plots with filters
plots_filtered = cache.generate_plots()
print(f"✓ Generated {len(plots_filtered)} filtered plots")

# Save filtered plot
filtered_output = Path(__file__).parent / "ctdp_filtered_plot.html"
plots_filtered[0].write_html(filtered_output)
print(f"✓ Saved filtered plot to: {filtered_output}")
print()

# Remove a filter and regenerate
print("Removing return filter...")
cache.remove_filter('return')
plots_partial = cache.generate_plots()
print(f"✓ Generated {len(plots_partial)} plots (lag filter still active)")
print()

# Update plotting settings
print("Updating plot settings...")
cache.update_settings(n_ticks=10, num_cols=1)
plots_updated = cache.generate_plots()
print(f"✓ Generated plots with updated settings (10 ticks)")
print()

# ==============================================================================
# Summary
# ==============================================================================

print("=" * 70)
print("Summary")
print("=" * 70)
print()
print("Three API patterns demonstrated:")
print()
print("1. Functional (stateless)")
print("   - Quick exploration with plot_ctdp()")
print("   - Supports Polars expressions for computed features")
print("   - No persistence, regenerate on each call")
print()
print("2. Cache creation")
print("   - Use create_ctdp_cache() to persist data")
print("   - Evaluates expressions once, saves compressed data")
print("   - Returns path for later reuse")
print()
print("3. CTDPCache (stateful)")
print("   - Load cache with CTDPCache(path)")
print("   - Manipulate filters interactively")
print("   - Update settings without recomputing features")
print("   - Ideal for iterative exploration or apps")
print()
print("Output files:")
print(f"  - {output_file}")
print(f"  - {filtered_output}")
print(f"  - Cache directory: {cache_dir}")
print()
print("Next steps:")
print("  - Open HTML plots in browser to explore")
print("  - Modify filters in the CTDPCache section")
print("  - Try with your own data!")
