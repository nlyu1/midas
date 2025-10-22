"""Test Step 3: Core plotting functionality"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import polars as pl
from atlas.ctdp.plotting import plot_ctdp

print("✓ Step 3: Testing plotting.py")

# Load the actual test data (same as used in slice_plots_app.py)
print("\n1. Loading data...")
df = pl.read_parquet("./plotting_dev_parquet.parquet")
print(f"  Loaded {len(df):,} rows")
print(f"  Columns: {df.columns}")

# Aggregate like in the original app
print("\n2. Aggregating data...")
agg_df = (
    df.group_by("symbol", "date")
    .agg(
        pl.col("max_tick_to_query_lag").mean(),
        pl.col("return").sum(),
        (pl.col("return").null_count() / pl.len()).alias("null_frac"),
    )
    .with_columns(pl.col("symbol").cast(pl.String).str.len_chars().alias("weight"))
)
print(f"  Aggregated to {len(agg_df):,} rows")

# Convert to LazyFrame for the API
lazy_df = agg_df.lazy()

# Generate plots
print("\n3. Generating CTDP plots...")
plots = plot_ctdp(
    df=lazy_df,
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
    n_ticks=5,
)
print(f"  Generated {len(plots)} plots")

# Test with filters
print("\n4. Testing with filters...")
# Create a simple filter: return between -0.1 and 0.1
filter_df = pl.DataFrame({
    'return': [-0.1, 0.1],  # row 0 = min, row 1 = max
})
filtered_plots = plot_ctdp(
    df=lazy_df,
    accum_cols=['null_frac'],
    feature_cols=['return', 'max_tick_to_query_lag'],
    weight_col='weight',
    n_ticks=5,
    filters=filter_df,
)
print(f"  Generated {len(filtered_plots)} filtered plots")

# Save one plot as HTML to verify it works
print("\n5. Saving sample plot...")
output_path = Path("./test_ctdp_plot.html")
plots[0].write_html(output_path)
print(f"  Saved to: {output_path}")
print(f"  Open in browser to verify: file://{output_path.absolute()}")

print("\n✓ All plotting tests passed!")
print("\nPlot details:")
for i, fig in enumerate(plots):
    feature_name = ['return', 'max_tick_to_query_lag'][i]
    num_traces = len(fig.data)
    print(f"  Plot {i+1} ({feature_name}): {num_traces} trace(s)")
