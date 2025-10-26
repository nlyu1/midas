"""Interactive Slice Plots with Range Filters
Run: streamlit run slice_plots_app.py
"""

import streamlit as st
import polars as pl
import numpy as np
import hvplot.polars
import holoviews as hv
from datetime import timedelta as Timedelta, datetime as Datetime, date as Date
from typing import Union, Dict, Callable, Any, Tuple

# Initialize Plotly backend for hvplot
hv.extension("plotly")

# ============================================================================
# Formatting & Plotting Functions
# ============================================================================


def quick_format(content: Union[float, Timedelta, str]) -> str:
    """Format float to 2 decimals, timedelta to seconds, else string"""
    if isinstance(content, float):
        return f"{content:.2f}"
    elif isinstance(content, Timedelta):
        return f"{content.total_seconds():.3f}"
    return str(content)


def slice_plots(
    plot_df,
    accum_cols,
    cont_feature_cols,
    weight_col,
    n_ticks=5,
    plot_subsample=10_000,
    format_fns={},
):
    """Generate slice plots for continuous features"""
    plots = []
    for cont_feature in cont_feature_cols:
        feat_name = cont_feature.meta.output_name()

        # Sort, cumsum, and subsample
        sorted_df = (
            plot_df.select(cont_feature, weight_col, *accum_cols)
            .sort(cont_feature)
            .with_columns(
                weight_col.cum_sum() / weight_col.sum(),
                *[c.cum_sum() for c in accum_cols],
            )
        )

        # Generate x-axis ticks at quantiles
        quantiles = np.linspace(0, 1, n_ticks)
        format_fn = format_fns.get(feat_name, quick_format)
        xticks = []
        for q in quantiles:
            q_val = sorted_df[feat_name].quantile(q)
            idx = (sorted_df[feat_name] - q_val).abs().arg_min()
            xticks.append(
                (sorted_df["weight"][idx], format_fn(sorted_df[feat_name][idx]))
            )

        # Create plot
        plot = sorted_df.hvplot.line(
            x="weight",
            y=[ac.meta.output_name() for ac in accum_cols],
            hover_cols=[feat_name],
        ).opts(xticks=xticks, xlabel=feat_name, width=700, height=400)

        plots.append((feat_name, plot))
    return plots


# ============================================================================
# Adaptive Filter Functions
# ============================================================================


def get_col_type(df: pl.DataFrame, col_name: str) -> str:
    """Detect column type: 'timedelta', 'datetime', 'date', or 'numeric'"""
    dtype = df.schema[col_name]
    if dtype == pl.Duration:
        return "timedelta"
    elif dtype == pl.Datetime:
        return "datetime"
    elif dtype == pl.Date:
        return "date"
    else:
        return "numeric"


def create_filter_widget(
    df: pl.DataFrame, feat_name: str, key_prefix: str, use_sidebar: bool = False
) -> Tuple[Any, Any]:
    """Create appropriate filter widget based on column type"""
    col_type = get_col_type(df, feat_name)
    col_min_raw, col_max_raw = df[feat_name].min(), df[feat_name].max()

    # Choose where to render widgets
    widget = st.sidebar if use_sidebar else st

    if col_type == "timedelta":
        min_secs = col_min_raw.total_seconds()
        max_secs = col_max_raw.total_seconds()

        # Reset button
        col1, col2 = widget.columns([3, 1])
        with col1:
            widget.markdown("**Range (seconds)**")
        with col2:
            if widget.button("↺", key=f"reset_{key_prefix}", help="Reset filter"):
                st.session_state[f"{key_prefix}_min"] = min_secs
                st.session_state[f"{key_prefix}_max"] = max_secs
                st.rerun()

        min_val = widget.number_input(
            "Min",
            min_secs,
            max_secs,
            value=st.session_state.get(f"{key_prefix}_min", min_secs),
            format="%.3f",
            key=f"{key_prefix}_min",
        )
        max_val = widget.number_input(
            "Max",
            min_secs,
            max_secs,
            value=st.session_state.get(f"{key_prefix}_max", max_secs),
            format="%.3f",
            key=f"{key_prefix}_max",
        )
        return Timedelta(seconds=min_val), Timedelta(seconds=max_val)

    elif col_type == "datetime":
        col1, col2 = widget.columns([3, 1])
        with col1:
            widget.markdown("**Date Range**")
        with col2:
            if widget.button("↺", key=f"reset_{key_prefix}", help="Reset filter"):
                st.session_state[f"{key_prefix}_min"] = col_min_raw.date()
                st.session_state[f"{key_prefix}_max"] = col_max_raw.date()
                st.rerun()

        min_val = widget.date_input(
            "Min",
            value=st.session_state.get(f"{key_prefix}_min", col_min_raw.date()),
            key=f"{key_prefix}_min",
        )
        max_val = widget.date_input(
            "Max",
            value=st.session_state.get(f"{key_prefix}_max", col_max_raw.date()),
            key=f"{key_prefix}_max",
        )
        return Datetime.combine(min_val, Datetime.min.time()), Datetime.combine(
            max_val, Datetime.max.time()
        )

    elif col_type == "date":
        col1, col2 = widget.columns([3, 1])
        with col1:
            widget.markdown("**Date Range**")
        with col2:
            if widget.button("↺", key=f"reset_{key_prefix}", help="Reset filter"):
                st.session_state[f"{key_prefix}_min"] = col_min_raw
                st.session_state[f"{key_prefix}_max"] = col_max_raw
                st.rerun()

        min_val = widget.date_input(
            "Min",
            value=st.session_state.get(f"{key_prefix}_min", col_min_raw),
            key=f"{key_prefix}_min",
        )
        max_val = widget.date_input(
            "Max",
            value=st.session_state.get(f"{key_prefix}_max", col_max_raw),
            key=f"{key_prefix}_max",
        )
        return min_val, max_val

    else:  # numeric
        col_min, col_max = float(col_min_raw), float(col_max_raw)

        col1, col2 = widget.columns([3, 1])
        with col1:
            widget.markdown("**Range**")
        with col2:
            if widget.button("↺", key=f"reset_{key_prefix}", help="Reset filter"):
                st.session_state[f"{key_prefix}_min"] = col_min
                st.session_state[f"{key_prefix}_max"] = col_max
                st.rerun()

        min_val = widget.number_input(
            "Min",
            col_min,
            col_max,
            value=st.session_state.get(f"{key_prefix}_min", col_min),
            key=f"{key_prefix}_min",
        )
        max_val = widget.number_input(
            "Max",
            col_min,
            col_max,
            value=st.session_state.get(f"{key_prefix}_max", col_max),
            key=f"{key_prefix}_max",
        )
        return min_val, max_val


# ============================================================================
# Data Loading & Preprocessing
# ============================================================================


@st.cache_data
def load_data():
    """Load and preprocess data from parquet"""
    df = pl.read_parquet("./plotting_dev_parquet.parquet")
    return (
        df.group_by("symbol", "date")
        .agg(
            pl.col("max_tick_to_query_lag").mean(),
            pl.col("return").sum(),
            (pl.col("return").null_count() / pl.len()).alias("null_frac"),
        )
        .with_columns(pl.col("symbol").cast(pl.String).str.len_chars().alias("weight"))
    )


# ============================================================================
# Streamlit App
# ============================================================================

st.set_page_config(page_title="Slice Plots", layout="wide")
# st.title("Interactive Slice Plots")

# Load data
plot_df = load_data()

# Configuration
accum_cols = [pl.col("null_frac")]
cont_feature_cols = [pl.col("return"), pl.col("max_tick_to_query_lag")]
weight_col = pl.col("weight")

# ============================================================================
# Sidebar: Controls & Stats
# ============================================================================
st.sidebar.header("⚙️ Controls")
n_ticks = st.sidebar.number_input("X-axis ticks", 3, 20, 5)
plot_subsample = st.sidebar.number_input("Plot subsample", 1000, 100_000, 10_000)
num_cols = st.sidebar.number_input("Grid columns", 1, 4, 2)

st.sidebar.divider()

# Collect filters from grid (first pass)
filters = {}
for i in range(0, len(cont_feature_cols), num_cols):
    cols = st.columns(num_cols)
    for j, col in enumerate(cols):
        if i + j < len(cont_feature_cols):
            feat = cont_feature_cols[i + j]
            feat_name = feat.meta.output_name()
            with col:
                with st.container(border=True):
                    st.markdown(f"### {feat_name}")
                    min_val, max_val = create_filter_widget(
                        plot_df, feat_name, f"filter_{feat_name}", use_sidebar=False
                    )
                    filters[feat_name] = (min_val, max_val)

# Apply filters
filtered_df = plot_df
for feat_name, (min_val, max_val) in filters.items():
    filtered_df = filtered_df.filter(
        (pl.col(feat_name) >= min_val) & (pl.col(feat_name) <= max_val)
    )

# Display stats in sidebar
st.sidebar.header("📊 Statistics")
st.sidebar.metric("Filtered", f"{len(filtered_df):,}")
st.sidebar.metric("Total", f"{len(plot_df):,}")
st.sidebar.metric("Retention", f"{100 * len(filtered_df) / len(plot_df):.1f}%")

# ============================================================================
# Main Area: Generate and display plots in same grid
# ============================================================================
if len(filtered_df) > 0:
    plots = slice_plots(
        filtered_df, accum_cols, cont_feature_cols, weight_col, n_ticks, plot_subsample
    )

    # Re-render grid with plots
    for i in range(0, len(cont_feature_cols), num_cols):
        cols = st.columns(num_cols)
        for j, col in enumerate(cols):
            if i + j < len(cont_feature_cols):
                feat_name, plot = plots[i + j]
                with col:
                    # Modify Plotly figure for better legend/axis
                    fig_dict = hv.render(plot)
                    fig_dict.pop("config", None)

                    # Update layout: legend on bottom-right, no y-axis label
                    if "layout" not in fig_dict:
                        fig_dict["layout"] = {}

                    fig_dict["layout"]["legend"] = {
                        "orientation": "h",
                        "yanchor": "bottom",
                        "y": 0.02,
                        "xanchor": "right",
                        "x": 0.98,
                    }
                    fig_dict["layout"]["yaxis"] = {"title": ""}
                    fig_dict["layout"]["margin"] = {"l": 40, "r": 20, "t": 20, "b": 40}

                    # Update trace names to use accum col names
                    if "data" in fig_dict:
                        for k, trace in enumerate(fig_dict["data"]):
                            if k < len(accum_cols):
                                trace["name"] = accum_cols[k].meta.output_name()

                    st.plotly_chart(fig_dict, use_container_width=True)
else:
    st.warning("⚠️ No data matches the current filters!")
