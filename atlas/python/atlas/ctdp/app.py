"""Streamlit app interface for CTDP visualization"""

from pathlib import Path
from typing import Optional, Tuple, List, Any, Union
from datetime import datetime, date, timedelta

import streamlit as st
import polars as pl
import plotly.graph_objects as go

from .state import CTDPCache
from .formatting import detect_col_type, get_col_range
from .plotting import _apply_filters


def run_app(cache_dir: Union[str, Path]):
    """
    Main Streamlit app entry point.

    Loads cache, renders UI, handles interactions, and persists changes.

    Args:
        cache_dir: Path to CTDP cache directory
    """
    st.set_page_config(page_title="CTDP Viewer", layout="wide")

    # Load cache
    cache = CTDPCache(cache_dir)

    # Load full data once for filter ranges
    df_full = cache.load_data().collect()

    # Sidebar controls
    st.sidebar.header("⚙️ Controls")
    n_ticks = st.sidebar.number_input(
        "X-axis ticks",
        min_value=3,
        max_value=20,
        value=cache.metadata.n_ticks,
    )
    num_cols = st.sidebar.number_input(
        "Grid columns",
        min_value=1,
        max_value=4,
        value=cache.metadata.num_cols,
    )

    # Update settings if changed
    if n_ticks != cache.metadata.n_ticks or num_cols != cache.metadata.num_cols:
        cache.update_settings(n_ticks=n_ticks, num_cols=num_cols)

    st.sidebar.divider()

    # Render filters and collect updates
    new_filters = _render_filter_grid(
        df=df_full,
        feature_cols=cache.metadata.feature_cols,
        current_filters=cache.filters,
        num_cols=num_cols,
    )

    # Save if changed
    if not new_filters.equals(cache.filters):
        cache._filters = new_filters
        cache.save_filters()

    # Compute stats
    if cache.filters.width > 0:
        df_filtered = _apply_filters(cache.load_data(), cache.filters).collect()
    else:
        df_filtered = df_full

    # Display stats in sidebar
    st.sidebar.header("📊 Statistics")
    st.sidebar.metric("Filtered", f"{len(df_filtered):,}")
    st.sidebar.metric("Total", f"{len(df_full):,}")
    retention = 100 * len(df_filtered) / len(df_full) if len(df_full) > 0 else 0
    st.sidebar.metric("Retention", f"{retention:.1f}%")

    # Generate and render plots
    if len(df_filtered) > 0:
        plots = cache.generate_plots()
        _render_plots_grid(
            plots=plots,
            feature_cols=cache.metadata.feature_cols,
            accum_cols=cache.metadata.accum_cols,
            num_cols=num_cols,
        )
    else:
        st.warning("⚠️ No data matches filters!")


def _render_filter_grid(
    df: pl.DataFrame,
    feature_cols: List[str],
    current_filters: pl.DataFrame,
    num_cols: int,
) -> pl.DataFrame:
    """
    Render grid of filter widgets.

    Args:
        df: Full DataFrame (for computing ranges)
        feature_cols: List of feature column names
        current_filters: Current filters DataFrame
        num_cols: Number of columns in grid

    Returns:
        Updated filters DataFrame
    """
    # Import here to avoid circular dependency
    from .state import _update_filter_df

    new_filters = (
        current_filters.clone() if current_filters.width > 0 else pl.DataFrame()
    )

    # Render in grid
    for i in range(0, len(feature_cols), num_cols):
        cols = st.columns(num_cols)
        for j, col in enumerate(cols):
            if i + j < len(feature_cols):
                feat_col = feature_cols[i + j]
                with col:
                    with st.container(border=True):
                        st.markdown(f"### {feat_col}")

                        # Get current filter range if exists
                        if feat_col in current_filters.columns:
                            current_min = current_filters[feat_col][0]
                            current_max = current_filters[feat_col][1]
                            current_range = (current_min, current_max)
                        else:
                            current_range = None

                        # Create filter widget
                        min_val, max_val = _create_filter_widget(
                            df=df,
                            col_name=feat_col,
                            current_range=current_range,
                            key_prefix=f"filter_{feat_col}",
                        )

                        # Update filters
                        new_filters = _update_filter_df(
                            new_filters, feat_col, min_val, max_val
                        )

    return new_filters


def _create_filter_widget(
    df: pl.DataFrame,
    col_name: str,
    current_range: Optional[Tuple[Any, Any]] = None,
    key_prefix: str = "",
) -> Tuple[Any, Any]:
    """
    Create adaptive filter widget based on column type.

    Args:
        df: DataFrame containing the column
        col_name: Column name to create filter for
        current_range: Current (min, max) filter values, or None
        key_prefix: Unique key prefix for widget

    Returns:
        (min_val, max_val) tuple
    """
    col_type = detect_col_type(df, col_name)
    data_min, data_max = get_col_range(df, col_name)

    # Set default range if not specified
    if current_range is None:
        current_range = (data_min, data_max)

    # Reset button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Range**")
    with col2:
        if st.button("↺", key=f"reset_{key_prefix}", help="Reset filter"):
            st.session_state[f"{key_prefix}_min"] = data_min
            st.session_state[f"{key_prefix}_max"] = data_max
            st.rerun()

    # Type-specific widgets
    if col_type == "timedelta":
        # Convert to seconds for input
        data_min_sec = data_min.total_seconds()
        data_max_sec = data_max.total_seconds()
        current_min_sec = (
            current_range[0].total_seconds() if current_range[0] else data_min_sec
        )
        current_max_sec = (
            current_range[1].total_seconds() if current_range[1] else data_max_sec
        )

        min_val = st.number_input(
            "Min (seconds)",
            min_value=data_min_sec,
            max_value=data_max_sec,
            value=st.session_state.get(f"{key_prefix}_min", current_min_sec),
            format="%.3f",
            key=f"{key_prefix}_min",
        )
        max_val = st.number_input(
            "Max (seconds)",
            min_value=data_min_sec,
            max_value=data_max_sec,
            value=st.session_state.get(f"{key_prefix}_max", current_max_sec),
            format="%.3f",
            key=f"{key_prefix}_max",
        )
        return (timedelta(seconds=min_val), timedelta(seconds=max_val))

    elif col_type == "datetime":
        # Use date inputs for datetime
        data_min_date = data_min.date()
        data_max_date = data_max.date()
        current_min_date = (
            current_range[0].date() if current_range[0] else data_min_date
        )
        current_max_date = (
            current_range[1].date() if current_range[1] else data_max_date
        )

        min_val = st.date_input(
            "Min",
            value=st.session_state.get(f"{key_prefix}_min", current_min_date),
            key=f"{key_prefix}_min",
        )
        max_val = st.date_input(
            "Max",
            value=st.session_state.get(f"{key_prefix}_max", current_max_date),
            key=f"{key_prefix}_max",
        )
        # Convert back to datetime
        return (
            datetime.combine(min_val, datetime.min.time()),
            datetime.combine(max_val, datetime.max.time()),
        )

    elif col_type == "date":
        current_min_date = current_range[0] if current_range[0] else data_min
        current_max_date = current_range[1] if current_range[1] else data_max

        min_val = st.date_input(
            "Min",
            value=st.session_state.get(f"{key_prefix}_min", current_min_date),
            key=f"{key_prefix}_min",
        )
        max_val = st.date_input(
            "Max",
            value=st.session_state.get(f"{key_prefix}_max", current_max_date),
            key=f"{key_prefix}_max",
        )
        return (min_val, max_val)

    else:  # numeric
        data_min_float = float(data_min)
        data_max_float = float(data_max)
        current_min_float = (
            float(current_range[0]) if current_range[0] else data_min_float
        )
        current_max_float = (
            float(current_range[1]) if current_range[1] else data_max_float
        )

        min_val = st.number_input(
            "Min",
            min_value=data_min_float,
            max_value=data_max_float,
            value=st.session_state.get(f"{key_prefix}_min", current_min_float),
            key=f"{key_prefix}_min",
        )
        max_val = st.number_input(
            "Max",
            min_value=data_min_float,
            max_value=data_max_float,
            value=st.session_state.get(f"{key_prefix}_max", current_max_float),
            key=f"{key_prefix}_max",
        )
        return (min_val, max_val)


def _render_plots_grid(
    plots: List[go.Figure],
    feature_cols: List[str],
    accum_cols: List[str],
    num_cols: int,
):
    """
    Render grid of plots.

    Args:
        plots: List of plotly Figures
        feature_cols: Feature column names (for titles)
        accum_cols: Accumulator column names (for trace names)
        num_cols: Number of columns in grid
    """
    for i in range(0, len(plots), num_cols):
        cols = st.columns(num_cols)
        for j, col in enumerate(cols):
            if i + j < len(plots):
                fig = plots[i + j]
                with col:
                    st.plotly_chart(fig, use_container_width=True)
