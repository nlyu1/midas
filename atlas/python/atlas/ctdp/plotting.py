"""Core CTDP plotting functionality - fully vectorized using Polars and Plotly"""

from typing import List, Dict, Callable, Optional, Tuple, Any
import polars as pl
import plotly.graph_objects as go
import numpy as np

from .formatting import detect_col_type, format_value, get_col_range
from .config import DEFAULT_N_TICKS


def plot_ctdp(
    df: pl.LazyFrame,
    accum_cols: List[str],
    feature_cols: List[str],
    weight_col: str,
    n_ticks: int = DEFAULT_N_TICKS,
    filters: Optional[pl.DataFrame] = None,
    format_fns: Optional[Dict[str, Callable[[Any], str]]] = None,
) -> List[go.Figure]:
    """
    Generate Cumulative Total-Dependence Plots (CTDP).

    For each feature column:
    1. Sort data by feature values
    2. Compute cumulative sums of weight and accumulator columns
    3. Normalize weight cumsum to [0, 1]
    4. Generate x-axis ticks at quantiles showing feature values
    5. Create plotly line plot with accum_cols as traces

    Args:
        df: Input LazyFrame
        accum_cols: Column names to accumulate (y-axis metrics)
        feature_cols: Feature column names to slice by (one plot per feature)
        weight_col: Weight column name
        n_ticks: Number of x-axis ticks at quantiles (default: 5)
        filters: Optional 2-row DataFrame with min/max filter values
        format_fns: Optional custom formatters {feature_name: format_fn}

    Returns:
        List of plotly Figures, one per feature column

    Example:
        >>> plots = plot_ctdp(
        ...     df=my_lazyframe,
        ...     accum_cols=['null_frac'],
        ...     feature_cols=['return', 'lag'],
        ...     weight_col='weight',
        ... )
        >>> plots[0].show()
    """
    # Apply filters if provided
    if filters is not None and filters.width > 0:
        df = _apply_filters(df, filters)

    # Collect LazyFrame once
    df_collected = df.collect()

    # Generate format functions
    if format_fns is None:
        format_fns = {}

    # Create one plot per feature
    plots = []
    for feature_col in feature_cols:
        # Get or create format function
        if feature_col not in format_fns:
            col_type = detect_col_type(df_collected, feature_col)
            format_fn = lambda val, ct=col_type: format_value(val, ct)
        else:
            format_fn = format_fns[feature_col]

        # Create plot
        fig = _create_single_plot(
            df=df_collected,
            feature_col=feature_col,
            accum_cols=accum_cols,
            weight_col=weight_col,
            n_ticks=n_ticks,
            format_fn=format_fn,
        )
        plots.append(fig)

    return plots


def _apply_filters(df: pl.LazyFrame, filters: pl.DataFrame) -> pl.LazyFrame:
    """
    Apply 2-row filter DataFrame to LazyFrame.

    Filter format: Each column represents a filtered feature
    - Row 0: min value (None = no lower bound)
    - Row 1: max value (None = no upper bound)

    Args:
        df: Input LazyFrame
        filters: 2-row DataFrame with filter bounds

    Returns:
        Filtered LazyFrame
    """
    if filters.width == 0:
        return df

    for col_name in filters.columns:
        min_val = filters[col_name][0]
        max_val = filters[col_name][1]

        if min_val is not None:
            df = df.filter(pl.col(col_name) >= min_val)
        if max_val is not None:
            df = df.filter(pl.col(col_name) <= max_val)

    return df


def _create_single_plot(
    df: pl.DataFrame,
    feature_col: str,
    accum_cols: List[str],
    weight_col: str,
    n_ticks: int,
    format_fn: Callable[[Any], str],
) -> go.Figure:
    """
    Create a single CTDP plot for one feature.

    Args:
        df: Polars DataFrame (already collected)
        feature_col: Feature to slice by
        accum_cols: Columns to accumulate
        weight_col: Weight column
        n_ticks: Number of x-axis ticks
        format_fn: Function to format feature values for display

    Returns:
        Plotly Figure
    """
    # Prepare data: sort, cumsum, normalize
    plot_df = _prepare_plot_data(df, feature_col, accum_cols, weight_col)

    # Generate x-axis ticks at quantiles
    xticks = _generate_xticks(plot_df, feature_col, 'weight_norm', n_ticks, format_fn)

    # Create plotly figure
    fig = _create_plotly_figure(
        plot_df=plot_df,
        feature_col=feature_col,
        accum_cols=accum_cols,
        weight_col='weight_norm',
        xticks=xticks,
    )

    return fig


def _prepare_plot_data(
    df: pl.DataFrame,
    feature_col: str,
    accum_cols: List[str],
    weight_col: str,
) -> pl.DataFrame:
    """
    Prepare data for plotting (fully vectorized).

    Steps:
    1. Select relevant columns
    2. Sort by feature
    3. Compute cumulative sums
    4. Normalize weight cumsum to [0, 1]

    Args:
        df: Input DataFrame
        feature_col: Feature column to sort by
        accum_cols: Columns to accumulate
        weight_col: Weight column

    Returns:
        DataFrame with:
        - feature_col: original feature values
        - weight_norm: normalized weight cumsum [0, 1]
        - {accum_col}_cumsum: cumulative sums of accum_cols
    """
    # Select and sort
    selected_df = df.select(
        [feature_col, weight_col] + accum_cols
    ).sort(feature_col)

    # Compute cumulative sums
    cumsum_exprs = [pl.col(ac).cum_sum().alias(f"{ac}_cumsum") for ac in accum_cols]
    weight_cumsum = pl.col(weight_col).cum_sum()
    weight_total = pl.col(weight_col).sum()

    result_df = selected_df.with_columns([
        (weight_cumsum / weight_total).alias('weight_norm'),
        *cumsum_exprs,
    ])

    return result_df


def _generate_xticks(
    df: pl.DataFrame,
    feature_col: str,
    weight_col: str,
    n_ticks: int,
    format_fn: Callable[[Any], str],
) -> List[Tuple[float, str]]:
    """
    Generate x-axis tick positions and labels at quantiles.

    Vectorized approach:
    1. Compute quantiles of feature
    2. Find nearest row indices in sorted data
    3. Extract (weight_position, formatted_feature_value)

    Args:
        df: Prepared DataFrame (sorted by feature)
        feature_col: Feature column name
        weight_col: Normalized weight column (x-axis values)
        n_ticks: Number of ticks
        format_fn: Function to format feature values

    Returns:
        List of (x_position, x_label) tuples
    """
    quantiles = np.linspace(0, 1, n_ticks)
    xticks = []

    for q in quantiles:
        # Get quantile value of feature
        q_val = df[feature_col].quantile(q)

        # Find nearest index (vectorized)
        idx = (df[feature_col] - q_val).abs().arg_min()

        # Extract position and label
        x_pos = df[weight_col][idx]
        x_label = format_fn(df[feature_col][idx])

        xticks.append((x_pos, x_label))

    return xticks


def _create_plotly_figure(
    plot_df: pl.DataFrame,
    feature_col: str,
    accum_cols: List[str],
    weight_col: str,
    xticks: List[Tuple[float, str]],
    width: int = 700,
    height: int = 400,
) -> go.Figure:
    """
    Create Plotly figure with custom styling.

    Features:
    - One trace per accum_col
    - Custom x-axis ticks showing feature values at quantiles
    - Horizontal legend (bottom-right)
    - No y-axis title (shown in legend/trace names)
    - Hover shows feature value

    Args:
        plot_df: Prepared DataFrame
        feature_col: Feature column (for hover)
        accum_cols: Accumulator columns (one trace each)
        weight_col: Normalized weight column (x-axis)
        xticks: List of (x_position, x_label) tuples
        width: Figure width in pixels
        height: Figure height in pixels

    Returns:
        Plotly Figure
    """
    fig = go.Figure()

    # Add one trace per accumulator column
    for accum_col in accum_cols:
        cumsum_col = f"{accum_col}_cumsum"

        fig.add_trace(go.Scatter(
            x=plot_df[weight_col].to_list(),
            y=plot_df[cumsum_col].to_list(),
            mode='lines',
            name=accum_col,
            hovertemplate=(
                f'{accum_col}: %{{y:.4f}}<br>'
                f'{feature_col}: %{{customdata}}<br>'
                '<extra></extra>'
            ),
            customdata=plot_df[feature_col].to_list(),
        ))

    # Update layout
    fig.update_layout(
        xaxis=dict(
            title=feature_col,
            tickmode='array',
            tickvals=[x[0] for x in xticks],
            ticktext=[x[1] for x in xticks],
        ),
        yaxis=dict(
            title='',  # No y-axis title (use trace names)
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=0.02,
            xanchor='right',
            x=0.98,
        ),
        width=width,
        height=height,
        margin=dict(l=40, r=20, t=20, b=40),
        hovermode='closest',
    )

    return fig
