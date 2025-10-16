import polars as pl
from typing import Dict, List, Optional
import plotly.graph_objects as go
from .array import hist_tensor

def hist_df(
    df: pl.DataFrame,
    columns: Optional[List[str]] = None,
    bins: int = 50,
    **kwargs
) -> Dict[str, go.Figure]:
    """Create histogram for each column in polars dataframe.

    Args:
        df: Polars dataframe
        columns: Columns to plot. If None, plots all numeric columns
        bins: Number of histogram bins
        **kwargs: Additional kwargs passed to go.Histogram

    Returns:
        Dict mapping column names to plotly figures
    """
    if columns is None:
        columns = [col for col in df.columns if df[col].dtype.is_numeric()]

    figures = {}
    for col in columns:
        data = df[col].to_numpy()

        fig = go.Figure(data=[go.Histogram(x=data, nbinsx=bins, **kwargs)])
        fig.update_layout(
            title=col,
            xaxis_title="Value",
            yaxis_title="Count",
            showlegend=False,
            template="plotly_white",
        )
        figures[col] = fig

    return figures
