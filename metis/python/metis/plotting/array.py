import torch
import numpy as np
from typing import Union, List, Dict, Optional
import plotly.graph_objects as go
from einops import parse_shape


def hist_tensor(
    array: Union[torch.Tensor, np.ndarray],
    feature_names: Optional[List[str]] = None,
    bins: int = 50,
    **kwargs,
) -> Dict[str, go.Figure]:
    """Create histogram for each feature in array.

    Args:
        array: Input array with shape matching pattern
        feature_names: Names for each feature. If None, uses feature indices
        pattern: Einops pattern describing array shape (default: "batch feature")
        bins: Number of histogram bins
        **kwargs: Additional kwargs passed to go.Histogram

    Returns:
        Dict mapping feature names to plotly figures
    """
    if isinstance(array, torch.Tensor):
        array = array.detach().cpu().numpy()
    pattern = "batch feature"
    shape_dict = parse_shape(array, pattern)
    n_features = shape_dict["feature"]
    feature_axis = list(shape_dict.keys()).index("feature")
    if feature_names is None:
        feature_names = [f"feature_{i}" for i in range(n_features)]
    if len(feature_names) != n_features:
        raise ValueError(
            f"Expected {n_features} feature names, got {len(feature_names)}"
        )
    figures = {}
    for i, name in enumerate(feature_names):
        data = np.take(array, i, axis=feature_axis).flatten()

        fig = go.Figure(data=[go.Histogram(x=data, nbinsx=bins, **kwargs)])
        fig.update_layout(
            title=name,
            xaxis_title="Value",
            yaxis_title="Count",
            showlegend=False,
            template="plotly_white",
        )
        figures[name] = fig
    return figures
