from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from typing import Dict
import plotly.graph_objects as go


def dropdown_plot(
    figures: Dict[str, go.Figure],
    app_name: str = "Feature Viewer",
) -> Dash:
    """Create interactive Dash app with dropdown to view multiple figures.

    Args:
        figures: Dict mapping names to plotly figures
        app_name: Title for the app

    Returns:
        Dash app instance
    """
    app = Dash(__name__)

    names = list(figures.keys())

    app.layout = html.Div(
        [
            html.H1(app_name, style={"textAlign": "center"}),
            dcc.Dropdown(
                id="figure-selector",
                options=[{"label": name, "value": name} for name in names],
                value=names[0] if names else None,
                clearable=False,
            ),
            dcc.Graph(id="figure-display"),
        ]
    )

    @app.callback(Output("figure-display", "figure"), Input("figure-selector", "value"))
    def update_figure(selected_name):
        return figures.get(selected_name, go.Figure())

    return app
