"""Reusable Streamlit chart helpers."""

from __future__ import annotations

import plotly.graph_objects as go


def add_threshold_lines(
    fig: go.Figure,
    values: list[float],
    *,
    labels: list[str | None] | None = None,
    opacity: float | list[float] = 0.45,
    line_dash: str = "dot",
    colors: list[str | None] | None = None,
) -> go.Figure:
    """Add horizontal reference lines to a Plotly figure.

    TD-99 (F3): extended with per-line `labels`/`opacity` (both optional,
    defaulting to the original no-label/single-opacity behavior) when this
    helper was actually adopted -- the real call sites it was built for
    (regime pressure-level thresholds, correlation strength thresholds) all
    need a distinct annotation per line and, in one case, a distinct
    opacity per line; the original signature couldn't express either.

    TD-118: added per-line `colors` (optional, defaulting to `None` --
    Plotly's own default line color) since opacity alone wasn't a strong
    enough visual distinction between adjacent threshold lines.
    """

    for i, value in enumerate(values):
        label = labels[i] if labels else None
        line_opacity = opacity[i] if isinstance(opacity, list) else opacity
        line_color = colors[i] if colors else None
        fig.add_hline(
            y=value,
            opacity=line_opacity,
            line_dash=line_dash,
            line_color=line_color,
            annotation_text=label,
        )

    return fig
