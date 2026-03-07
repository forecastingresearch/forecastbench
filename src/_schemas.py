"""Pandera DataFrame schemas for cross-module data contracts."""

from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class QuestionFrame(pa.DataFrameModel):
    """Output of every source's update()."""

    id: Series[str]
    question: Series[str]
    background: Series[str]
    url: Series[str]
    resolved: Series[bool]
    forecast_horizons: Series[object]
    freeze_datetime_value: Series[str]
    freeze_datetime_value_explanation: Series[str]
    market_info_resolution_criteria: Series[str]
    market_info_open_datetime: Series[str]
    market_info_close_datetime: Series[str]
    market_info_resolution_datetime: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class ResolutionFrame(pa.DataFrameModel):
    """Per-question resolution time series.

    `value` is intentionally untyped: float for markets and data, str or
    int for some wikipedia pages.
    """

    id: Series[str]
    date: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class AcledResolutionFrame(pa.DataFrameModel):
    """ACLED-specific: aggregated events by country and date.

    One column per event type plus fatalities. Columns are dynamic so
    strict=False.
    """

    country: Series[str]
    event_date: Series[object]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class MetadataFrame(pa.DataFrameModel):
    """Question metadata produced by tag() and validate()."""

    source: Series[str]
    id: Series[str]
    category: Series[str]
    valid_question: Series[bool]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class ForecastFrame(pa.DataFrameModel):
    """What forecasters submit (and what naive/dummy forecasters produce)."""

    id: Series[object]  # str or tuple for combo
    source: Series[str]
    forecast: Series[float]
    resolution_date: Series[str]
    reasoning: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class ExplodedQuestionSetFrame(pa.DataFrameModel):
    """The question set after explosion into one row per (question × resolution_date × combo_direction).

    This is the input to resolve_all() and is produced by explode_question_set() in resolve/prepare.py.
    """

    id: Series[object]  # str or tuple for combo
    source: Series[str]
    direction: Series[object]  # tuple
    forecast_due_date: Series[object]
    resolution_date: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False
