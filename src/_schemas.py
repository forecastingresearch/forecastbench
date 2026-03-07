"""Pandera DataFrame schemas for cross-module data contracts."""

from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series

# pandera >= 0.29 renamed SchemaModel to DataFrameModel
_BaseModel = getattr(pa, "SchemaModel", None) or pa.DataFrameModel


class QuestionFrame(_BaseModel):
    """Output of every source's update().

    The 12-column canonical question schema.
    From: src/helpers/constants.py QUESTION_FILE_COLUMNS.
    """

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


class ResolutionFrame(_BaseModel):
    """Per-question resolution time series.

    Used by all sources except ACLED (which uses AcledResolutionFrame).
    `value` is intentionally untyped: float for markets and data, str or
    int for some wikipedia pages.
    From: src/helpers/constants.py RESOLUTION_FILE_COLUMNS.
    """

    id: Series[str]
    date: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class AcledResolutionFrame(_BaseModel):
    """ACLED-specific: aggregated events by country and date.

    One column per event type plus fatalities. Columns are dynamic so
    strict=False.
    From: src/helpers/acled.py read_dff() output.
    """

    country: Series[str]
    event_date: Series[object]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class MetadataFrame(_BaseModel):
    """Question metadata produced by tag() and validate().

    From: src/helpers/constants.py META_DATA_FILE_COLUMNS.
    """

    source: Series[str]
    id: Series[str]
    category: Series[str]
    valid_question: Series[bool]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = False


class ForecastFrame(_BaseModel):
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


class ExplodedQuestionSetFrame(_BaseModel):
    """The question set after explosion into one row per (question x resolution_date x direction).

    This is the input to resolve_all() and is produced by explode_question_set().
    From: src/resolve_forecasts/main.py lines 218-287 (get_resolutions_for_llm_question_set).
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
