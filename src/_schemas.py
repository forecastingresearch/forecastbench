"""Pandera DataFrame schemas for cross-module data contracts.

Schemas with coerce=True are used as the type-casting step when reading data
from disk: read loosely with pd.read_json, then call Schema.validate(df) to
coerce columns to the declared types and catch structural problems early.
"""

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
        coerce = True


class ResolutionFrame(pa.DataFrameModel):
    """Per-question resolution time series.

    `value` is typed as object because it can be float, str, or int
    depending on the source.
    """

    id: Series[str]
    date: Series[str]
    value: Series[object]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = True


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
        coerce = True


class MetadataFrame(pa.DataFrameModel):
    """Question metadata produced by tag() and validate()."""

    source: Series[str]
    id: Series[str]
    category: Series[str]
    valid_question: Series[bool]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = True


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
        coerce = True


class ExplodedQuestionSetFrame(pa.DataFrameModel):
    """One row per (question x resolution_date x combo_direction).

    Produced by explode_question_set(), consumed by resolve_all().
    """

    id: Series[object]  # str or tuple for combo
    source: Series[str]
    direction: Series[object]  # tuple
    forecast_due_date: Series[object]
    resolution_date: Series[str]

    class Config:
        """Schema configuration."""

        strict = False
        coerce = True
