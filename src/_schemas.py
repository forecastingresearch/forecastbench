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
    freeze_datetime_value: Series[str] = pa.Field(nullable=True)
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
    value: Series[object] = pa.Field(nullable=True)

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
