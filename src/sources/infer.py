"""INFER question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._base import MarketSource


class InferSource(MarketSource):
    """INFER Public prediction market source — inherits standard market resolution."""

    name: ClassVar[str] = "infer"
    display_name: ClassVar[str] = "INFER"
    source_type: ClassVar[SourceType] = SourceType.MARKET
    source_intro: ClassVar[str] = (
        "We would like you to predict the outcome of a prediction market. A prediction market, "
        "in this context, is the aggregate of predictions submitted by users on the website "
        "INFER Public. You're going to predict the probability that the market will resolve "
        "as 'Yes'."
    )
    resolution_criteria: ClassVar[str] = "Resolves to the outcome of the question found at {url}."
