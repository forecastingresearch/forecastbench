"""Polymarket question source."""

from __future__ import annotations

from typing import ClassVar

from _types import NullifiedQuestion, SourceType
from helpers_new.constants import BENCHMARK_START_DATE_DATETIME_DATE

from ._market import MarketSource


class PolymarketSource(MarketSource):
    """Polymarket prediction market source."""

    name: ClassVar[str] = "polymarket"
    display_name: ClassVar[str] = "Polymarket"
    source_type: ClassVar[SourceType] = SourceType.MARKET
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(
            id="0x525820c5314f4143091d05079a8d810ecc07c8d5c8954ec2e6b6e163e40de9cb",
            nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE,
        ),
        NullifiedQuestion(
            id="0x9b46e4d85db0b2cd29acc36b836e1dad6cd2ac4fe495643cca64f7b962b6ab24",
            nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE,
        ),
        NullifiedQuestion(
            id="0x1e4d38c9b9e4aa154e350099216f4d86d94f1277eaa0d22fd33f48c0402155d5",
            nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE,
        ),
        NullifiedQuestion(
            id="0x738a551b7e2680669ea268911b2dc2079d156c350e40dc847d2a00eb0c57cfc2",
            nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE,
        ),
    ]
