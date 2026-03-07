"""DBnomics question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._base import DataSource


class DbnomicsSource(DataSource):
    """DBnomics data source — inherits standard data resolution."""

    name: ClassVar[str] = "dbnomics"
    display_name: ClassVar[str] = "DBnomics"
    source_type: ClassVar[SourceType] = SourceType.DATA
    source_intro: ClassVar[str] = (
        "DBnomics collects data on topics such as population and living conditions, environment "
        "and energy, agriculture, finance, trade and others from publicly available resources, "
        "for example national and international statistical institutions, researchers and private "
        "companies. You're going to predict how questions based on this data will resolve."
    )
    resolution_criteria: ClassVar[str] = (
        "Resolves to the value found at {url} once the data is published."
    )
