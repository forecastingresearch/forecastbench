"""DBnomics question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._data import DataSource


class DbnomicsSource(DataSource):
    """DBnomics economic data source."""

    name: ClassVar[str] = "dbnomics"
    display_name: ClassVar[str] = "DBnomics"
    source_type: ClassVar[SourceType] = SourceType.DATA
