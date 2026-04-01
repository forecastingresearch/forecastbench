"""DBnomics question source."""

from __future__ import annotations

from typing import ClassVar

from _fb_types import SourceType

from ._dataset import DatasetSource


class DbnomicsSource(DatasetSource):
    """DBnomics economic data source."""

    name: ClassVar[str] = "dbnomics"
    display_name: ClassVar[str] = "DBnomics"
    source_type: ClassVar[SourceType] = SourceType.DATASET
