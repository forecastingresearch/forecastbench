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

    def fetch(self, **kwargs):
        """Fetch DBnomics data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched DBnomics data into questions and resolution files."""
        raise NotImplementedError
