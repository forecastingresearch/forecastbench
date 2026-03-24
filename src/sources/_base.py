"""Base class for all question sources."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING, ClassVar, Union

import numpy as np
import pandas as pd

from _schemas import ResolutionFrame
from _types import NullifiedQuestion, SourceType

if TYPE_CHECKING:
    from pandera.typing import DataFrame

    from _schemas import QuestionFrame, ResolveReadyFrame

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """Abstract base for every question source.

    Identity ClassVars are set on concrete subclasses.
    """

    name: ClassVar[str]
    display_name: ClassVar[str]
    source_type: ClassVar[SourceType]
    nullified_questions: ClassVar[list[NullifiedQuestion]] = []
    resolution_schema: ClassVar[type] = ResolutionFrame

    def __init__(self) -> None:
        """Initialize with empty hash mapping."""
        self.hash_mapping: dict[str, dict] = {}

    def __init_subclass__(cls, **kwargs):
        """Enforce required ClassVars on concrete (non-intermediate) subclasses."""
        super().__init_subclass__(**kwargs)
        # Skip enforcement for DatasetSource / MarketSource (they're still abstract)
        if cls.__name__ in ("DatasetSource", "MarketSource"):
            return
        for attr in ("name", "display_name", "source_type"):
            if not hasattr(cls, attr) or getattr(cls, attr) is getattr(BaseSource, attr, None):
                raise TypeError(f"Concrete source {cls.__name__} must define ClassVar '{attr}'")

    # ------------------------------------------------------------------
    # Public resolve interface
    # ------------------------------------------------------------------

    def resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: pd.DataFrame,
        *,
        as_of: date | None = None,
    ) -> DataFrame[ResolveReadyFrame]:
        """Resolve questions for this source.

        Nullified rows are removed before _resolve() so source-specific logic never sees them,
        then added back with resolved_to=NaN afterward.
        """
        nullified_ids = self.get_nullified_ids(as_of=as_of)
        if nullified_ids:
            null_mask = df["id"].apply(self._id_is_nullified, nullified_ids=nullified_ids)
            df_nullified = df[null_mask].copy()
            df = df[~null_mask].copy()
        else:
            df_nullified = None

        if df.empty:
            if df_nullified is not None and not df_nullified.empty:
                df_nullified["resolved_to"] = np.nan
                df_nullified["resolved"] = True
                return df_nullified
            return df

        df = self._resolve(df, dfq, dfr)

        if df_nullified is not None and not df_nullified.empty:
            df_nullified["resolved_to"] = np.nan
            df_nullified["resolved"] = True
            df = pd.concat([df, df_nullified], ignore_index=True)

        return df

    def get_nullified_ids(self, as_of: date | None = None) -> set[str]:
        """Return IDs that are nullified as of the given date."""
        if not self.nullified_questions:
            return set()
        if as_of is None:
            return {nq.id for nq in self.nullified_questions}
        return {nq.id for nq in self.nullified_questions if nq.nullification_start_date <= as_of}

    @staticmethod
    def _id_is_nullified(id_val, nullified_ids: set[str]) -> bool:
        """Check whether a question ID (single or combo tuple) is nullified."""
        if isinstance(id_val, tuple):
            return any(sub_id in nullified_ids for sub_id in id_val)
        return id_val in nullified_ids

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def _resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: pd.DataFrame,
    ) -> DataFrame[ResolveReadyFrame]:
        """Source-specific resolution logic. df contains only this source's non-nullified rows."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _validate_ids(self, df: pd.DataFrame, dfr: pd.DataFrame) -> None:
        """Verify all question IDs in df exist in dfr.

        Called by DatasetSource._resolve() and MarketSource._resolve().
        Sources with custom dfr schemas (e.g. ACLED) simply don't call this.
        """
        unique_ids = set(dfr["id"].unique())

        def check_id(mid):
            if self._is_combo(mid):
                for sub_id in mid:
                    check_id(sub_id)
            elif mid not in unique_ids:
                msg = f"Missing resolution values in dfr for (source: {self.name}, id: {mid})"
                logger.error(msg)
                raise ValueError(msg)

        df["id"].apply(check_id)

    # ------------------------------------------------------------------
    # Static utility methods
    # ------------------------------------------------------------------

    @staticmethod
    def _is_combo(row) -> bool:
        """Tell whether a row or ID represents a combo question (tuple of sub-IDs)."""
        if isinstance(row, pd.Series) and "id" in row.index:
            return isinstance(row["id"], tuple)
        elif isinstance(row, (str, tuple)):
            return isinstance(row, tuple)
        raise ValueError(f"Problem in `_is_combo` with {row}. Type not handled: {type(row)}")

    @staticmethod
    def _combo_change_sign(value: Union[bool, int, float], sign: int):
        """Flip a binary value when sign is -1; pass through when sign is 1."""
        if sign not in (1, -1):
            raise ValueError(f"Wrong value for sign: {sign}")
        return value if sign == 1 else 1 - value

    @staticmethod
    def _get_question(dfq: pd.DataFrame, mid: str):
        """Look up a single question row by ID, or None if not found."""
        dftmp = dfq[dfq["id"] == mid]
        return None if dftmp.empty else dftmp.iloc[0]

    @staticmethod
    def _make_columns_hashable(df: pd.DataFrame) -> pd.DataFrame:
        """Convert list-valued id/direction columns to tuples."""
        for col in ["id", "direction"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
                df[col] = df[col].apply(lambda x: tuple() if pd.isna(x) else x)
        return df

    # ------------------------------------------------------------------
    # Hash mapping IO boundary
    # ------------------------------------------------------------------

    def populate_hash_mapping(self, raw_json: str) -> None:  # noqa: B027
        """Parse hash mapping from raw JSON string. No-op by default."""

    def dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON string. Returns None by default."""
        return None
