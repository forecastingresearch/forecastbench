"""Base class for all question sources."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING, Any, ClassVar, Union

import numpy as np
import pandas as pd

from _fb_types import NullifiedQuestion, SourceType, UpdateResult
from _schemas import ResolutionFrame

from ._metadata import SOURCE_METADATA

if TYPE_CHECKING:
    from pandera.typing import DataFrame

    from _schemas import QuestionFrame, ResolveReadyFrame

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """Abstract base for every question source.

    Identity ClassVars are set on concrete subclasses.
    """

    name: ClassVar[str]
    source_type: ClassVar[SourceType]
    source_intro: ClassVar[str]
    resolution_criteria: ClassVar[str]
    nullified_questions: ClassVar[list[NullifiedQuestion]]
    resolution_schema: ClassVar[type] = ResolutionFrame

    # Questions retired from the sampling pool, see `is_retired()`. Both come from SOURCE_METADATA,
    # which defaults them to empty; the class defaults only serve test stubs without an entry.
    retired_question_ids: ClassVar[frozenset[str]] = frozenset()
    retired_question_types: ClassVar[frozenset[str]] = frozenset()
    # The kinds of question a source mints, when there is more than one to tell apart (e.g. the
    # DBnomics providers). A source that lists any overrides `get_question_types()`, and
    # `retired_question_types` may only name kinds listed here.
    question_types: ClassVar[frozenset[str]] = frozenset()

    # Source-specific metadata keys this source needs to even run, beyond the globally required set.
    # Declared on the concrete subclass (e.g. yfinance -> {"ticker_renames"}) so a missing/typo'd key
    # in SOURCE_METADATA fails loudly at class-definition time rather than late at runtime.
    additional_required_metadata_keys: ClassVar[set[str]] = set()

    # Test-only escape hatch. Every production source must have a SOURCE_METADATA entry; test stubs
    # that exercise BaseSource directly set this True to opt out of that requirement.
    _allow_missing_metadata: ClassVar[bool] = False

    def __init__(self) -> None:
        """Initialize with empty hash mapping."""
        self.hash_mapping: dict[str, dict] = {}
        self.api_key: str | None = None

    def _require_api_key(self) -> str:
        """Return api_key or raise if not set."""
        if not self.api_key:
            raise RuntimeError(
                f"{self.__class__.__name__}.api_key must be set before calling "
                "fetch() or update(). Set it in the orchestration layer."
            )
        return self.api_key

    def __init_subclass__(cls, **kwargs):
        """Enforce required ClassVars, auto-populate from _metadata and check retirement.

        Concrete subclasses must define ``name`` and ``source_type``. After
        enforcement, any keys present in ``SOURCE_METADATA[cls.name]`` are set
        as class attributes automatically (source_intro, resolution_criteria,
        nullified_questions, etc.) so source files only need ``name``. A source may only
        retire question types it declares in ``question_types`` and can classify.
        """
        super().__init_subclass__(**kwargs)
        # Skip enforcement for DatasetSource / MarketSource (they're still abstract)
        if cls.__name__ in ("DatasetSource", "MarketSource"):
            return
        for attr in ("name", "source_type"):
            if not hasattr(cls, attr) or getattr(cls, attr) is getattr(BaseSource, attr, None):
                raise TypeError(f"Concrete source {cls.__name__} must define ClassVar '{attr}'")

        # Auto-populate from metadata
        _REQUIRED_METADATA_KEYS = {
            "source_type",
            "source_intro",
            "resolution_criteria",
            "nullified_questions",
        }
        name = getattr(cls, "name", None)
        if name in SOURCE_METADATA:
            meta = SOURCE_METADATA[name]
            missing = (
                _REQUIRED_METADATA_KEYS | cls.additional_required_metadata_keys
            ) - meta.keys()
            if missing:
                raise TypeError(
                    f"SOURCE_METADATA['{name}'] missing required keys: {sorted(missing)}"
                )
            for key, value in meta.items():
                setattr(cls, key, value)
        elif not cls._allow_missing_metadata:
            raise TypeError(
                f"Concrete source {cls.__name__} (name={name!r}) has no entry in SOURCE_METADATA. "
                "Add one in sources/_metadata.py."
            )

        unknown = cls.retired_question_types - cls.question_types
        if unknown:
            raise TypeError(
                f"{cls.__name__} retires question types it does not declare in "
                f"`question_types`: {sorted(unknown)}"
            )
        if cls.question_types and cls.get_question_types is BaseSource.get_question_types:
            raise TypeError(
                f"{cls.__name__} declares `question_types` but does not override "
                "`get_question_types()`"
            )

    # ------------------------------------------------------------------
    # Public resolve interface
    # ------------------------------------------------------------------

    def resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: pd.DataFrame,
        *,
        forecast_due_date: date,
    ) -> tuple[DataFrame[ResolveReadyFrame], list[str]]:
        """Resolve questions for this source.

        df must contain only rows for this source.
        forecast_due_date is used to determine which questions are nullified.
        Nullified rows are removed before _resolve() so source-specific logic never sees them,
        then added back with resolved_to=NaN afterward.

        Returns:
            (resolved_df, warnings_for_slack)
        """
        if not df.empty:
            foreign = df[df["source"] != self.name]["source"].unique()
            if len(foreign) > 0:
                raise ValueError(
                    f"{self.name}.resolve() received rows for other sources: {list(foreign)}. "
                    f"Only rows for '{self.name}' are allowed."
                )

        nullified_ids = self.get_nullified_ids(as_of=forecast_due_date)
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
                return df_nullified, []
            return df, []

        df, warnings_for_slack = self._resolve(df, dfq, dfr)

        if df_nullified is not None and not df_nullified.empty:
            df_nullified["resolved_to"] = np.nan
            df_nullified["resolved"] = True
            df = pd.concat([df, df_nullified], ignore_index=True)

        return df, warnings_for_slack

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
    # Public sampling interface
    # ------------------------------------------------------------------

    def is_retired(self, dfq: pd.DataFrame) -> pd.Series:
        """Flag the questions of dfq that are retired from the sampling pool.

        Retirement keeps a question out of new question sets and does nothing else: the question
        bank keeps the row and published questions keep resolving, unlike nullification. It is
        decided in code and metadata rather than in the question files (issue #317), so it can be
        reversed and can depend on the data as of the freeze date.

        A question is retired when its id is in `retired_question_ids`, when its type
        (`get_question_types()`) is in `retired_question_types`, or when the source's own
        `_is_retired()` says so.

        Args:
          dfq (pd.DataFrame): This source's questions (`QuestionFrame`)

        Returns:
          retired (pd.Series): Boolean mask aligned with dfq
        """
        retired = dfq["id"].isin(self.retired_question_ids)
        if self.retired_question_types:
            retired |= self.get_question_types(dfq).isin(self.retired_question_types)
        return retired | self._is_retired(dfq)

    def get_question_types(self, dfq: pd.DataFrame) -> pd.Series:
        """Return the type of each question of dfq, one of `question_types`.

        A source that mints several kinds of question overrides this. The default mints one kind
        and types nothing.

        Args:
          dfq (pd.DataFrame): This source's questions (`QuestionFrame`)
        """
        return pd.Series(None, index=dfq.index, dtype=object)

    def _is_retired(self, dfq: pd.DataFrame) -> pd.Series:
        """Retire questions on source-specific grounds, e.g. their current freeze values.

        Called by is_retired() on top of the retired ids and types. The default retires nothing.

        Args:
          dfq (pd.DataFrame): This source's questions (`QuestionFrame`)
        """
        return pd.Series(False, index=dfq.index)

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def _resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: pd.DataFrame,
    ) -> tuple[DataFrame[ResolveReadyFrame], list[str]]:
        """Source-specific resolution logic. df contains only this source's non-nullified rows.

        Returns:
            (resolved_df, warnings_for_slack)
        """

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

    @abstractmethod
    def fetch(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch raw data from external API. Return shape is source-specific."""
        ...

    @abstractmethod
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: pd.DataFrame,
        **kwargs: Any,
    ) -> UpdateResult:
        """Process fetched data into questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (pd.DataFrame): Freshly fetched data (source-specific schema).
        """
        ...

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
