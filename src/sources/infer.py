"""INFER question source (upstream no longer available)."""

from typing import Any, ClassVar

import pandas as pd
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import QuestionFrame

from ._market import MarketSource


class InferSource(MarketSource):
    """INFER prediction market source.

    The RAND Forecasting Initiative shut down, so the source can no longer be fetched.
    Already-published questions still resolve from their previously-stored resolution files.
    ``update`` is a no-op until INFER questions are resolved by LLM or by hand.
    """

    name: ClassVar[str] = "infer"

    def fetch(self, **kwargs: Any) -> pd.DataFrame:
        """Unavailable. INFER shut down, so there is nothing left to fetch."""
        raise RuntimeError(f"{self.name} can no longer be fetched.")

    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: pd.DataFrame | None = None,
        **kwargs: Any,
    ) -> UpdateResult:
        """No-op. In second pass, either use LLM to resolve or resolve by hand periodically.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions, returned unchanged.
            dff (pd.DataFrame | None): Always None. INFER is no longer fetched.
        """
        return UpdateResult(dfq=dfq, resolution_files={})
