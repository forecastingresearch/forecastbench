"""SerpAPI metadata and stable question-ID semantics."""

import re
from decimal import Decimal

from sources import SOURCE_METADATA

SOURCE_INTRO = SOURCE_METADATA["serpapi"]["source_intro"]
RESOLUTION_CRITERIA = SOURCE_METADATA["serpapi"]["resolution_criteria"]


def get_percentage_threshold(question_id: str) -> float | None:
    """Read a growth threshold from a permanent question ID, including retired questions."""
    match = re.search(r"__gte(\d+(?:\.\d+)?)pct$", question_id)
    return float(match[1]) if match else None


def threshold_target(baseline: float, threshold: float) -> Decimal:
    """Return the Decimal target for an inclusive percentage increase."""
    return Decimal(str(baseline)) * (1 + Decimal(str(threshold)) / 100)
