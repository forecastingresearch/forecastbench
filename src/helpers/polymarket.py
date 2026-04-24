"""Polymarket-specific variables."""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sources._metadata import SOURCE_METADATA  # noqa: E402

_META = SOURCE_METADATA["polymarket"]
SOURCE_INTRO = _META["source_intro"]
RESOLUTION_CRITERIA = _META["resolution_criteria"]
NULLIFIED_QUESTION_IDS = {nq.id for nq in _META["nullified_questions"]}
