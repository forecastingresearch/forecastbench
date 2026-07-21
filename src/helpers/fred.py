"""FRED constants — thin re-export shim over the lightweight sources._metadata layer.

The canonical FRED question list and nullification data live in
``sources._metadata`` (the predefined series under the ``questions`` key). This
module re-exports them for backwards-compat with existing import sites, without
pulling the heavy ``sources.fred`` module.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sources._metadata import SOURCE_METADATA  # noqa: E402

_META = SOURCE_METADATA["fred"]
SOURCE_INTRO = _META["source_intro"]
RESOLUTION_CRITERIA = _META["resolution_criteria"]
NULLIFIED_IDS = [nq.id for nq in _META["nullified_questions"]]
fred_questions = _META["questions"]
