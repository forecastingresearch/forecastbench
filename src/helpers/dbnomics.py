"""DBnomics constants — thin re-export shim over the lightweight sources._metadata layer."""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sources._metadata import SOURCE_METADATA  # noqa: E402

_META = SOURCE_METADATA["dbnomics"]
SOURCE_INTRO = _META["source_intro"]
RESOLUTION_CRITERIA = _META["resolution_criteria"]
