"""Cloud Run job: push all resolution sets to the git dataset repo in a single commit.

See `orchestration._io.push_all_resolution_sets`.
"""

import logging
from typing import Any

from helpers import decorator
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Push all resolution sets to git in a single commit."""
    _io.push_all_resolution_sets()


if __name__ == "__main__":
    driver(None)
