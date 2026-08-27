"""Cloud Run job: push everything the nightly run publishes to git in a single commit.

See `orchestration._io.push_datasets_to_git`.
"""

import logging
from typing import Any

from helpers import decorator
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Push resolution sets, leaderboards, parity dates & question fixed effects to git."""
    _io.push_datasets_to_git()


if __name__ == "__main__":
    driver(None)
