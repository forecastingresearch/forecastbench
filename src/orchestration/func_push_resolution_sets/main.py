"""Push all resolution sets to the git dataset repo in a single commit.

This runs as its own Cloud Run job after the (parallel) resolve-forecasts tasks have finished.
Each resolve task uploads its resolution set to the bucket only; this job gathers all of them
and pushes them to git in a single commit, so that only one process ever clones and pushes to
the dataset repository. This removes the race condition that occurred when each parallel task
pushed independently. See `orchestration._io.push_all_resolution_sets`.
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
