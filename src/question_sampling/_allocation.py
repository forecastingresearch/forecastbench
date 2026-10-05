"""Allocate question counts across sources and categories in proportion to their shares."""

import logging
import math
from copy import deepcopy

import pandas as pd

logger = logging.getLogger(__name__)


def _round_by_largest_remainder(targets: dict[str, float], n: int) -> dict[str, int]:
    """Round float targets to ints that sum to n.

    Every key gets the floor of its target. The leftover units go to the keys with the largest
    fractional parts. Ties go to earlier keys because `sorted` is stable.

    Args:
        targets (dict[str, float]): Keys mapped to float targets that sum to n
        n (int): The total the rounded values must sum to

    Returns
        rounded (dict[str, int]): Keys mapped to ints that sum to n
    """
    rounded = {key: math.floor(value) for key, value in targets.items()}
    leftover = n - sum(rounded.values())
    by_remainder = sorted(targets, key=lambda key: targets[key] - rounded[key], reverse=True)
    for key in by_remainder[:leftover]:
        rounded[key] += 1
    return rounded


def allocate_by_share(data: dict[str, int], shares: dict[str, float], n: int) -> dict:
    """Allocate n items across keys in proportion to shares, respecting availability.

    `data` maps keys to available counts (e.g., {'source1': 30, 'source2': 50}). `shares` maps
    the same keys to their fraction of n. Shares are normalized, so they need not sum to 1.
    Returns allocation dict with same keys where each value <= the original.
    If sum(data.values()) <= n, returns data unchanged.

    Each pass splits the items not yet fixed across the open keys in proportion to their shares
    and rounds with the largest-remainder method. Keys whose rounded target exceeds their
    availability are fixed at that availability and the pass repeats for the remaining keys.
    The last pass rounds the open keys only once, so keys with equal shares differ by at most
    one item. Because sum(data.values()) > n here, at least one key stays open, so every pass
    either fixes a key or ends the loop.

    Args:
        data (dict[str,int]): Keys to allocate across, values are available counts
        shares (dict[str,float]): Keys mapped to their share of n
        n (int): Total number of items to allocate

    Returns
        allocation (dict): Keys mapped to allocated counts
    """

    def validate_allocation(num_allocated: int, n: int) -> None:
        if num_allocated != n:
            raise ValueError(f"Failed to allocate by share: allocated {num_allocated:,}/{n}")
        logger.info(f"Successfully allocated {num_allocated:,}/{n}.")

    sum_n_items = sum(data.values())
    if sum_n_items <= n:
        validate_allocation(sum_n_items, n)
        return data

    fixed = {}
    while True:
        remaining = n - sum(fixed.values())
        open_shares = {key: shares[key] for key in data if key not in fixed}
        total_share = sum(open_shares.values())
        targets = {key: remaining * share / total_share for key, share in open_shares.items()}
        rounded = _round_by_largest_remainder(targets, remaining)
        over = {key: data[key] for key, amount in rounded.items() if amount > data[key]}
        if not over:
            break
        fixed.update(over)

    allocation = {key: fixed[key] if key in fixed else rounded[key] for key in data}
    validate_allocation(sum(allocation.values()), n)
    return allocation


def allocate_across_categories(num_questions: int, dfq: pd.DataFrame) -> dict:
    """Allocate questions evenly among categories.

    Args:
        num_questions (int): Total number of questions to allocate
        dfq (pd.DataFrame): DataFrame with a "category" column

    Returns
        allocation (dict): Category to count mapping
    """
    categories = dfq["category"].unique()
    data = {category: sum(dfq["category"] == category) for category in categories}
    shares = {category: 1 / len(categories) for category in categories}
    return allocate_by_share(data=data, shares=shares, n=num_questions)


def allocate_across_sources(questions: dict, num_questions: int) -> dict:
    """Allocate questions among sources by their sampling share.

    Args:
        questions (dict): Source data keyed by source name
        num_questions (int): Total number of questions to allocate

    Returns
        sources (dict): Deep copy with num_questions_to_sample added per source
    """
    sources = deepcopy(questions)
    data = {key: source["num_questions_available"] for key, source in sources.items()}
    shares = {key: source["sampling_share"] for key, source in sources.items()}

    allocation = allocate_by_share(data=data, shares=shares, n=num_questions)

    for source in sources:
        sources[source]["num_questions_to_sample"] = allocation[source]

    num_allocated = sum(allocation.values())
    if num_allocated != num_questions:
        raise ValueError(
            f"Failed to allocate across sources: allocated {num_allocated:,}/{num_questions:,}"
        )

    logger.info(f"Allocated {num_allocated:,}/{num_questions:,}.")
    return sources
