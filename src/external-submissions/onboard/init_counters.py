"""One-time per-environment setup: create the anonymous-number counter.

Run exactly once when setting up a new environment (dev or prod), before the first
registration. Refuses to overwrite an existing counter, so re-running later is harmless
but will not change anything.

Usage (from this directory, with `variables.mk` loaded):

    python init_counters.py --anon-count 8

`--anon-count` is the highest anonymous team number already issued (0 for a fresh
environment). Previously issued numbers must never be reissued, including any gaps.
"""

import argparse
import sys

from main import COUNTERS_COLLECTION, COUNTERS_DOCUMENT, get_clients


def init_counters(anon_count: int, db=None) -> dict:
    """Create the counter document; refuses to overwrite an existing one.

    Args:
        anon_count (int): Highest anonymous number already issued.
        db: Firestore client (injected in tests).
    """
    if db is None:
        db, _ = get_clients()
    counter_ref = db.collection(COUNTERS_COLLECTION).document(COUNTERS_DOCUMENT)
    if counter_ref.get().exists:
        raise ValueError(
            f"{COUNTERS_COLLECTION}/{COUNTERS_DOCUMENT} already exists; not overwriting."
        )
    counters = {"anon_count": anon_count}
    counter_ref.set(counters)
    return counters


def main() -> None:
    """Parse arguments and create the counter document."""
    parser = argparse.ArgumentParser(description="One-time anonymous-counter setup.")
    parser.add_argument("--anon-count", type=int, required=True)
    args = parser.parse_args()
    try:
        counters = init_counters(anon_count=args.anon_count)
    except ValueError as exception:
        sys.exit(str(exception))
    print(f"Created {COUNTERS_COLLECTION}/{COUNTERS_DOCUMENT}: {counters}")


if __name__ == "__main__":
    main()
