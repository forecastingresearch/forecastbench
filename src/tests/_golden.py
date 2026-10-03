"""Golden-file (snapshot / approval) regression helper for the e2e suite.

A *golden* freezes the canonical output of a pipeline run. The e2e test computes a result,
canonicalizes it (optional stable column subset, sorted by a key, numerics rounded), and asserts
it equals the committed golden. This is the catch-all net: it flags *any* drift the explicit
semantic asserts didn't anticipate.

The **check** runs as part of ``make test`` (offline, deterministic, CI-gating). Regenerating
("re-blessing") the goldens is a deliberate dev step, **never** CI:

    UPDATE_GOLDEN=1 make test                       # rewrite every golden
    UPDATE_GOLDEN=1 make test ARGS="src/tests/e2e"  # just the e2e goldens

The regenerated CSV shows up as a reviewable diff in the PR — that diff *is* the review artifact
("you changed these leaderboard rows — intended?"). Goldens pin "what is", not "what's correct",
so the e2e keeps its intent-revealing asserts alongside them: a wrong re-bless is still caught by a
failing semantic assertion. Determinism is the whole ballgame — only freeze frames produced under
``freeze_today`` + seeded RNG + a stable sort key, or the goldens flap.

CSV is used on purpose: the PR diff must be human-readable. (Note ``.gitignore`` blanket-ignores
``*.csv``; ``src/tests/golden/**/*.csv`` is un-ignored so goldens are committed.)

Two caveats: (1) ``key`` must uniquely identify rows — this is *enforced*, else a reorder could
hide under the sort; (2) the CSV round-trip normalizes object columns via ``read_csv`` inference
(leading-zero string IDs, empty-string vs null), so it can mask format-only drift there — prefer
scalar columns and freeze ID-like columns deliberately. The literal ``"N/A"`` sentinel is read
back as a string, not as missing, so "N/A" vs null drift *is* caught.
"""

import io
import os
from pathlib import Path
from typing import Sequence

import pandas as pd

GOLDEN_DIR = Path(__file__).parent / "golden"


def _update_requested() -> bool:
    """Whether goldens should be (re)written this run (``UPDATE_GOLDEN=1``)."""
    return os.environ.get("UPDATE_GOLDEN") == "1"


def _canonicalize(df: pd.DataFrame, key: Sequence[str], cols: Sequence[str] | None) -> pd.DataFrame:
    """Return a deterministic view of ``df``: optional column subset, sorted, numerics rounded."""
    frame = df[list(cols)] if cols else df
    sort_key = [key] if isinstance(key, str) else list(key)
    frame = frame.sort_values(sort_key).reset_index(drop=True)
    if frame.duplicated(subset=sort_key).any():
        raise AssertionError(
            f"Golden sort key {sort_key} is not unique — rows could reorder undetectably. "
            f"Pick a key (or column subset) that uniquely identifies each row."
        )
    numeric = frame.select_dtypes("number").columns
    if len(numeric):
        frame[numeric] = frame[numeric].round(9)
    return frame


def check_golden(
    name: str,
    df: pd.DataFrame,
    *,
    key,
    cols: Sequence[str] | None = None,
    rtol: float = 1e-5,
    atol: float = 1e-9,
) -> None:
    """Assert ``df`` matches the committed golden ``<name>.csv`` (or write it under UPDATE_GOLDEN=1).

    Args:
        name (str): Golden file stem; use one file per scenario (e.g. include a param in the name).
        df (pd.DataFrame): The frame to freeze / compare.
        key (str | Sequence[str]): Column(s) to sort by for a stable row order.
        cols (Sequence[str] | None): Restrict to these columns; drop volatile or non-scalar ones
            (e.g. tuple columns that don't round-trip through CSV).
        rtol (float): Relative float tolerance. The default ``1e-5`` (``assert_frame_equal``'s
            default) is deliberately permissive — appropriate for BLAS/pyfixest-backed values
            (2FE/bootstrap) that vary at the ~1e-6 level across platforms. For fully deterministic
            frames (pure pandas/numpy arithmetic), pass ``rtol=0`` so only ``atol`` governs and small
            score drift is actually caught.
        atol (float): Absolute float tolerance (default ``1e-9``, matching the round-to-9-dp canon).
    """
    canon = _canonicalize(df, key, cols)
    path = GOLDEN_DIR / f"{name}.csv"

    if _update_requested():
        path.parent.mkdir(parents=True, exist_ok=True)
        canon.to_csv(path, index=False)
        return

    if not path.exists():
        # Create it for convenience, but FAIL: a missing golden must never silently pass (a deleted
        # golden would otherwise re-create and go green in CI). Inspect, commit, re-run.
        path.parent.mkdir(parents=True, exist_ok=True)
        canon.to_csv(path, index=False)
        raise AssertionError(
            f"Golden {path.name} was missing and has been created — inspect and commit it, then "
            f"re-run. Regenerate deliberately with UPDATE_GOLDEN=1."
        )

    # Round-trip the current frame through CSV so both sides share read_csv's dtype inference
    # (datetimes -> ISO strings, ints stay ints); only values are compared, with a float tolerance.
    # Only an empty field reads back as missing: the project's literal "N/A" sentinel (and "NA",
    # "null", "None", ...) must stay a string, or a regression that emits null where the data
    # contract says "N/A" would be invisible under read_csv's default NA inference.
    buf = io.StringIO()
    canon.to_csv(buf, index=False)
    buf.seek(0)
    actual = pd.read_csv(buf, keep_default_na=False, na_values=[""])
    expected = pd.read_csv(path, keep_default_na=False, na_values=[""])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=rtol, atol=atol)
