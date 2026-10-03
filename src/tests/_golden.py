"""Golden-file (snapshot) helper for the e2e suite.

``check_golden`` canonicalises a frame (optional column subset, sorted by a unique key, numerics
rounded) and compares it with the committed ``golden/<name>.csv``. ``UPDATE_GOLDEN=1`` rewrites
the goldens, a dev step never run in CI. Freeze only frames produced under ``freeze_today``, a
seeded RNG and a stable sort key.

The CSV round-trip normalises object columns through ``read_csv`` inference (leading-zero string
ids, empty string vs null), so prefer scalar columns; the literal ``"N/A"`` stays a string.
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
        rtol (float): Relative float tolerance. The default suits BLAS/pyfixest values (2FE,
            bootstrap) that vary at the 1e-6 level across platforms; pass ``rtol=0`` for pure
            pandas/numpy frames so small score drift is caught.
        atol (float): Absolute float tolerance, matching the 9-decimal rounding.
    """
    canon = _canonicalize(df, key, cols)
    path = GOLDEN_DIR / f"{name}.csv"

    if _update_requested():
        path.parent.mkdir(parents=True, exist_ok=True)
        canon.to_csv(path, index=False)
        return

    if not path.exists():
        # Write it but fail: a deleted golden must not re-create itself and pass in CI.
        path.parent.mkdir(parents=True, exist_ok=True)
        canon.to_csv(path, index=False)
        raise AssertionError(
            f"Golden {path.name} was missing and has been created — inspect and commit it, then "
            f"re-run. Regenerate deliberately with UPDATE_GOLDEN=1."
        )

    # Round-trip through CSV so both sides share dtype inference; only an empty field reads as
    # missing, so a null that replaces the "N/A" sentinel still fails the comparison.
    buf = io.StringIO()
    canon.to_csv(buf, index=False)
    buf.seek(0)
    actual = pd.read_csv(buf, keep_default_na=False, na_values=[""])
    expected = pd.read_csv(path, keep_default_na=False, na_values=[""])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False, rtol=rtol, atol=atol)
