# -*- coding: utf-8 -*-
"""Wikipedia shared helpers.

Light home of Wikipedia's naive-forecast computation (scipy/numpy/pandas) plus the hash-mapping
and identity access used by the still-unrefactored ``base_eval`` naive forecaster and by
``question_curation``. Hash-mapping access routes through a lazily-instantiated ``WikipediaSource``
(see ``_get_source``); ``sources.wikipedia`` lazy-imports its scraping deps (requests/bs4) inside
fetch, so importing this module — and the many modules that import it — stays light.

When ``base_eval`` is refactored to call ``WikipediaSource.get_naive_forecast()`` this computation
can move onto the source class (Phase 1 plan) and this module shrinks to a metadata-only shim.
"""

import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from scipy.stats import norm

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from sources._metadata import SOURCE_METADATA  # noqa: E402
from sources.wikipedia import _IDS_TO_NULLIFY as IDS_TO_NULLIFY  # noqa: F401, E402
from sources.wikipedia import _PAGES as PAGES  # noqa: E402
from sources.wikipedia import (  # noqa: F401, E402
    _TRANSFORM_ID_MAPPING as transform_id_mapping,
)
from sources.wikipedia import QuestionType  # noqa: E402

from . import constants  # noqa: E402

SOURCE_INTRO = SOURCE_METADATA["wikipedia"]["source_intro"]
RESOLUTION_CRITERIA = SOURCE_METADATA["wikipedia"]["resolution_criteria"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME = (
    constants.QUESTION_BANK_DATA_STORAGE_START_DATETIME - timedelta(days=360 * 4)
)
WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE = (
    WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME.date()
)

source = "wikipedia"

# Lazy import to avoid circular imports at module level
_source = None


def _get_source():
    global _source
    if _source is None:
        from sources.wikipedia import WikipediaSource

        _source = WikipediaSource()
    return _source


def transform_id(wid):
    """Transform old id to new id."""
    return _get_source()._transform_id(wid)


def populate_hash_mapping():
    """Download and load hash mapping into source singleton."""
    from orchestration._io import load_hash_mapping

    _get_source().populate_hash_mapping(load_hash_mapping(source))


def upload_hash_mapping():
    """Dump and upload hash mapping from source singleton."""
    from orchestration._io import upload_hash_mapping as _upload

    raw_json = _get_source().dump_hash_mapping()
    if raw_json:
        _upload(raw_json, source)


def ffill_dfr(dfr):
    """Forward fill dfr to yesterday."""
    return _get_source()._ffill_dfr(dfr)


def id_hash(id_root: str, id_field_value: str) -> str:
    """Encode wikipedia Ids."""
    return _get_source()._id_hash(id_root=id_root, id_field_value=id_field_value)


def id_unhash(hash_key: str) -> tuple:
    """Decode wikipedia Ids."""
    return _get_source()._id_unhash(hash_key)


def get_probability_forecast(mid, comparison_value, forecast_mean, forecast_std):
    """Get forecast based on question type.

    Used for the naive forecaster.
    """
    question_type = get_question_type(mid)
    if pd.isna(question_type):
        raise ValueError(f"Wikipedia: Should not encounter nan question type: {mid}.")

    if question_type == QuestionType.SAME_OR_MORE or question_type == QuestionType.MORE:
        return 1 - norm.cdf(comparison_value, loc=forecast_mean, scale=forecast_std)
    elif question_type == QuestionType.SAME_OR_LESS:
        return norm.cdf(comparison_value, loc=forecast_mean, scale=forecast_std)
    elif question_type == QuestionType.ONE_PERCENT_MORE:
        return 1 - norm.cdf(comparison_value * 1.01, loc=forecast_mean, scale=forecast_std)
    elif question_type == QuestionType.SAME:
        # For exact equality, use a small epsilon
        # If swimming or infection disease data (which is binary)
        epsilon = (
            0.5
            if get_id_root(mid)
            in [
                "List_of_world_records_in_swimming",
                "List_of_infectious_diseases",
            ]
            else 0.001 * comparison_value
        )
        return norm.cdf(
            comparison_value + epsilon, loc=forecast_mean, scale=forecast_std
        ) - norm.cdf(comparison_value - epsilon, loc=forecast_mean, scale=forecast_std)
    raise ValueError("Invalid QuestionType")


def get_min_max_possible_value(mid):
    """Return the min/max possible values for this question type.

    Used by the naive forecaster.
    """
    d = id_unhash(mid)
    if d is None:
        raise ValueError(f"Could not unhash {mid}.")

    id_root = d["id_root"]
    if id_root == "FIDE_rankings_elo_rating":
        return 0, 2950

    if id_root == "FIDE_rankings_ranking":
        # we only look at the top 20, so putting 1000 as the worst ranking gives enough space for
        # Prophet to move.
        return 1, 1000

    if id_root in [
        "List_of_world_records_in_swimming",
        "List_of_infectious_diseases",
    ]:
        # The min/max values are 0,1 as it's really a dummy variable:
        # * the swimmer has a WR or they don't
        # * the vaccine has either been created or it hasn't
        return 0, 1

    raise ValueError(f"Could not find min/max for {id_root}.")


def get_question_type(mid):
    """Retun the question type given mid."""
    d = id_unhash(mid)
    if d is None:
        logger.warn(f"Wikipedia: could NOT unhash {mid}")
        return np.nan

    question_type = [q["question_type"] for q in PAGES if q["id_root"] == d["id_root"]]
    if len(question_type) != 1:
        logger.error(
            f"Nullifying Wikipedia market {mid}. Couldn't find comparison type "
            "(should not arrive here)."
        )
        return np.nan

    return question_type[0]


def get_id_root(mid):
    """Return the id_root given the mid."""
    d = id_unhash(mid)
    if d is None:
        logger.warn(f"Wikipedia: could NOT unhash {mid}")
        return np.nan
    return d["id_root"]


def backfill_for_forecast(mid, dfr):
    """Backfill dfr provided mid.

    This is only used for the naive forecaster.
    """
    if get_id_root(mid) != "List_of_world_records_in_swimming":
        return dfr

    min_datetime = dfr["date"].min()
    if min_datetime.date() > WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE:
        fill_dates = pd.date_range(
            start=WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
            end=min_datetime - pd.Timedelta(days=1),
            freq="D",
        )
        fill_df = pd.DataFrame(
            {
                "date": fill_dates,
                "value": None,
                "id": dfr["id"].iloc[0],  # Use the same ID as existing data
            }
        )
        dfr = pd.concat([fill_df, dfr]).sort_values("date")

    return dfr
