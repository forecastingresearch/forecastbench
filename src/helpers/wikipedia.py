# -*- coding: utf-8 -*-
"""Wikipedia constants."""
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy.stats import norm

from sources.wikipedia import _IDS_TO_NULLIFY as IDS_TO_NULLIFY  # noqa: F401
from sources.wikipedia import (  # noqa: F401
    _TRANSFORM_ID_MAPPING as transform_id_mapping,
)
from sources.wikipedia import QuestionType  # noqa: F401

from . import constants

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": f"{constants.BENCHMARK_NAME}Bot/0.0 ({constants.BENCHMARK_URL}; {constants.BENCHMARK_EMAIL})"  # noqa: B950
}

WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME = (
    constants.QUESTION_BANK_DATA_STORAGE_START_DATETIME - timedelta(days=360 * 4)
)
WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE = (
    WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME.date()
)

source = "wikipedia"

fetch_directory = f"{source}/fetch"

# Lazy import to avoid circular imports at module level
_source = None


def _get_source():
    global _source
    if _source is None:
        from sources import SOURCES

        _source = SOURCES[source]
    return _source


SOURCE_INTRO = (
    "Wikipedia is an online encyclopedia created and edited by volunteers. You're going to predict "
    "how questions based on data sourced from Wikipedia will resolve."
)

RESOLUTION_CRITERIA = "Resolves to the value calculated from {url} on the resolution date."


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


def get_fetch_filename(question_id_root: str) -> str:
    """Provide the name of the fetch file for the id_root."""
    return f"{question_id_root}.jsonl"


def id_hash(id_root: str, id_field_value: str) -> str:
    """Encode wikipedia Ids."""
    return _get_source()._id_hash(id_root=id_root, id_field_value=id_field_value)


def id_unhash(hash_key: str) -> tuple:
    """Decode wikipedia Ids."""
    return _get_source()._id_unhash(hash_key)


def clean_FIDE_rankings(df):
    """Clean fetched data for `FIDE_rankings`.

    Fix inconsistent player names.
    """
    df = df[~df["Player"].str.contains("Change from the previous month")].copy()
    replacements = {
        "Gukesh D.": "Gukesh Dommaraju",
        "Gukesh D": "Gukesh Dommaraju",
        "Leinier Dominguez": "Leinier Domínguez Pérez",
        "Leinier Dominguez Pérez": "Leinier Domínguez Pérez",
        "Nana Dzagnidze]": "Nana Dzagnidze",
    }
    df["Player"] = df["Player"].replace(replacements)
    return df


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


def clean_List_of_world_records_in_swimming(df):
    """Clean fetched data for `List_of_world_records_in_swimming`.

    Drop any rows that contain parens.
    """
    df = df[~df["Name"].str.contains(r"[()]")].reset_index(drop=True)
    df = df[~df["Name"].str.contains("eventsort")].reset_index(drop=True)
    df = df[~df["Name"].str.contains("recordinfo")].reset_index(drop=True)
    return df


def clean_List_of_infectious_diseases(df):
    """Clean fetched data for `List_of_infectious_diseases`.

    * Remove rows with multiple answers.
    * Change all `Under research[x]` to `No`
    * Change all `No` to 0
    * Change all `Yes` to 1
    """
    duplicates = df[df.duplicated(subset=["date", "Common name"], keep=False)]
    df = df.drop(duplicates.index).reset_index(drop=True)
    # On and before this date the `"Vaccine(s)"` field had other info in it.
    df = df[df["date"] > pd.Timestamp("2021-07-07")]
    df["Vaccine(s)"] = df["Vaccine(s)"].replace(
        {
            r"Under research.*": "No",
            r"Under Development.*": "No",
            r"Yes.*": "Yes",
            r"No.*": "No",
        },
        regex=True,
    )
    df.loc[df["Vaccine(s)"] == "No", "Vaccine(s)"] = 0
    df.loc[df["Vaccine(s)"] == "Yes", "Vaccine(s)"] = 1
    df["Vaccine(s)"] = df["Vaccine(s)"].astype(int)
    df = df.dropna(ignore_index=True)
    return df


def is_resolved_List_of_infectious_diseases(value):
    """Return true if the vaccine has been developed."""
    return value == 1 or str(value).lower() == "yes"


def get_value_List_of_infectious_diseases(value):
    """Return Yes/No instead of 1/0."""
    return "Yes" if value else "No"


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


FIDE_BACKGROUND = (
    (
        "The International Chess Federation (FIDE) governs international chess "
        "competition. Each month, FIDE publishes the lists 'Top 100 Players', 'Top 100 "
        "Women', 'Top 100 Juniors' and 'Top 100 Girls' and rankings of countries according "
        "to the average rating of their top 10 players and top 10 female players.\n"
        "To create the rankings, FIDE uses the Elo rating system, which is a method for "
        "calculating the relative skill levels of players in zero-sum games such as chess. "
        "The difference in the ratings between two players serves as a predictor of the "
        "outcome of a match. Two players with equal ratings who play against each other "
        "are expected to score an equal number of wins. A player whose rating is 100 "
        "points greater than their opponent's is expected to score 64%; if the difference "
        "is 200 points, then the expected score for the stronger player is 76%.\n"
        "A player's Elo rating is a number which may change depending on the outcome of "
        "rated games played. After every game, the winning player takes points from the "
        "losing one. The difference between the ratings of the winner and loser determines "
        "the total number of points gained or lost after a game. If the higher-rated "
        "player wins, then only a few rating points will be taken from the lower-rated "
        "player. However, if the lower-rated player scores an upset win, many rating "
        "points will be transferred. The lower-rated player will also gain a few points "
        "from the higher rated player in the event of a draw. This means that this rating "
        "system is self-correcting. Players whose ratings are too low or too high should, "
        "in the long run, do better or worse correspondingly than the rating system "
        "predicts and thus gain or lose rating points until the ratings reflect their true "
        "playing strength.\n"
        "Elo ratings are comparative only, and are valid only within the rating pool in "
        "which they were calculated, rather than being an absolute measure of a player's "
        "strength."
    ),
    tuple(),
)

PAGES = [
    {
        "id_root": "FIDE_rankings_elo_rating",
        "page_title": "FIDE_rankings",
        "table_index": [
            {
                "start_date": WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [1, 3],
            },
        ],
        "question_type": QuestionType.ONE_PERCENT_MORE,
        "key": {
            "id",
        },
        "fields": {
            "id": "Player",
            "value": "Rating",
        },
        "resolution_file_value_column_dtype": int,
        "question": (
            (
                "According to Wikipedia, will {id} have an Elo rating on {resolution_date} that's "
                "at least 1% higher than on {forecast_due_date}?"
            ),
            ("id",),
        ),
        "background": FIDE_BACKGROUND,
        "freeze_datetime_value_explanation": (
            "{id}'s ELO rating.",
            ("id",),
        ),
        "clean_func": "clean_FIDE_rankings",
    },
    {
        "id_root": "FIDE_rankings_ranking",
        "page_title": "FIDE_rankings",
        "table_index": [
            {
                "start_date": WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [1, 3],
            },
        ],
        "question_type": QuestionType.SAME_OR_LESS,
        "key": {
            "id",
        },
        "fields": {
            "id": "Player",
            "value": "Rank",
        },
        "resolution_file_value_column_dtype": int,
        "question": (
            (
                "According to Wikipedia, will {id} have a FIDE ranking on {resolution_date} as "
                "high or higher than their ranking on {forecast_due_date}?"
            ),
            ("id",),
        ),
        "background": FIDE_BACKGROUND,
        "freeze_datetime_value_explanation": (
            "{id}'s FIDE ranking.",
            ("id",),
        ),
        "clean_func": "clean_FIDE_rankings",
    },
    {
        "id_root": "List_of_world_records_in_swimming",
        "page_title": "List_of_world_records_in_swimming",
        "table_index": [
            {
                "start_date": WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [0, 2],
            },
            {
                "start_date": datetime(2025, 5, 4).date(),
                "table_index": [0, 1],
            },
        ],
        "question_type": QuestionType.SAME,
        "key": {
            "id",
            "value",
        },
        "fields": {
            "id": "Name",
            "value": "Event",
        },
        "resolution_file_value_column_dtype": str,
        "question": (
            (
                "According to Wikipedia, will {id} still hold the world record for {value} in "
                "long course (50 metres) swimming pools on {resolution_date}?"
            ),
            ("id", "value"),
        ),
        "background": (
            (
                "The world records in swimming are ratified by World Aquatics (formerly known as FINA), "
                "the international governing body of swimming. Records can be set in long course (50 "
                "metres) or short course (25 metres) swimming pools.\n"
                "The ratification process is described in FINA Rule SW12, and involves submission of "
                "paperwork certifying the accuracy of the timing system and the length of the pool, "
                "satisfaction of FINA rules regarding swimwear and a negative doping test by the "
                "swimmer(s) involved. Records can be set at intermediate distances in an individual "
                "race and for the first leg of a relay race. Records which have not yet been fully "
                "ratified are marked with a '#' symbol in these lists."
            ),
            tuple(),
        ),
        "freeze_datetime_value_explanation": (
            "{id} is a record holder in the {value}.",
            (
                "id",
                "value",
            ),
        ),
        "clean_func": "clean_List_of_world_records_in_swimming",
    },
    {
        "id_root": "List_of_infectious_diseases",
        "page_title": "List_of_infectious_diseases",
        "table_index": [
            {
                "start_date": WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": 0,
            },
        ],
        "question_type": QuestionType.MORE,
        "key": {
            "id",
        },
        "fields": {
            "id": "Common name",
            "value": "Vaccine(s)",
        },
        "resolution_file_value_column_dtype": str,
        "question": (
            (
                "According to Wikipedia, will a vaccine have been developed for {id} by "
                "{resolution_date}?"
            ),
            ("id",),
        ),
        "background": (
            (
                "According to Wikipedia, {id} is the common name of an infectious disease. A vaccine "
                "is a biological preparation that provides active acquired immunity to a particular "
                "infectious or malignant disease. The safety and effectiveness of vaccines has "
                "been widely studied and verified. A vaccine typically contains an agent that "
                "resembles a disease-causing microorganism and is often made from weakened or killed "
                "forms of the microbe, its toxins, or one of its surface proteins. The agent "
                "stimulates the body's immune system to recognize the agent as a threat, destroy it, "
                "and recognize further and destroy any of the microorganisms associated with that "
                "agent that it may encounter in the future."
            ),
            ("id",),
        ),
        "freeze_datetime_value_explanation": (
            "Vaccine status for {id}. 'No' means that a vaccine has not yet been created. "
            "'Yes' means that it has.",
            ("id",),
        ),
        "clean_func": "clean_List_of_infectious_diseases",
        "is_resolved_func": "is_resolved_List_of_infectious_diseases",
        "value_func": "get_value_List_of_infectious_diseases",
    },
]

for page in PAGES:
    page["table_index"].sort(key=lambda e: e["start_date"])
