"""Wikipedia constants."""

import hashlib
import json
import logging
import os
import sys
from enum import Enum

import numpy as np
import pandas as pd
from tqdm import tqdm

from . import constants, env

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"

fetch_directory = f"{source}/fetch"

hash_mapping = {}

hash_filename = "hash_mapping.json"
local_hash_filename = f"/tmp/{hash_filename}"

SOURCE_INTRO = (
    "Wikipedia is an online encyclopedia created and edited by volunteers. You're going to predict "
    "how questions based on data sourced from Wikipedia will resolve."
)

RESOLUTION_CRITERIA = "Resolves to the value calculated from {url} on the resolution date."


def populate_hash_mapping():
    """Download the hash_mapping from storage and load into global."""
    global hash_mapping
    remote_filename = f"{source}/{hash_filename}"
    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=remote_filename,
        local_filename=local_hash_filename,
    )
    if os.path.getsize(local_hash_filename) > 0:
        with open(local_hash_filename, "r") as file:
            hash_mapping = json.load(file)


def upload_hash_mapping():
    """Write and upload the hash_mapping to storage from global."""
    with open(local_hash_filename, "w") as file:
        json.dump(hash_mapping, file, indent=4)

    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_hash_filename,
        destination_folder=source,
    )


def make_resolution_df():
    """Prepare data for resolution."""
    files = gcp.storage.list_with_prefix(bucket_name=env.QUESTION_BANK_BUCKET, prefix=source)
    files = [f for f in files if f.endswith(".jsonl")]
    df = pd.DataFrame()
    for f in tqdm(files, f"downloading `{source}` resoultion files"):
        if f.startswith(f"{source}/"):
            df_tmp = pd.read_json(
                f"gs://{env.QUESTION_BANK_BUCKET}/{f}",
                lines=True,
                dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                convert_dates=False,
            )
            # The Wikipedia folder contains files that aren't resolution files.
            # That should be changed at some point. For now, test that it's a resolution file
            # by checking the columns
            if set(df_tmp.columns) == set(constants.RESOLUTION_FILE_COLUMNS):
                df = pd.concat([df, df_tmp], ignore_index=True)

    df["date"] = pd.to_datetime(df["date"])
    df["id"] = df["id"].astype(str)
    return df


def get_fetch_filename(question_id_root: str) -> str:
    """Provide the name of the fetch file for the id_root."""
    return f"{question_id_root}.jsonl"


def id_hash(id_root: str, id_field_value: str) -> str:
    """Encode wikipedia Ids."""
    global hash_mapping
    d = {
        "id_root": id_root,
        "id_field_value": id_field_value,
    }
    dictionary_json = json.dumps(d, sort_keys=True)
    hash_key = hashlib.sha256(dictionary_json.encode()).hexdigest()
    hash_mapping[hash_key] = d
    return hash_key


def id_unhash(hash_key: str) -> tuple:
    """Decode wikipedia Ids."""
    return hash_mapping[hash_key] if hash_key in hash_mapping else None


class QuestionType(Enum):
    """Types of questions.

    These will determine how a given question is resolved.

    When adding a new one, be sure to update `compare_values()`.
    """

    SAME = 0
    SAME_OR_MORE = 1
    MORE = 2
    ONE_PERCENT_MORE = 3
    SAME_OR_LESS = 4


def compare_values(question_type, resolution_date_value, forecast_due_date_value):
    """Compare values given the QuestionType."""
    if question_type == QuestionType.SAME:
        return resolution_date_value == forecast_due_date_value
    elif question_type == QuestionType.SAME_OR_MORE:
        return resolution_date_value >= forecast_due_date_value
    elif question_type == QuestionType.SAME_OR_LESS:
        return resolution_date_value <= forecast_due_date_value
    elif question_type == QuestionType.MORE:
        return resolution_date_value > forecast_due_date_value
    elif question_type == QuestionType.ONE_PERCENT_MORE:
        return resolution_date_value >= forecast_due_date_value * 1.01
    else:
        raise ValueError("Invalid QuestionType")


def clean_List_of_world_records_in_swimming(df):
    """Clean fetched data for `List_of_world_records_in_swimming`.

    Drop any rows that contain parens.
    """
    return df[~df["Name"].str.contains(r"[()]")].reset_index(drop=True)


def clean_List_of_infectious_diseases(df):
    """Clean fetched data for `List_of_infectious_diseases`.

    * Remove rows with multiple answers.
    * Change all `Under research[x]` to `No`
    * Change all `No` to 0
    * Change all `Yes` to 1
    """
    duplicates = df[df.duplicated(subset=["date", "Common name"], keep=False)]
    df = df.drop(duplicates.index).reset_index(drop=True)
    df["Vaccine(s)"] = df["Vaccine(s)"].replace(
        {r"Under research.*": "No", r"Yes.*": "Yes", r"No.*": "No"}, regex=True
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


def resolve(mid, dfr, forecast_due_date, resolution_date):
    """Resolve Wikipedia forecast questions."""
    d = id_unhash(mid)
    if d is None:
        logger.warn(f"Wikipedia: could NOT unhash {mid}")
        return np.nan

    def get_value(dfr, mid, date):
        value = dfr[(dfr["id"] == mid) & (dfr["date"].dt.date == date)]["value"]
        return value.iloc[0] if not value.empty else None

    forecast_due_date_value = get_value(dfr, mid, forecast_due_date)
    resolution_date_value = get_value(dfr, mid, resolution_date)

    if forecast_due_date_value is None:
        logger.info(
            f"Nullifying Wikipedia market {mid}. "
            "The forecast question resolved between the freeze date and the forecast due date."
        )
        return np.nan

    question_type = [q["question_type"] for q in PAGES if q["id_root"] == d["id_root"]]
    if len(question_type) != 1:
        logger.error(
            f"Nullifying Wikipedia market {mid}. Couldn't find comparison type "
            "(should not arrive here)."
        )
        return np.nan

    return compare_values(
        question_type=question_type[0],
        resolution_date_value=resolution_date_value,
        forecast_due_date_value=forecast_due_date_value,
    )


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
        "table_index": [1, 3],
        "question_type": QuestionType.ONE_PERCENT_MORE,
        "fields": {
            "id": "Player",
            "value": "Rating",
        },
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
    },
    {
        "id_root": "FIDE_rankings_ranking",
        "page_title": "FIDE_rankings",
        "table_index": [1, 3],
        "question_type": QuestionType.SAME_OR_LESS,
        "fields": {
            "id": "Player",
            "value": "Rank",
        },
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
    },
    {
        "id_root": "List_of_world_records_in_swimming",
        "page_title": "List_of_world_records_in_swimming",
        "table_index": [0, 2],
        "question_type": QuestionType.SAME,
        "fields": {
            "id": "Name",
            "value": "Event",
        },
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
        "table_index": 0,
        "question_type": QuestionType.MORE,
        "fields": {
            "id": "Common name",
            "value": "Vaccine(s)",
        },
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
