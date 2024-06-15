"""Wikipedia constants."""

import hashlib
import json
from enum import Enum

fetch_directory = "wikipedia/fetch"

hash_mapping = {}


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
    """

    SAME = 0
    SAME_OR_MORE = 1
    MORE = 2
    ONE_PERCENT_MORE = 3
    FIVE_PERCENT_MORE = 4


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
                "According to Wikipedia, on the resolution date will {id} have an Elo rating "
                "that's at least 1% higher than it is today?"
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
        "question_type": QuestionType.SAME_OR_MORE,
        "fields": {
            "id": "Player",
            "value": "Rank",
        },
        "question": (
            (
                "According to Wikipedia, will {id} have a FIDE ranking at least as good as their "
                "current ranking on the resolution date?"
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
                "long course (50 metres) swimming pools on the resolution date?"
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
                "According to Wikipedia, will a vaccine have been developed for {id} by the "
                "resolution date?"
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
