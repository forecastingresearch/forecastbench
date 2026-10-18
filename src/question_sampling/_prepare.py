"""Keep a source's eligible questions and add the columns the question set needs."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from helpers import constants, yfinance
from sources import MARKET_SOURCE_NAMES

if TYPE_CHECKING:
    from sources._base import BaseSource


# Market questions resolving more than this many days after the forecast due date are not sampled.
MAX_DAYS_TO_MARKET_RESOLUTION = 365

# Market questions resolving within this many days after the forecast due date are not sampled.
MIN_DAYS_TO_MARKET_RESOLUTION = 7


def is_market_source(source: str) -> bool:
    """Tell whether `source` is a market source (otherwise it is a dataset source).

    Args:
        source (str): Source name
    """
    return source in MARKET_SOURCE_NAMES


def get_resolution_criteria(dfq: pd.DataFrame, source_template: str) -> pd.Series:
    """Use source-wide criteria, or require custom criteria when the template is empty."""
    if source_template:
        return dfq["url"].apply(lambda url: source_template.format(url=url))
    # SerpAPI uses different resolution criteria for each API source, so its source-wide
    # template is empty and we preserve the criteria supplied for each question instead.
    criteria = dfq.get("resolution_criteria")
    if (
        criteria is None
        or not criteria.apply(
            lambda value: isinstance(value, str) and value.strip() not in {"", "N/A"}
        ).all()
    ):
        raise ValueError(
            "Missing per-question resolution_criteria; update the question bank first."
        )
    return criteria


def drop_invalid_questions(dfq: pd.DataFrame, dfmeta: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid questions from dfq.

    Args:
        dfq (pd.DataFrame): Questions to filter
        dfmeta (pd.DataFrame): Metadata with valid_question column

    Returns
        dfq (pd.DataFrame): Only questions marked valid in metadata
    """
    if dfmeta.empty:
        return dfq
    dfq = pd.merge(
        dfq,
        dfmeta,
        how="inner",
        on=["id", "source"],
    )
    return dfq[dfq["valid_question"]].drop(columns="valid_question")


def drop_questions_not_in_english(dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop questions whose metadata does not mark them as English.

    A question with no language classification yet is dropped too, the same way an
    unvalidated question is dropped by `drop_invalid_questions`.

    Args:
        dfq (pd.DataFrame): Questions merged with metadata, with an `english` column

    Returns
        dfq (pd.DataFrame): Only questions marked English in metadata
    """
    return dfq[dfq["english"]].drop(columns="english")


def drop_nullified_questions(dfq: pd.DataFrame, source: "BaseSource") -> pd.DataFrame:
    """Drop questions that the source has nullified.

    Nullified questions are never sampled into question sets, see `NullifiedQuestion`.

    Args:
        dfq (pd.DataFrame): Questions to filter
        source (BaseSource): The source of the questions

    Returns
        dfq (pd.DataFrame): Questions that are not nullified
    """
    nullified_ids = source.get_nullified_ids()
    if not nullified_ids:
        return dfq
    return dfq[~dfq["id"].isin(nullified_ids)]


def drop_missing_freeze_datetime(dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop questions with missing values in the freeze_datetime_value column.

    Args:
        dfq (pd.DataFrame): Questions to filter

    Returns
        dfq (pd.DataFrame): Questions with valid freeze_datetime_value
    """
    col = "freeze_datetime_value"
    dfq = dfq.dropna(subset=col, ignore_index=True)
    dfq = dfq[dfq[col] != "N/A"]
    dfq = dfq[dfq[col] != "nan"]
    return dfq


def keep_current_forecast_horizons(dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop forecast horizons no longer in `constants.FORECAST_HORIZONS_IN_DAYS`.

    Sources that only refresh some fields of existing questions leave stale horizon lists in the
    question bank, so the constant is enforced here rather than trusted from storage.

    Args:
        dfq (pd.DataFrame): Dataset questions with a `forecast_horizons` column

    Returns
        dfq (pd.DataFrame): Copy with `forecast_horizons` restricted to current horizons
    """
    dfq = dfq.copy()
    dfq["forecast_horizons"] = dfq["forecast_horizons"].apply(
        lambda horizons: (
            [h for h in horizons if h in constants.FORECAST_HORIZONS_IN_DAYS]
            if isinstance(horizons, list)
            else horizons
        )
    )
    return dfq


def drop_questions_that_resolve_too_soon(
    source: str, dfq: pd.DataFrame, forecast_datetime: datetime
) -> pd.DataFrame:
    """Drop questions that resolve too soon.

    Given the freeze date:
    * for market questions determine whether or not the market will resolve within
      `MIN_DAYS_TO_MARKET_RESOLUTION` of the forecast due date. A market resolves too soon if its
      close datetime or its resolution datetime is at or before the cutoff. If it does, then do not
      use this question.
    * for data questions if forecast_horizons is empty, don't use the question

    Args:
        source (str): Source name
        dfq (pd.DataFrame): Questions to filter
        forecast_datetime (datetime): The forecast due date

    Returns
        dfq (pd.DataFrame): Questions that don't resolve too soon
    """
    if not is_market_source(source):
        empty_horizons = dfq["forecast_horizons"].apply(lambda x: len(x) == 0)
        is_na = dfq["forecast_horizons"] == "N/A"
        return dfq[~(empty_horizons | is_na)]

    forecast_due_date = forecast_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
    cutoff_date = forecast_due_date + timedelta(days=MIN_DAYS_TO_MARKET_RESOLUTION)
    resolves_too_soon = dfq.apply(
        lambda row: any(dt <= cutoff_date for dt in market_datetimes(row)),
        axis=1,
        result_type="reduce",
    )
    return dfq[~resolves_too_soon]


def drop_culled_questions(source: str, dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop question families we no longer want in the sampling pool.

    HACK: matches on id prefix / question text instead of a proper per-question flag.
    * dbnomics: all Météo-France weather questions. Ids start with ``meteofrance_`` because the
      question file stores ids with ``/`` replaced by ``_``.
    * acled: all "ten times as many" (x10) questions
    * acled: all questions whose freeze value, the 30-day average over the past 360 days, is zero
    * acled: all questions under a country spelling ACLED has since replaced (the bank keeps both
      so published questions resolve)
    * yfinance: all single-ticker questions ("will X go up"). Retired in favor of the pair
      questions (id ``X_Y``); the rows stay in the bank so published sets keep resolving.
    * metaculus: the ids in ``metaculus_culled_ids.txt``, questions that resolve more than 50 days
      after they close and were ingested before fetch started skipping them
    * kalshi: the ids in ``kalshi_culled_ids.txt``, markets that settle before the forecast due
      date although the API reports a later close (earnings calls, first hurricane of the season)
    * polymarket: the ids in ``polymarket_culled_ids.txt``, markets that are likely to settle
      before the forecast due date although the API reports a later close (touch markets on
      prices and front lines, "by date" events already in motion)
    * fred: the ids in ``fred_culled_ids.txt``, series retired from sampling; the rows stay in
      the bank so published sets keep resolving

    Args:
        source (str): Source name
        dfq (pd.DataFrame): Questions to filter

    Returns
        dfq (pd.DataFrame): Questions not in a culled family
    """
    if source == "dbnomics":
        return dfq[~dfq["id"].str.startswith("meteofrance_")]
    if source == "acled":
        # A zero 30-day average over the past 360 days turns "more than the average" into "will any
        # event happen at all", which almost always resolves No.
        # ACLED has served a country under more than one spelling. The question bank keeps
        # questions under each spelling so published ones resolve; only the current one is sampled.
        retired_country_spellings = ["Akrotiri and Dekhelia"]
        is_x10 = dfq["question"].str.contains("more than ten times as many", regex=False)
        is_zero_baseline = dfq["freeze_datetime_value"].astype(float) == 0
        is_retired_spelling = dfq["question"].str.contains(
            "|".join(f" in {spelling} for the 30 days" for spelling in retired_country_spellings)
        )
        return dfq[~(is_x10 | is_zero_baseline | is_retired_spelling)]
    if source == "yfinance":
        return dfq[dfq["id"].apply(yfinance.is_pair_id)]
    if source in ("metaculus", "kalshi", "polymarket", "fred"):
        # Metaculus ids were ingested before the close-to-resolve gap rule existed. See
        # metaculus-culled-questions.txt at the repository root for the urls and titles.
        culled_ids = (Path(__file__).parent / f"{source}_culled_ids.txt").read_text().split()
        return dfq[~dfq["id"].isin(culled_ids)]
    return dfq


def market_datetimes(row: pd.Series) -> list[datetime]:
    """Return the known close and resolution datetimes of a market question.

    The question bank stores "N/A" for a datetime it does not know yet, e.g. the resolution
    datetime of a market that has not resolved.

    Args:
        row (pd.Series): One market question

    Returns
        datetimes (list[datetime]): The close and resolution datetimes that are not "N/A"
    """
    return [
        datetime.fromisoformat(value)
        for value in (row["market_info_close_datetime"], row["market_info_resolution_datetime"])
        if value != "N/A"
    ]


def drop_questions_that_resolve_too_late(
    source: str, dfq: pd.DataFrame, forecast_datetime: datetime
) -> pd.DataFrame:
    """Drop market questions that resolve more than `MAX_DAYS_TO_MARKET_RESOLUTION` after the due date.

    A market resolves too late if its close datetime or its resolution datetime is after the cutoff.
    Data questions resolve at the fixed forecast horizons and are returned unchanged.

    Args:
        source (str): Source name
        dfq (pd.DataFrame): Questions to filter
        forecast_datetime (datetime): The forecast due date

    Returns
        dfq (pd.DataFrame): Questions that resolve within the cap
    """
    if not is_market_source(source):
        return dfq
    cutoff_date = forecast_datetime + timedelta(days=MAX_DAYS_TO_MARKET_RESOLUTION)
    resolves_in_time = dfq.apply(
        lambda row: all(dt <= cutoff_date for dt in market_datetimes(row)),
        axis=1,
        result_type="reduce",
    )
    return dfq[resolves_in_time]


def prepare_source_questions(
    source: "BaseSource",
    dfq: pd.DataFrame,
    dfmeta: pd.DataFrame,
    freeze_datetime: datetime,
    forecast_datetime: datetime,
) -> pd.DataFrame:
    """Filter one source's question bank down to the questions eligible for sampling.

    Adds the `source`, `source_intro`, `resolution_criteria` and `freeze_datetime` columns the
    question set needs and drops the columns it does not.

    Args:
        source (BaseSource): The source of the questions
        dfq (pd.DataFrame): The source's question bank (`QuestionFrame`)
        dfmeta (pd.DataFrame): Question metadata with `category`, `valid_question` and `english`
            columns
        freeze_datetime (datetime): The datetime the question bank values were frozen at
        forecast_datetime (datetime): The forecast due date

    Returns
        dfq (pd.DataFrame): Questions eligible for sampling
    """
    dfq = dfq.copy()
    dfq["source"] = source.name
    dfq = drop_invalid_questions(dfq=dfq, dfmeta=dfmeta)
    dfq = drop_questions_not_in_english(dfq)
    dfq = drop_missing_freeze_datetime(dfq)
    dfq = dfq[dfq["category"] != "Other"]
    dfq = dfq[~dfq["resolved"]]
    if not is_market_source(source.name):
        dfq = keep_current_forecast_horizons(dfq)
    dfq = drop_questions_that_resolve_too_soon(
        source=source.name, dfq=dfq, forecast_datetime=forecast_datetime
    )
    dfq = drop_questions_that_resolve_too_late(
        source=source.name, dfq=dfq, forecast_datetime=forecast_datetime
    )
    dfq = drop_culled_questions(source=source.name, dfq=dfq)
    dfq = drop_nullified_questions(dfq=dfq, source=source)
    dfq["source_intro"] = source.source_intro
    dfq["resolution_criteria"] = get_resolution_criteria(dfq, source.resolution_criteria)
    dfq["freeze_datetime"] = freeze_datetime.isoformat()
    dfq = dfq.drop(columns=["market_info_resolution_datetime", "resolved"])
    return dfq.reset_index(drop=True)
