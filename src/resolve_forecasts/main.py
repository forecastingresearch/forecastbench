"""Resolve forecasting questions."""

import itertools
import json
import logging
import os
import pickle
import sys
from pprint import pprint

import acled
import markets
import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import data_utils, dates, decorator, env, resolution  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

required_forecast_file_keys = [
    "organization",
    "model",
    "question_set",
    "forecast_date",
    "forecasts",
]

QUESTION_SET_FIELDS = [
    "id",
    "source",
    "direction",
    "forecast_submitted_date",
    "market_value_at_submission",
    "forecast_evaluation_date",
    "resolved_to",
    "resolved",
]

TODAY = dates.get_date_today()


def upload_questions_and_resolutions_file(df, forecast_date):
    """Upload resolutions dataset."""
    local_filename = f"/tmp/{forecast_date}_resolutions.jsonl"
    df = df[
        [
            "id",
            "source",
            "direction",
            "forecast_submitted_date",
            "forecast_evaluation_date",
            "resolved_to",
            "resolved",
        ]
    ]
    df["direction"] = df["direction"].apply(lambda x: None if len(x) == 0 else x)
    df["forecast_submitted_date"] = (
        df["forecast_submitted_date"].dt.strftime("%Y-%m-%d").astype(str)
    )
    df["forecast_evaluation_date"] = (
        df["forecast_evaluation_date"].dt.strftime("%Y-%m-%d").astype(str)
    )
    df.to_json(local_filename, orient="records", lines=True)
    gcp.storage.upload(
        bucket_name=env.LEADERBOARD_BUCKET,
        local_filename=local_filename,
        destination_folder="supplementary_materials/datasets/question_and_resolution_sets",
    )


def download_and_read_forecast_file(filename):
    """Download forecast file."""
    local_filename = "/tmp/tmp.json"
    gcp.storage.download(
        bucket_name=env.FORECAST_SETS_BUCKET, filename=filename, local_filename=local_filename
    )
    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data if all(key in data for key in required_forecast_file_keys) else None


def upload_processed_forecast_file(data, forecast_date, filename):
    """Upload processed forecast file."""
    local_filename = "/tmp/tmp.json"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))
    gcp.storage.upload(
        bucket_name=env.PROCESSED_FORECAST_SETS_BUCKET,
        local_filename=local_filename,
        destination_folder=forecast_date,
        filename=filename,
    )


def resolve_questions(df, resolution_values):
    """Resolve all questions.

    Params:
    - df: the questions in the question set
    - resolution_values: all resolutions from data sources
    """
    for source in df["source"].unique():
        source_data = resolution_values.get(source, {})
        dfq = source_data.get("dfq").copy()
        dfr = source_data.get("dfr").copy()
        if source in markets.MARKET_SOURCES:
            df = markets.resolve(source=source, df=df.copy(), dfq=dfq, dfr=dfr)
        elif source == "acled":
            df = acled.resolve(df=df.copy(), dfq=dfq, dfr=dfr)
        else:
            logger.warning(f"*** Not able to resolve {source} ***")

    # Remove all forecasts on dataset questions that have not resolved
    n_pre_drop = len(df)
    df = df[~(~df["source"].isin(markets.MARKET_SOURCES) & ~df["resolved"])]
    logger.info(f"Dropped {n_pre_drop - len(df)} dataset questions that have not yet resolved.")

    # Remove all forecast questions that have resolved to np.nan
    n_pre_drop = len(df)
    df = df[~df["resolved_to"].isna()]
    logger.info(f"Dropped {n_pre_drop - len(df)} questions that have resolved to NaN.")

    return df.reset_index(drop=True)


def get_forecast_horizon_for_combo(combo_rows):
    """Get min forecast horizon for combo questions."""
    fh1 = combo_rows.iloc[0]["forecast_horizons"]
    fh2 = combo_rows.iloc[1]["forecast_horizons"]
    return fh1 if len(fh1) < len(fh2) else fh2


def get_forecast_horizon(row, dfq):
    """Get forecast horizon for all questions."""
    if resolution.is_combo(row):
        matches = dfq[dfq["id"].isin(row["id"])]
        if len(matches) == 2:
            return get_forecast_horizon_for_combo(matches)
        logger.error(f"Problem in get_forecast_horizon {row['id']}")
        return None
    else:
        matches = dfq[dfq["id"] == row["id"]]
        return matches.iloc[0]["forecast_horizons"] if not matches.empty else []


def get_resolutions_for_llm_question_set(forecast_date, resolution_values):
    """
    Given a forecast date, find available resolution values for the associatdd llm question file.

    Params:
    - forecast-date: ISO date as string
    - resolution_values: dictionary of latest resolution values downloaded from storage bucket.
    """
    filename = f"{forecast_date}-llm.jsonl"
    logger.info(f"Getting resolutions for {filename}.")
    df = pd.read_json(
        f"gs://{env.QUESTION_SETS_BUCKET}/{filename}",
        lines=True,
        convert_dates=False,
    )
    df = df[["id", "source", "forecast_horizons"]]
    df = resolution.make_columns_hashable(df)
    n_start = len(df)
    logger.info(f"LM question set starting with {n_start} questions.")

    # DROP COMBO QUESTIONS FOR MARKETS
    df = df[
        ~df.apply(
            lambda x: resolution.is_combo(x) and x["source"] in markets.MARKET_SOURCES,
            axis=1,
        )
    ].reset_index(drop=True)

    # Expand horizons & get resolution_dates
    df["forecast_submitted_date"] = pd.to_datetime(forecast_date)
    df = df.explode("forecast_horizons", ignore_index=True)
    df["forecast_evaluation_date"] = df["forecast_submitted_date"] + pd.to_timedelta(
        df["forecast_horizons"], unit="D"
    )
    df = df[df["forecast_evaluation_date"] < pd.Timestamp(TODAY)]

    # For Market questions that have resolved early, keep only those forecast periods that are
    # < resolution_date and the first period that is >= resolution_date
    for source in markets.MARKET_SOURCES:
        source_data = resolution_values.get(source, {})
        dfq = source_data.get("dfq").copy()
        dfq_resolved = dfq[dfq["resolved"]]
        mask = (df["source"] == source) & df["id"].isin(dfq_resolved["id"])
        df_common_resolved = df[mask]
        for mid in df_common_resolved["id"]:
            resolution_date = dfq_resolved.loc[
                dfq_resolved["id"] == mid, "source_resolution_datetime"
            ].iloc[0]
            resolution_date = pd.to_datetime(resolution_date).date()
            df_tmp = df[(df["id"] == mid) & (df["source"] == source)]
            df_to_drop = df_tmp[
                df_tmp["forecast_evaluation_date"].dt.date >= resolution_date
            ].sort_values(by="forecast_evaluation_date", ascending=True)
            if len(df_to_drop) > 1:
                df_to_drop = df_to_drop.iloc[1:]
                df = df.drop(df_to_drop.index)

    # Expand combo question directions:
    new_rows = []
    indices_to_drop = []
    df["direction"] = df.apply(lambda x: tuple(), axis=1)
    for index, row in df.iterrows():
        if resolution.is_combo(row):
            indices_to_drop += [index]
            for direction in itertools.product((1, -1), repeat=len(row["id"])):
                new_row = row.copy()
                new_row["direction"] = direction
                new_rows.append(new_row)
    df = df.drop(indices_to_drop)
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.sort_values(by=["source", "forecast_evaluation_date"], ignore_index=True)

    # Resolve all questions
    df["resolved"] = False
    df = df.assign(resolved_to=np.nan, market_value_at_submission=np.nan)
    df = resolve_questions(df, resolution_values)
    logger.info(f"LM question set has {len(df)} questions.")
    return df[QUESTION_SET_FIELDS]


def get_resolutions_for_human_question_set(forecast_date, df_llm_resolutions):
    """Extract resolutions for human questions from llm resolutions.

    Assumes human questions are a subset of llm questions.
    """
    df = pd.read_json(
        f"gs://{env.QUESTION_SETS_BUCKET}/{forecast_date}-human.jsonl",
        lines=True,
        convert_dates=False,
    )
    df = resolution.make_columns_hashable(df)
    df = pd.merge(df_llm_resolutions, df, on=["id", "source"]).reset_index(drop=True)

    logger.info(f"Human question set has {len(df)} questions expanded using llm_resolutions.")
    return df[QUESTION_SET_FIELDS]


def impute_missing_forecasts(df):
    """
    Fill in np.nan forecast values with context-appropriate forecasts.

    Forecasters are expeceted to provide forecasts on all questions. If they have omitted
    forecasts, we impute the following values to their forecasts:
    * data questions: 0.5
    * market questions: the market value at forecast_submitted_date (i.e. the naive forecast)
    """
    df["forecast"] = df["forecast"].astype(float)
    n_orig = df["forecast"].isna().sum()
    if n_orig == 0:
        logger.info("No missing values → nothing to impute.")
        return df
    logger.info(f"Imputing {n_orig} missing values.")

    # For data tasks, apply a forecast of 0.5 to missing forecasts
    df.loc[(df["source"] == "acled") & (df["forecast"].isna()), "forecast"] = 0.5

    # For market tasks, apply a forecast of the market value at forecast_submitted_date
    df.loc[(df["source"].isin(markets.MARKET_SOURCES)) & (df["forecast"].isna()), "forecast"] = df[
        "market_value_at_submission"
    ]

    return df


def score_forecasts(df, df_question_resolutions):
    """Score the forecasts in df."""
    logger.info("Scoring forecasts.")

    # Split dataframe into market questions and non-market questions
    df_markets, df_not_markets = (
        df[df["source"].isin(markets.MARKET_SOURCES)].copy(),
        df[~df["source"].isin(markets.MARKET_SOURCES)].copy(),
    )

    # Add `forecast_evaluation_date` to df_markets; drop `horizon`
    df_markets = pd.merge(
        df_question_resolutions,
        df_markets,
        on=[
            "id",
            "source",
            "direction",
            "forecast_submitted_date",
        ],
        suffixes=(None, "_df"),
    )
    df_markets = df_markets.drop(
        columns=[
            "horizon",
            "horizon_df",
            "resolved",
            "resolved_to",
            "market_value_at_submission",
        ]
    ).reset_index(drop=True)

    # Add `forecast_evaluation_date` for non-market questions; join on `horizon` given
    # forecasts are expected for all horizons; drop `horizon`.
    df_not_markets = pd.merge(
        df_question_resolutions,
        df_not_markets,
        on=[
            "id",
            "source",
            "direction",
            "forecast_submitted_date",
            "horizon",
        ],
    )
    df_not_markets = df_not_markets.drop(
        columns=[
            "horizon",
            "resolved",
            "resolved_to",
            "market_value_at_submission",
        ]
    ).reset_index(drop=True)

    df = pd.concat([df_markets, df_not_markets], ignore_index=True)

    # Left merge on question_resolutions s.t. any missing forecasts appear as np.nan
    df = pd.merge(
        df_question_resolutions,
        df,
        on=[
            "id",
            "source",
            "direction",
            "forecast_submitted_date",
            "forecast_evaluation_date",
        ],
        how="left",
    ).reset_index(drop=True)

    df = impute_missing_forecasts(df)

    df["score"] = (df["forecast"] - df["resolved_to"]) ** 2
    return df


def update_leaderboard(leaderboard, organization, model, df):
    """Update leaderboard dict."""
    if organization not in leaderboard:
        leaderboard[organization] = {}
    if model not in leaderboard[organization]:
        leaderboard[organization][model] = {}

    df_resolved = df[df["resolved"]].reset_index(drop=True)
    df_unresolved = df[~df["resolved"]].reset_index(drop=True)
    resolved_score = df_resolved["score"].mean()
    unresolved_score = df_unresolved["score"].mean()
    leaderboard[organization][model] = {
        "resolved": resolved_score,
        "n_resolved": len(df_resolved),
        "unresolved": unresolved_score,
        "n_unresolved": len(df_unresolved),
    }
    return leaderboard


def write_leaderboard_csv(leaderboard):
    """Write the leaderboard dict as a csv. Don't upload."""
    flattened_data = []
    for organization, models in leaderboard.items():
        for model_name, attributes in models.items():
            row = {
                "Organization": organization,
                "Model Name": model_name,
                "Number Resolved": attributes.get("n_resolved", None),
                "Number Unresolved": attributes.get("n_unresolved", None),
                "Resolved": attributes.get("resolved", None),
                "Unresolved": attributes.get("unresolved", None),
            }
            flattened_data.append(row)

    df = pd.DataFrame(flattened_data).sort_values(by=["Resolved", "Unresolved"])
    df.to_csv("/tmp/leaderboard.csv", index=False)


def get_resolution_values_for_forecast_date(
    forecast_date, resolved_values_for_question_sources, resolution_values
):
    """Get resolution values once for every question set."""
    if forecast_date in resolved_values_for_question_sources.keys():
        return resolved_values_for_question_sources

    logger.info(f"Found new question source: {forecast_date}. Downloading .jsonl and resolving...")
    resolved_values_for_question_sources[forecast_date] = {
        "llm": get_resolutions_for_llm_question_set(forecast_date, resolution_values)
    }
    resolved_values_for_question_sources[forecast_date]["human"] = (
        get_resolutions_for_human_question_set(
            forecast_date, resolved_values_for_question_sources[forecast_date]["llm"]
        )
    )

    upload_questions_and_resolutions_file(
        df=resolved_values_for_question_sources[forecast_date]["llm"].copy(),
        forecast_date=forecast_date,
    )
    return resolved_values_for_question_sources


def prepare_forecast_file(df, forecast_date):
    """Prepare the organization's forecast file."""
    df = resolution.make_columns_hashable(df)
    if "reasoning" in df.columns:
        df = df.drop(columns=["reasoning"])
    df["horizon"] = df["horizon"].apply(lambda x: "N/A" if pd.isna(x) else x)

    # DROP COMBO QUESTIONS FOR MARKETS
    df = df[
        ~df.apply(
            lambda x: resolution.is_combo(x) and x["source"] in markets.MARKET_SOURCES,
            axis=1,
        )
    ].reset_index(drop=True)

    df["forecast_submitted_date"] = pd.to_datetime(dates.convert_iso_str_to_date(forecast_date))
    return df


@decorator.log_runtime
def driver(_):
    """Resolve forecasts."""
    if os.path.exists("resolution_values.pkl"):
        with open("resolution_values.pkl", "rb") as handle:
            resolution_values = pickle.load(handle)
    else:
        resolution_values = {
            "acled": {
                "dfr": acled.make_resolution_df(),
                "dfq": data_utils.get_data_from_cloud_storage(
                    source="acled", return_question_data=True
                ),
            }
        }
        for source in markets.MARKET_SOURCES:
            dfr = markets.make_resolution_df(source=source)
            dfq = data_utils.get_data_from_cloud_storage(source=source, return_question_data=True)
            resolution_values[source] = {
                "dfr": dfr,
                "dfq": dfq,
            }
        with open("resolution_values.pkl", "wb") as handle:
            pickle.dump(resolution_values, handle)

    leaderboard = {}
    resolved_values_for_question_sources = {}
    files = gcp.storage.list(env.FORECAST_SETS_BUCKET)
    files = [f for f in files if "/" not in f]
    for f in files:
        logger.info(f"Downloading, reading, and scoring forecasts in `{f}`...")

        data = download_and_read_forecast_file(filename=f)
        if not data or not isinstance(data, dict):
            continue

        organization = data.get("organization")
        model = data.get("model")
        question_set_filename = data.get("question_set")
        forecast_date = data.get("forecast_date")
        forecasts = data.get("forecasts")
        if (
            not organization
            or not model
            or not question_set_filename
            or not forecast_date
            or not forecasts
        ):
            continue

        team_forecast = {
            "organization": organization,
            "model": model,
            "question_set": question_set_filename,
            "forecast_date": forecast_date,
        }

        is_human_question_set = "human" in question_set_filename
        human_llm_key = "human" if is_human_question_set else "llm"

        df = pd.DataFrame(forecasts)
        if df.empty:
            continue

        resolved_values_for_question_sources = get_resolution_values_for_forecast_date(
            forecast_date=forecast_date,
            resolved_values_for_question_sources=resolved_values_for_question_sources,
            resolution_values=resolution_values,
        )
        df_question_resolutions = resolved_values_for_question_sources[forecast_date][
            human_llm_key
        ].copy()

        # Only for joining on horizon; drop when we no longer use horizon and join on resolution
        # dates instead
        df_question_resolutions["horizon"] = (
            df_question_resolutions["forecast_evaluation_date"]
            - df_question_resolutions["forecast_submitted_date"]
        ).dt.days

        df = prepare_forecast_file(df=df, forecast_date=forecast_date)

        df = score_forecasts(df=df, df_question_resolutions=df_question_resolutions)

        leaderboard = update_leaderboard(
            leaderboard=leaderboard, organization=organization, model=model, df=df
        )
        print("\n")
        # Convert to json then load to keep pandas json conversion
        # df.to_dict has different variable conversions and hence is undesireable
        team_forecast["forecasts"] = json.loads(df.to_json(orient="records", date_format="iso"))
        upload_processed_forecast_file(data=team_forecast, forecast_date=forecast_date, filename=f)

    logger.info(leaderboard)
    pprint(leaderboard)
    write_leaderboard_csv(leaderboard)
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
