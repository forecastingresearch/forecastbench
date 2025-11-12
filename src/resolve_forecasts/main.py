"""Resolve forecasting questions."""

import itertools
import json
import logging
import os
import sys
from datetime import timedelta
from typing import Any

import acled
import data
import markets
import numpy as np
import pandas as pd
import wikipedia
from pandas._libs.tslibs.np_datetime import OutOfBoundsDatetime
from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    dates,
    decorator,
    env,
    git,
    keys,
    resolution,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DELAY_IN_DAYS_BEFORE_FIRST_RESOLOUTION = 14

required_forecast_file_keys = [
    "organization",
    "model",
    "question_set",
    "forecast_due_date",
    "forecasts",
]

valid_forecast_keys = [
    "id",
    "source",
    "direction",
    "forecast",
    "resolution_date",
    "reasoning",
]

QUESTION_SET_FIELDS = [
    "id",
    "source",
    "direction",
    "forecast_due_date",
    "market_value_on_due_date",
    "market_value_on_due_date_minus_one",
    "resolution_date",
    "resolved_to",
    "resolved",
]


def upload_resolution_set(df, forecast_due_date, question_set_filename):
    """Upload resolutions dataset."""
    basename = f"{forecast_due_date}_resolution_set.json"
    local_filename = f"/tmp/{basename}"
    df = df[
        [
            "id",
            "source",
            "direction",
            "resolution_date",
            "resolved_to",
            "resolved",
        ]
    ]
    df["direction"] = df["direction"].apply(lambda x: None if len(x) == 0 else x)
    df["resolution_date"] = df["resolution_date"].dt.strftime("%Y-%m-%d").astype(str)
    json_data = {
        "forecast_due_date": forecast_due_date,
        "question_set": question_set_filename,
        "resolutions": df.to_dict(orient="records"),
    }
    with open(local_filename, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
    logger.info(f"Wrote Resolution File {local_filename}.")

    upload_folder = "datasets/resolution_sets"
    gcp.storage.upload(
        bucket_name=env.PUBLIC_RELEASE_BUCKET,
        local_filename=local_filename,
        destination_folder=upload_folder,
    )
    logger.info(f"Uploaded Resolution File {local_filename} to {upload_folder}.")
    mirrors = keys.get_secret_that_may_not_exist("HUGGING_FACE_REPO_URL")
    mirrors = [mirrors] if mirrors else []
    git.clone_and_push_files(
        repo_url=keys.API_GITHUB_DATASET_REPO_URL,
        files={
            local_filename: f"{upload_folder}/{basename}",
        },
        commit_message=f"resolution set: automatic update for {question_set_filename}.",
        mirrors=mirrors,
    )


def upload_processed_forecast_file(data, forecast_due_date, filename):
    """Upload processed forecast file."""
    local_filename = "/tmp/tmp.json"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))

    gcp.storage.upload(
        bucket_name=env.PROCESSED_FORECAST_SETS_BUCKET,
        local_filename=local_filename,
        filename=filename,
    )


def resolve_questions(df, resolution_values):
    """Resolve all questions.

    Params:
    - df: the questions in the question set
    - resolution_values: all resolutions from data sources
    """
    df = df.assign(
        resolved=False,
        resolved_to=np.nan,
        market_value_on_due_date=np.nan,
    )
    err_msg_pre = "Error in `resolve_questions()`:"
    for source in df["source"].unique():
        logger.info(colored(f"Resolving {source}.", "yellow"))
        source_data = resolution_values.get(source, {})
        dfq = source_data.get("dfq", {}).copy()
        dfr = source_data.get("dfr", {}).copy()
        if isinstance(dfq, dict) or isinstance(dfr, dict):
            msg = (
                f"{err_msg_pre} {source}: "
                f"dfq empty: {isinstance(dfq, dict)}. "
                f"dfr empty: {isinstance(dfr, dict)}."
            )
            logger.error(msg)
            raise ValueError(msg)

        if source in resolution.MARKET_SOURCES:
            df = markets.resolve(source=source, df=df.copy(), dfq=dfq, dfr=dfr)
        elif source in ["dbnomics", "fred", "yfinance"]:
            df = data.resolve(source=source, df=df.copy(), dfq=dfq, dfr=dfr)
        elif source == "acled":
            df = acled.resolve(df=df.copy(), dfq=dfq, dfr=dfr)
        elif source == "wikipedia":
            df = wikipedia.resolve(df=df.copy(), dfq=dfq, dfr=dfr)
        else:
            msg = f"{err_msg_pre} not able to resolve {source}."
            logger.error(msg)
            raise ValueError(msg)

        df_tmp = df[df["source"] == source]
        n_na = len(df_tmp[df_tmp["resolved_to"].isna()])
        n_dates = len(df_tmp["resolution_date"].unique())
        n_combo = int(len(df_tmp[df_tmp["id"].apply(resolution.is_combo)]) / n_dates)
        n_single = int(len(df_tmp[~df_tmp["id"].apply(resolution.is_combo)]) / n_dates)
        logger.info(
            f"* Resolving {source}: #NaN {n_na}/{len(df_tmp)} Total for "
            f"{n_dates} dates, {n_single} single & {n_combo} combo questions."
        )

    # Remove all forecasts on dataset questions that have not resolved
    n_pre_drop = len(df)
    df = df[~(df["source"].isin(resolution.DATA_SOURCES) & (~df["resolved"]))]
    unresolved_dataset_drop_count = n_pre_drop - len(df)
    if unresolved_dataset_drop_count > 0:
        logger.info(
            f"Dropped {unresolved_dataset_drop_count:,} dataset questions that have not yet resolved."
        )

    # Remove all forecast questions that have resolved to np.nan
    n_pre_drop = len(df)
    df = df[~df["resolved_to"].isna()]
    na_drop_count = n_pre_drop - len(df)
    if na_drop_count > 0:
        logger.warning(
            f"! WARNING ! Dropped {na_drop_count:,} questions that have resolved to NaN."
        )

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


def get_resolutions_for_llm_question_set(forecast_due_date, resolution_values):
    """
    Given a forecast date, find available resolution values for the associated llm question file.

    * add resolution dates for market questions
    * explode resolution dates
    * add directions for combo questions
    * get resolution for each entry, calling `resolve_questions()`

    Params:
    - forecast_due_date: ISO date as string
    - resolution_values: dictionary of latest resolution values downloaded from storage bucket.
    """
    filename = f"{forecast_due_date}-llm.json"
    logger.info(f"Getting resolutions for {filename}.")

    df_orig_question_set = resolution.download_and_read_question_set_file(filename)
    df = df_orig_question_set[["id", "source", "resolution_dates"]].copy()
    logger.info(f"LLM question set starting with {len(df):,} questions.")

    df["forecast_due_date"] = pd.to_datetime(forecast_due_date)

    # Assign max resolution dates to all market sources, which will be trimmed in a few lines to
    # only include resolution dates that occurred before a market question was resolved.
    def get_all_resolution_dates(df):
        all_resolution_dates = set()
        for resolution_date in df["resolution_dates"]:
            if resolution_date != "N/A" and isinstance(resolution_date, list):
                all_resolution_dates.update(resolution_date)
        return sorted(all_resolution_dates)

    all_resolution_dates = get_all_resolution_dates(df)

    # Fill resolution dates for market questions as forecasters only provide forecasts on the
    # market outcome and we evaluate those forecasts at every horizion.
    df["resolution_dates"] = df.apply(
        lambda x: (
            all_resolution_dates
            if x["source"] in resolution.MARKET_SOURCES
            else x["resolution_dates"]
        ),
        axis=1,
    )
    df = df.explode("resolution_dates", ignore_index=True)
    df.rename(columns={"resolution_dates": "resolution_date"}, inplace=True)
    df["resolution_date"] = pd.to_datetime(df["resolution_date"]).dt.date
    df = df[df["resolution_date"] < dates.get_date_today()]

    # Expand combo question directions
    df["direction"] = df.apply(
        lambda x: (
            list(itertools.product((1, -1), repeat=len(x["id"])))
            if isinstance(x["id"], tuple)
            else [()]
        ),
        axis=1,
    )
    df = df.explode("direction", ignore_index=True)
    df = df.sort_values(by=["source", "resolution_date"], ignore_index=True)

    # Resolve all questions across horizons and directions in question set
    df["resolution_date"] = pd.to_datetime(df["resolution_date"])
    df = resolve_questions(df, resolution_values)
    print_question_set_breakdown(
        human_or_llm="LLM",
        forecast_due_date=forecast_due_date,
        df=df,
        df_orig_question_set=df_orig_question_set,
    )
    return df[QUESTION_SET_FIELDS]


def get_resolutions_for_human_question_set(forecast_due_date, df_llm_resolutions):
    """Extract resolutions for human questions from llm resolutions.

    Assumes human questions are a subset of llm questions.
    """
    filename = f"{forecast_due_date}-human.json"
    df_orig_question_set = resolution.download_and_read_question_set_file(filename)
    df = pd.merge(df_llm_resolutions, df_orig_question_set, on=["id", "source"]).reset_index(
        drop=True
    )
    df = df[QUESTION_SET_FIELDS]

    print_question_set_breakdown(
        human_or_llm="HUMAN",
        forecast_due_date=forecast_due_date,
        df=df,
        df_orig_question_set=df_orig_question_set,
    )
    return df[QUESTION_SET_FIELDS]


def print_question_set_breakdown(human_or_llm, forecast_due_date, df, df_orig_question_set):
    """Print info about question set, saying how many resolved to N/A.

    This will let us know haw many questions to expect in the leaderboard tables. This is
    approximate as it only does this for the first horizon, and some questions may not start
    resolving until another horizon (e.g. some data questions). Also, some may resolve to NA for the
    first period but not for others, e.g. sometimes weather data may not have been available.
    """
    logger.info("")
    logger.info(f"{human_or_llm} QUESTION SET Breakdown {forecast_due_date}")

    resolution_date = [df["resolution_date"].unique()]
    if len(resolution_date) == 0:
        logger.error("No resolution dates to breakdown.")
        return
    resolution_date = resolution_date[0]

    def get_df_len(df, single, sources):
        combo_mask = df["id"].apply(resolution.is_combo)
        df_tmp = df[~combo_mask] if single else df[combo_mask]
        df_tmp = df_tmp[df_tmp["resolution_date"].isin(resolution_date)]
        df_tmp = df_tmp[df_tmp["source"].isin(sources)]
        return len(df_tmp) if single else int(len(df_tmp) / 4)

    for source_type in ["data", "market"]:
        sources = resolution.DATA_SOURCES if source_type == "data" else resolution.MARKET_SOURCES
        for source in sources:
            if source in df["source"].unique():
                n_single_questions = get_df_len(df=df, single=True, sources=[source])
                n_combo_questions = get_df_len(df=df, single=False, sources=[source])
                n_orig_questions = len(
                    df_orig_question_set[df_orig_question_set["source"] == source]
                )
                combo_info = (
                    "."
                    if n_combo_questions == 0
                    else f", No. Combo Q's {n_combo_questions:,}/{n_orig_questions:,} Orig No. Q's."
                )
                logger.info(
                    f" * {source} No. Single Q's: {n_single_questions:,}/{n_orig_questions:,} "
                    f"Orig No. Q's{combo_info}"
                )
        n_single_questions = get_df_len(df=df, single=True, sources=sources)
        n_combo_questions = get_df_len(df=df, single=False, sources=sources)
        n_questions = n_single_questions + n_combo_questions
        logger.info(f"TOTAL {source_type} questions: {n_questions:,}")

    n_single_questions = get_df_len(
        df=df, single=True, sources=resolution.MARKET_SOURCES + resolution.DATA_SOURCES
    )
    n_combo_questions = get_df_len(
        df=df, single=False, sources=resolution.MARKET_SOURCES + resolution.DATA_SOURCES
    )
    n_questions = n_single_questions + n_combo_questions
    logger.info(f"TOTAL questions: {n_questions:,}")

    df_res_date = df[df["resolution_date"].isin(resolution_date)]
    for _, row in df_orig_question_set.iterrows():
        df_tmp = df_res_date[
            (df_res_date["id"] == row["id"]) & (df_res_date["source"] == row["source"])
        ]
        if df_tmp.empty:
            logger.warning(f" N/A resolution for {row['source']} {row['id']}")


def impute_missing_forecasts(
    df: pd.DataFrame,
    organization: str,
    model_organization: str,
    model: str,
) -> pd.DataFrame:
    """
    Fill in np.nan forecast values with context-appropriate forecasts.

    Forecasters are expeceted to provide forecasts on all questions. If they have omitted
    forecasts, we impute the value 0.5 to their forecast.

    Except:
    - the Imputed Forecaster is assigned the market value on the forecast due date
    - the Naive Forecaster is assigned the market value on the forecast due date - 1 day

    Args:
        df (pd.DataFrame): forecasts dataframe.
        organization (str): the submitting organization.
        model_organization (str): the organization associated with the model
        model (str): the model name.

    Returns:
        pd.DataFrame: with imputed values if any.
    """
    logger.info("Impute missing forecasts.")
    df["forecast"] = df["forecast"].astype(float)
    df["imputed"] = False
    n_orig = df["forecast"].isna().sum()
    if n_orig == 0:
        logger.info("No missing values → nothing to impute.")
        return df
    logger.info(f" Found {n_orig:,} missing values to impute.")

    df.loc[df["forecast"].isna(), "imputed"] = True
    df.loc[df["imputed"], "forecast"] = 0.5
    if organization == constants.BENCHMARK_NAME and model_organization == constants.BENCHMARK_NAME:
        market_imputed_mask = (df["source"].isin(resolution.MARKET_SOURCES)) & df["imputed"]
        if model == "Imputed Forecaster":
            df.loc[market_imputed_mask, "forecast"] = df["market_value_on_due_date"]
        elif model == "Naive Forecaster":
            df.loc[market_imputed_mask, "forecast"] = df["market_value_on_due_date_minus_one"]
    return df


def set_resolution_dates(df, df_question_resolutions):
    """Set resolution dates."""
    logger.info("Setting resolution dates.")

    # Split dataframe into market questions and dataset questions
    df_market_sources = df[df["source"].isin(resolution.MARKET_SOURCES)].copy()
    df_data_sources = df[df["source"].isin(resolution.DATA_SOURCES)].copy()

    # Add `resolution_date` to market questions; since there are no resolution dates for market
    # questions and thus a forecast is valid at all resolution dates, simply drop the column and
    # join on the existing resolution dates in df_question_resolutions.
    df_market_sources = df_market_sources.drop(
        columns=["resolution_date"] if "resolution_date" in df_market_sources.columns else []
    )
    df_market_sources = pd.merge(
        df_question_resolutions[df_question_resolutions["source"].isin(resolution.MARKET_SOURCES)],
        df_market_sources,
        how="left",
        on=[
            "id",
            "source",
            "direction",
            "forecast_due_date",
        ],
    )

    # For data questions, drop forecasts for periods that are not yet resolvable.
    df_data_sources = pd.merge(
        df_question_resolutions[df_question_resolutions["source"].isin(resolution.DATA_SOURCES)],
        df_data_sources,
        how="left",
        on=[
            "id",
            "source",
            "direction",
            "forecast_due_date",
            "resolution_date",
        ],
    )

    df = pd.concat([df_market_sources, df_data_sources], ignore_index=True)
    return df


def get_resolution_values_for_forecast_due_date(
    question_set_filename,
    forecast_due_date,
    resolved_values_for_question_sources,
    resolution_values,
):
    """Get resolution values once for every question set."""
    if forecast_due_date in resolved_values_for_question_sources.keys():
        return resolved_values_for_question_sources

    logger.info(
        f"Found new question source: {forecast_due_date}. Downloading .json and resolving..."
    )
    resolved_values_for_question_sources[forecast_due_date] = {
        "llm": get_resolutions_for_llm_question_set(forecast_due_date, resolution_values)
    }

    resolved_values_for_question_sources[forecast_due_date]["human"] = (
        get_resolutions_for_human_question_set(
            forecast_due_date,
            resolved_values_for_question_sources[forecast_due_date]["llm"],
        )
    )

    upload_resolution_set(
        df=resolved_values_for_question_sources[forecast_due_date]["llm"].copy(),
        forecast_due_date=forecast_due_date,
        question_set_filename=question_set_filename,
    )
    return resolved_values_for_question_sources


def convert_and_bound_dates(date_str):
    """Safely convert dates to datetime, setting max date for dates that are too large."""
    try:
        return pd.to_datetime(date_str)
    except (OutOfBoundsDatetime, OverflowError):
        # Max date pandas can handle
        return pd.Timestamp("2262-04-11")


def check_and_prepare_forecast_file(df, forecast_due_date, organization):
    """Check and prepare the organization's forecast file.

    - Only keep columns needed for resolution
    - Check values are within correct ranges
    - Ensure dates are correct

    Parameters:
    * df (dataframe): organization's forecasts
    * forecast_due_date (string): date as YYYY-MM-DD
    * organization (string): the organization that created the forecasts

    Returns:
    * df (dataframe): Validated and ready for resolution
    """
    df = df.drop(columns=[col for col in df.columns if col not in valid_forecast_keys])
    if "reasoning" in df.columns:
        df = df.drop(columns=["reasoning"])

    # Drop invalid sources
    df_len = len(df)
    df["source"] = df["source"].str.lower()
    df = df[df["source"].isin(resolution.MARKET_SOURCES + resolution.DATA_SOURCES)]
    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len-len(df)} rows because of invalid "
            "data sources."
        )

    # Drop invalid forecasts
    df_len = len(df)
    df = df[~df["forecast"].isna()]
    df = df[(df["forecast"] >= 0) & (df["forecast"] <= 1)]
    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len-len(df)} rows because of invalid "
            "forecasts."
        )

    # Drop invalid resolution dates for dataset questions
    df_len = len(df)
    forecast_due_date_date = dates.convert_iso_str_to_date(forecast_due_date)
    valid_resolution_dates = [
        (forecast_due_date_date + timedelta(days=horizon)).strftime("%Y-%m-%d")
        for horizon in constants.FORECAST_HORIZONS_IN_DAYS
    ]
    df["resolution_date"] = df["resolution_date"].str.slice(0, 10)  # Remove timestamps if present
    df = df[
        df["source"].isin(resolution.MARKET_SOURCES)
        | (
            (df["source"].isin(resolution.DATA_SOURCES))
            & (df["resolution_date"].isin(valid_resolution_dates))
        )
    ]

    df["resolution_date"] = df["resolution_date"].apply(convert_and_bound_dates)

    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len-len(df)} rows because of invalid "
            "dates."
        )

    # Add forecast due date
    df["forecast_due_date"] = pd.to_datetime(forecast_due_date)

    # Make columns hashable
    df = resolution.make_columns_hashable(df)

    # Ensure no duplicate forecasts for dataset questions
    df_data = df[df["source"].isin(resolution.DATA_SOURCES)]
    df_tmp = df_data.drop_duplicates(
        subset=["id", "source", "resolution_date", "direction"], keep="first", ignore_index=True
    )
    if len(df_tmp) != len(df_data):
        dropped_rows = (
            df_data.merge(
                df_tmp,
                on=["id", "source", "resolution_date", "direction"],
                how="left",
                indicator=True,
            )
            .query('_merge == "left_only"')
            .drop("_merge", axis=1)
        )
        print(dropped_rows)
        msg = f"Duplicate Rows encountered in {organization} forecast file."
        logger.error(msg)
        raise ValueError(msg)

    return df


@decorator.log_runtime
def driver(_: Any) -> None:
    """Resolve forecasts.

    Env:
        - CLOUD_RUN_TASK_INDEX: automatically set by Cloud Run Jobs.

    Args:
        _ (Any): Unused placeholder argument for GCP Cloud Run Job.

    Returns:
        None: Exits the process on completion.
    """
    task_num = 0
    try:
        env_var = os.getenv("CLOUD_RUN_TASK_INDEX")
        task_num = int(env_var)
    except Exception as e:
        logger.error(f"Improperly set environment variable: CLOUD_RUN_TASK_INDEX = {env_var}")
        logger.error(e)
        return f"Error: {str(e)}", 400

    forecast_files, valid_dates = resolution.get_valid_forecast_files_and_dates(
        bucket=env.FORECAST_SETS_BUCKET
    )

    # only consider forecasts asked > `DELAY_IN_DAYS_BEFORE_FIRST_RESOLOUTION` days ago
    cutoff_date = dates.get_date_today() - timedelta(days=DELAY_IN_DAYS_BEFORE_FIRST_RESOLOUTION)
    valid_dates = [d for d in valid_dates if dates.convert_iso_str_to_date(d) <= cutoff_date]
    if task_num >= len(valid_dates):
        logger.info(f"task number {task_num} not needed, winding down...")
        return "OK", 200

    my_date = valid_dates[task_num]
    forecast_files = [f for f in forecast_files if f.startswith(my_date)]

    logger.info(f"\nProcessing forecasts for {my_date}.\n")

    resolution_values = resolution.get_resolution_values()

    local_forecast_set_dir = data_utils.get_local_file_dir(bucket=env.FORECAST_SETS_BUCKET)
    resolved_values_for_question_sources = {}
    for f in forecast_files:
        logger.info(f"Resolving {f}")
        data = resolution.read_forecast_file(filename=f"{local_forecast_set_dir}/{f}")
        if data is None:
            continue

        organization = data.get("organization")
        model = data.get("model")
        model_organization = data.get("model_organization")
        question_set_filename = data.get("question_set")
        forecast_due_date = question_set_filename[:10]
        df = data.get("df")
        if "direction" not in df:
            df["direction"] = None

        is_human_question_set = "human" in question_set_filename
        human_llm_key = "human" if is_human_question_set else "llm"

        try:
            resolved_values_for_question_sources = get_resolution_values_for_forecast_due_date(
                question_set_filename=question_set_filename,
                forecast_due_date=forecast_due_date,
                resolved_values_for_question_sources=resolved_values_for_question_sources,
                resolution_values=resolution_values,
            )
        except ValueError as e:
            logger.exception(f"EXCEPTION caught {e}")
            return f"Error: {str(e)}", 400

        df_question_resolutions = resolved_values_for_question_sources[forecast_due_date][
            human_llm_key
        ].copy()

        df = check_and_prepare_forecast_file(
            df=df,
            forecast_due_date=forecast_due_date,
            organization=organization,
        )

        df = set_resolution_dates(
            df=df,
            df_question_resolutions=df_question_resolutions,
        )
        df = impute_missing_forecasts(
            df=df,
            organization=organization,
            model_organization=model_organization,
            model=model,
        )

        team_forecast = {
            "organization": organization,
            "model": model,
            "model_organization": model_organization,
            "forecast_due_date": forecast_due_date,
            "question_set": question_set_filename,
            # Convert to json then load to keep pandas json conversion
            # df.to_dict has different variable conversions and hence is undesireable
            "forecasts": json.loads(df.to_json(orient="records", date_format="iso")),
        }
        upload_processed_forecast_file(
            data=team_forecast,
            forecast_due_date=forecast_due_date,
            filename=f,
        )

    logger.info(colored("Done.", "red"))


if __name__ == "__main__":
    driver(None)
