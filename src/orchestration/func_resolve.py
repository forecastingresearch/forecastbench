"""Resolve forecasts entry point."""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any

import pandas as pd
from termcolor import colored

from helpers import dates, decorator, env, slack
from orchestration import _io
from resolve._impute import impute_missing_forecasts
from resolve._prepare import check_and_prepare_forecast_file, set_resolution_dates
from resolve.explode_question_set import explode_question_set
from resolve.resolve_all import resolve_all
from sources import DATA_SOURCE_NAMES, MARKET_SOURCE_NAMES, SOURCES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DELAY_IN_DAYS_BEFORE_FIRST_RESOLUTION = 14

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


def _get_resolutions_for_llm_question_set(forecast_due_date, question_bank):
    """Resolve LLM question set.

    Downloads question set, explodes, resolves, sends warnings.
    """
    filename = f"{forecast_due_date}-llm.json"
    logger.info(f"Getting resolutions for {filename}.")

    df_orig_question_set = _io.download_question_set_file(filename)
    df = explode_question_set(
        question_set_df=df_orig_question_set,
        forecast_due_date=forecast_due_date,
    )

    forecast_due_date_date = dates.convert_iso_str_to_date(forecast_due_date)
    df = resolve_all(
        df=df,
        question_bank=question_bank,
        sources=SOURCES,
        forecast_due_date=forecast_due_date_date,
    )

    # Send any Slack warnings from market resolution
    warnings = df.attrs.get("_resolve_warnings", [])
    if warnings:
        for message in warnings:
            slack.send_message(message=message)

    _print_question_set_breakdown(
        human_or_llm="LLM",
        forecast_due_date=forecast_due_date,
        df=df,
        df_orig_question_set=df_orig_question_set,
    )
    return df[QUESTION_SET_FIELDS]


def _get_resolutions_for_human_question_set(forecast_due_date, df_llm_resolutions):
    """Extract resolutions for human questions from LLM resolutions."""
    filename = f"{forecast_due_date}-human.json"
    df_orig_question_set = _io.download_question_set_file(filename)
    df = pd.merge(df_llm_resolutions, df_orig_question_set, on=["id", "source"]).reset_index(
        drop=True
    )
    df = df[QUESTION_SET_FIELDS]

    _print_question_set_breakdown(
        human_or_llm="HUMAN",
        forecast_due_date=forecast_due_date,
        df=df,
        df_orig_question_set=df_orig_question_set,
    )
    return df[QUESTION_SET_FIELDS]


def _print_question_set_breakdown(human_or_llm, forecast_due_date, df, df_orig_question_set):
    """Print info about question set breakdown."""
    logger.info("")
    logger.info(f"{human_or_llm} QUESTION SET Breakdown {forecast_due_date}")

    resolution_date = [df["resolution_date"].unique()]
    if len(resolution_date) == 0:
        logger.error("No resolution dates to breakdown.")
        return
    resolution_date = resolution_date[0]

    def is_combo(x):
        return isinstance(x, tuple)

    def get_df_len(df, single, sources):
        combo_mask = df["id"].apply(is_combo)
        df_tmp = df[~combo_mask] if single else df[combo_mask]
        df_tmp = df_tmp[df_tmp["resolution_date"].isin(resolution_date)]
        df_tmp = df_tmp[df_tmp["source"].isin(sources)]
        return len(df_tmp) if single else int(len(df_tmp) / 4)

    for source_type in ["data", "market"]:
        sources = DATA_SOURCE_NAMES if source_type == "data" else MARKET_SOURCE_NAMES
        for source in sources:
            if source in df["source"].unique():
                n_single = get_df_len(df=df, single=True, sources=[source])
                n_combo = get_df_len(df=df, single=False, sources=[source])
                n_orig = len(df_orig_question_set[df_orig_question_set["source"] == source])
                combo_info = (
                    "." if n_combo == 0 else f", No. Combo Q's {n_combo:,}/{n_orig:,} Orig No. Q's."
                )
                logger.info(
                    f" * {source} No. Single Q's: {n_single:,}/{n_orig:,} "
                    f"Orig No. Q's{combo_info}"
                )
        n_single = get_df_len(df=df, single=True, sources=sources)
        n_combo = get_df_len(df=df, single=False, sources=sources)
        n_questions = n_single + n_combo
        logger.info(f"TOTAL {source_type} questions: {n_questions:,}")

    n_single = get_df_len(
        df=df,
        single=True,
        sources=MARKET_SOURCE_NAMES + DATA_SOURCE_NAMES,
    )
    n_combo = get_df_len(
        df=df,
        single=False,
        sources=MARKET_SOURCE_NAMES + DATA_SOURCE_NAMES,
    )
    logger.info(f"TOTAL questions: {n_single + n_combo:,}")

    df_res_date = df[df["resolution_date"].isin(resolution_date)]
    for _, row in df_orig_question_set.iterrows():
        df_tmp = df_res_date[
            (df_res_date["id"] == row["id"]) & (df_res_date["source"] == row["source"])
        ]
        if df_tmp.empty:
            logger.warning(f" N/A resolution for {row['source']} {row['id']}")


@decorator.log_runtime
def driver(_: Any) -> None:
    """Resolve forecasts.

    Env:
        CLOUD_RUN_TASK_INDEX: automatically set by Cloud Run Jobs.
    """
    task_num = 0
    try:
        env_var = os.getenv("CLOUD_RUN_TASK_INDEX")
        task_num = int(env_var)
    except Exception as e:
        logger.error(f"Improperly set environment variable: CLOUD_RUN_TASK_INDEX = {env_var}")
        logger.error(e)
        return f"Error: {str(e)}", 400

    forecast_files, valid_dates = _io.get_valid_forecast_files_and_dates(
        bucket=env.FORECAST_SETS_BUCKET,
    )

    # Only consider forecasts asked > DELAY days ago
    cutoff_date = dates.get_date_today() - timedelta(days=DELAY_IN_DAYS_BEFORE_FIRST_RESOLUTION)
    valid_dates = [d for d in valid_dates if dates.convert_iso_str_to_date(d) <= cutoff_date]
    if task_num >= len(valid_dates):
        logger.info(f"task number {task_num} not needed, winding down...")
        return "OK", 200

    my_date = valid_dates[task_num]
    forecast_files = [f for f in forecast_files if f.startswith(my_date)]

    logger.info(f"\nProcessing forecasts for {my_date}.\n")

    # Load question bank
    question_bank = _io.load_question_bank()

    # Load hash mappings for sources that need them
    _io.load_hash_mapping(SOURCES["acled"], "acled")
    _io.load_hash_mapping(SOURCES["wikipedia"], "wikipedia")

    local_forecast_set_dir = _io._get_local_file_dir(bucket=env.FORECAST_SETS_BUCKET)
    resolved_cache: dict[str, dict] = {}

    for f in forecast_files:
        logger.info(f"Resolving {f}")
        file_data = _io.read_forecast_file(filename=f"{local_forecast_set_dir}/{f}")
        if file_data is None:
            continue

        organization = file_data.get("organization")
        model = file_data.get("model")
        model_organization = file_data.get("model_organization")
        question_set_filename = file_data.get("question_set")
        leaderboard_eligible = file_data.get("leaderboard_eligible", True)
        forecast_due_date = question_set_filename[:10]
        df = file_data.get("df")
        if "direction" not in df:
            df["direction"] = None

        is_human_question_set = "human" in question_set_filename
        human_llm_key = "human" if is_human_question_set else "llm"

        # Cache resolved question sets per forecast_due_date
        if forecast_due_date not in resolved_cache:
            logger.info(
                f"Found new question source: {forecast_due_date}. "
                "Downloading .json and resolving..."
            )
            try:
                llm_resolutions = _get_resolutions_for_llm_question_set(
                    forecast_due_date,
                    question_bank,
                )
                human_resolutions = _get_resolutions_for_human_question_set(
                    forecast_due_date,
                    llm_resolutions,
                )
                resolved_cache[forecast_due_date] = {
                    "llm": llm_resolutions,
                    "human": human_resolutions,
                }
                _io.upload_resolution_set(
                    df=llm_resolutions.copy(),
                    forecast_due_date=forecast_due_date,
                    question_set_filename=question_set_filename,
                )
            except ValueError as e:
                logger.exception(f"EXCEPTION caught {e}")
                return f"Error: {str(e)}", 400

        df_question_resolutions = resolved_cache[forecast_due_date][human_llm_key].copy()

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
            "leaderboard_eligible": leaderboard_eligible,
            "forecasts": json.loads(df.to_json(orient="records", date_format="iso")),
        }
        _io.upload_processed_forecast_file(
            data=team_forecast,
            forecast_due_date=forecast_due_date,
            filename=f,
        )

    logger.info(colored("Done.", "red"))


if __name__ == "__main__":
    driver(None)
