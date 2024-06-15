"""Resolve forecasting questions."""

import itertools
import json
import logging
import os
import sys

import acled
import markets
import numpy as np
import pandas as pd
import resolution_helpers

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    return df


def get_question_file(filename):
    """Download question set file."""
    df = pd.read_json(
        f"gs://{env.QUESTION_SETS_BUCKET}/{filename}",
        lines=True,
        convert_dates=False,
    )
    df = df[["id", "source", "forecast_horizons"]]
    df = resolution_helpers.make_columns_hashable(df)
    n_start = len(df)
    logger.info(f"Question set starting with {n_start} questions.")

    # DROP COMBO QUESTIONS FOR MARKETS
    df = df[
        ~df.apply(
            lambda x: resolution_helpers.is_combo(x) and x["source"] in markets.MARKET_SOURCES,
            axis=1,
        )
    ].reset_index(drop=True)
    return df


def get_resolutions_for_llm_question_set(filename, forecast_date, resolution_values):
    """
    Given a forecast date, find available resolution values for the associatdd llm question file.

    Params:
    - forecast-date: ISO date as string
    - resolution_values: dictionary of latest resolution values downloaded from storage bucket.
    """
    logger.info(f"Getting question set: {filename}.")
    df = get_question_file(filename)

    # Use the forecast date - 1 day because that's the data available ON the forecast date.
    df["forecast_submitted_date"] = pd.to_datetime(forecast_date) - pd.to_timedelta(1, unit="D")
    df["forecast_evaluation_date"] = df["forecast_submitted_date"]

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
                dfq_resolved["id"] == mid, "market_info_resolution_datetime"
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
        if resolution_helpers.is_combo(row):
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
    logger.info(f"Question set has {len(df)} questions.")
    return df[QUESTION_SET_FIELDS]


def create_naive_forecast(filename, forecast_date, df_question_resolutions, dataset_base_rates):
    """Create the base rate forecast."""
    df = get_question_file(filename)

    df["forecast_submitted_date"] = pd.to_datetime(forecast_date)

    # Set forecasts for data questions across all horizons to the base rate for that question
    # Expand horizons and directions for combo questions (which only exist for data questions)
    df_dataset = None
    new_rows = []
    indices_to_drop = []
    df["direction"] = df.apply(lambda x: tuple(), axis=1)
    df.rename(columns={"forecast_horizons": "horizon"}, inplace=True)
    for key, value in dataset_base_rates.items():
        df_tmp = df[df["source"] == key].copy()
        df = df[df["source"] != key]
        df_question_resolutions = df_question_resolutions[df_question_resolutions["source"] != key]
        df_tmp = df_tmp.explode("horizon", ignore_index=True)

        for index, row in df_tmp.iterrows():
            if resolution_helpers.is_combo(row):
                indices_to_drop += [index]
                for direction in itertools.product((1, -1), repeat=len(row["id"])):
                    new_row = row.copy()
                    new_row["direction"] = direction
                    new_rows.append(new_row)
        df_tmp = df_tmp.drop(indices_to_drop)
        df_tmp = pd.concat([df_tmp, pd.DataFrame(new_rows)], ignore_index=True)
        df_tmp["forecast"] = value
        df_dataset = (
            df_tmp if df_dataset is None else pd.concat([df_dataset, df_tmp], ignore_index=True)
        )

    # For market questions, horizon doesn't matter and there are no combos (yet)
    # Now df and df_question_resolutions will only have market questions
    df = pd.merge(
        df,
        df_question_resolutions[["id", "source", "forecast"]],
        on=[
            "id",
            "source",
        ],
        suffixes=(None, "_df"),
    )
    df["horizon"] = None
    df["direction"] = None

    df = pd.concat([df, df_dataset], ignore_index=True)
    df["reasoning"] = ""

    # Forecasts with value na are those that would have resolved between freeze date and LM
    # forecast date.
    df.loc[df["forecast"].isna(), "forecast"] = None
    return df[
        [
            "id",
            "source",
            "horizon",
            "direction",
            "forecast",
            "reasoning",
        ]
    ]


@decorator.log_runtime
def driver(request):
    """Generate the naive forecast."""
    request_json = request.get_json(silent=True)

    filename = request_json.get("filename") if request_json else None
    if not filename:
        return "Bad Request", 400
    forecast_date = filename[:10]

    """Resolve forecasts."""
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

    logger.info(f"Getting base rate values`{filename}`...")
    df_question_resolutions = get_resolutions_for_llm_question_set(
        filename=filename, forecast_date=forecast_date, resolution_values=resolution_values
    )

    logger.info("Calculate base rate for all data sources.")
    dataset_base_rates = {}
    for source in df_question_resolutions["source"].unique():
        if source in constants.DATA_SOURCES:
            df_source = df_question_resolutions[df_question_resolutions["source"] == source]
            base_rate = df_source["resolved_to"].eq(1).sum() / len(df_source)
            dataset_base_rates = {source: base_rate}

    logger.info("Create forecast file.")
    df_question_resolutions.rename(columns={"resolved_to": "forecast"}, inplace=True)
    df = create_naive_forecast(
        filename=filename,
        forecast_date=forecast_date,
        df_question_resolutions=df_question_resolutions,
        dataset_base_rates=dataset_base_rates,
    ).reset_index(drop=True)

    logger.info("Write forecast file.")

    df["forecast"] = df["forecast"].apply(lambda x: None if pd.isna(x) else x)
    forecasts = df.reset_index(drop=True).to_dict(orient="records")

    # For questions that could have resolved between freeze and forecast date
    for forecast in forecasts:
        if pd.isna(forecast["forecast"]):
            forecast["forecast"] = None
    data = {
        "organization": constants.BENCHMARK_NAME,
        "model": "Naive Forecast",
        "question_set": filename,
        "forecast_date": forecast_date,
        "forecasts": forecasts,
    }

    local_filename = f"/tmp/{forecast_date}.{constants.BENCHMARK_NAME}.naive-forecast.json"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))

    gcp.storage.upload(
        bucket_name=constants.FORECAST_BUCKET_NAME,
        local_filename=local_filename,
    )

    df.to_json(local_filename)

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    """Local dev."""

    class MockRequest:
        """Class to mock requsets for local dev."""

        def __init__(self, json_data):
            """Mock __init__ from request class."""
            self._json = json_data

        def get_json(self, silent=False):
            """Mock get_json from request class."""
            return self._json

    mock_request = MockRequest({"filename": "2024-05-03-llm.jsonl"})
    driver(mock_request)
