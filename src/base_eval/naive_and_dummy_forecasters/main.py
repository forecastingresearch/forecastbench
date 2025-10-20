"""Generate the naive forecast."""

import json
import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from prophet import Prophet
from scipy.stats import norm

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    acled,
    constants,
    decorator,
    env,
    question_sets,
    resolution,
    wikipedia,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").propagate = False
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("prophet").propagate = False

N_WINDOWS_FOR_FORECAST = 100
SHORT_WINDOW_LENGTH_FOR_FORECAST = 30
LONG_WINDOW_LENGTH_FOR_FORECAST = 90

MAX_FORECAST_HORIZON = max(constants.FORECAST_HORIZONS_IN_DAYS)

# The amount of data to remove from the dataset
# This is because when running on forecast due date, not all data is available until the day before
# so we can retroactively run the naive forecast
DATA_OFFSETS = {
    "acled": 30,
    "fred": 30,
}


def get_day_before_forecast_due_date(forecast_due_date):
    """Subtract 1 day from the provided date."""
    return forecast_due_date - timedelta(days=1)


def get_prophet_forecast(
    source, df, dfr, day_before_forecast_due_date, prophet_args, forecast_due_date_plus_max_horizon
):
    """Get forecast for source from Prophet."""
    df_standard, df = resolution.split_dataframe_on_source(df=df, source=source)

    dfr["value"] = pd.to_numeric(dfr["value"], errors="coerce")

    resolution_dates = sorted(df_standard["resolution_date"].unique())

    for mid in df_standard["id"].unique():
        dfr_mid = dfr[dfr["id"] == mid].sort_values(by="date", ignore_index=True).ffill().bfill()
        comparison_value = dfr_mid["value"].iloc[-1]

        if source == "fred":
            dfr_mid = dfr_mid[
                dfr_mid["date"]
                < day_before_forecast_due_date - timedelta(days=DATA_OFFSETS["fred"])
            ]
        prophet_df = dfr_mid.rename(columns={"date": "ds", "value": "y"})

        model = Prophet(**prophet_args)
        model.fit(prophet_df)

        periods = (forecast_due_date_plus_max_horizon - max(prophet_df["ds"]).date()).days
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        for resolution_date in resolution_dates:
            row = forecast[forecast["ds"].dt.date == resolution_date]

            forecast_mean = row["yhat"].values[0]
            lower = row["yhat_lower"].values[0]
            upper = row["yhat_upper"].values[0]
            forecast_std = (upper - lower) / (2 * 1.28)

            if source in ["fred", "yfinance"]:
                # linear interpolation
                prob_increase = (forecast_mean - lower) / (upper - lower)
            else:
                # normal approximation
                prob_increase = 1 - norm.cdf(
                    comparison_value, loc=forecast_mean, scale=forecast_std
                )

            mask = (df_standard["id"] == mid) & (df_standard["resolution_date"] == resolution_date)
            df_standard.loc[mask, "forecast"] = get_bounded_forecast(prob_increase)

    df = pd.concat(
        [
            df,
            df_standard,
        ],
        ignore_index=True,
    )
    return df


def get_wikipedia_forecast(df, dfr, forecast_due_date_plus_max_horizon):
    """Return the forecasts for all wikipedia questions in df."""
    wikipedia.populate_hash_mapping()
    df_standard, df = resolution.split_dataframe_on_source(df=df, source="wikipedia")

    resolution_dates = sorted(df_standard["resolution_date"].unique())

    for mid in df_standard["id"].unique():
        dfr_mid = dfr[dfr["id"] == mid].sort_values(by="date", ignore_index=True)

        dfr_mid = wikipedia.backfill_for_forecast(mid, dfr_mid)

        v = dfr_mid["value"].map(lambda x: isinstance(x, str)).any()
        if v:
            dfr_mid["value"] = dfr_mid["value"].apply(lambda x: 0 if pd.isna(x) else 1)

        comparison_value = dfr_mid["value"].iloc[-1]

        prophet_df = dfr_mid.rename(columns={"date": "ds", "value": "y"})
        floor, carrying_capacity = wikipedia.get_min_max_possible_value(mid)
        prophet_df["cap"] = carrying_capacity
        prophet_df["floor"] = floor

        model = Prophet(
            growth="logistic",
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        model.fit(prophet_df)

        periods = (forecast_due_date_plus_max_horizon - max(prophet_df["ds"]).date()).days
        future = model.make_future_dataframe(periods=periods)
        future["cap"] = carrying_capacity
        future["floor"] = floor
        forecast = model.predict(future)

        for resolution_date in resolution_dates:
            row = forecast[forecast["ds"].dt.date == resolution_date]

            forecast_mean = row["yhat"].values[0]
            forecast_std = (row["yhat_upper"].values[0] - row["yhat_lower"].values[0]) / (2 * 1.28)

            mask = (df_standard["id"] == mid) & (df_standard["resolution_date"] == resolution_date)
            df_standard.loc[mask, "forecast"] = get_bounded_forecast(
                wikipedia.get_probability_forecast(
                    mid,
                    comparison_value,
                    forecast_mean,
                    forecast_std,
                )
            )

    df = pd.concat(
        [
            df,
            df_standard,
        ],
        ignore_index=True,
    )
    return df


def get_acled_forecast(df, dfr, day_before_forecast_due_date, forecast_due_date_plus_max_horizon):
    """Return the forecasts for all acled questions in df."""
    acled.populate_hash_mapping()
    df_standard, df = resolution.split_dataframe_on_source(df=df, source="acled")

    resolution_dates = sorted(df_standard["resolution_date"].unique())

    for mid in df_standard["id"].unique():
        d = acled.id_unhash(mid)
        country = d["country"]
        col_event_type = d["event_type"]
        logger.info(f"Getting ACLED forecast for {mid}.")

        end_date = day_before_forecast_due_date - timedelta(days=DATA_OFFSETS["acled"])
        comparison_value = acled.get_base_comparison_value(
            key=d["key"],
            dfr=dfr,
            country=country,
            col=col_event_type,
            ref_date=end_date.date(),
        )

        # Fill dfr_country with 0s for event type on days where no events ocurred
        dfr_country = dfr[dfr["country"] == country]
        start_date = dfr["event_date"].min()

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        full_df = pd.DataFrame({"event_date": date_range})
        dfr_mid = pd.merge(
            full_df, dfr_country[["event_date", col_event_type]], on="event_date", how="left"
        ).fillna(0)
        prophet_df = dfr_mid.rename(columns={"event_date": "ds", col_event_type: "y"})

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        model.fit(prophet_df)

        periods = (forecast_due_date_plus_max_horizon - max(prophet_df["ds"]).date()).days
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)

        for resolution_date in resolution_dates:
            mask = (df_standard["id"] == mid) & (df_standard["resolution_date"] == resolution_date)
            df_standard.loc[mask, "forecast"] = get_bounded_forecast(
                acled.get_forecast(
                    comparison_value=comparison_value,
                    dfr=forecast.copy(),
                    country=country,
                    col=col_event_type,
                    ref_date=resolution_date,
                )
            )

    df = pd.concat([df, df_standard], ignore_index=True)
    return df


def get_bounded_forecast(mean):
    """
    Cap the min and max possible forecasts.

    Force the min possible forecast to be 0.05 and the max possible forecast to be 0.95.
    """
    if pd.isna(mean):
        return 0.5
    return 0.05 if mean < 0.05 else 0.95 if mean > 0.95 else float(mean)


def get_market_holidays(start_date, end_date):
    """Return a list of market holidays (federal and weekend) for NYSE/Nasdaq."""
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    nyse = mcal.get_calendar("NYSE")
    holidays = [h for h in nyse.holidays().holidays if h >= start_date and h <= end_date]
    df_holidays = pd.DataFrame(
        {
            "ds": holidays,
            "holiday": "Market Holiday",
        }
    )

    all_dates = pd.date_range(start=start_date, end=end_date)
    weekends = [np.datetime64(d, "D") for d in all_dates if d.weekday() >= 5]
    df_weekends = pd.DataFrame(
        {
            "ds": weekends,
            "holiday": "Weekend",
        }
    )

    return pd.concat([df_weekends, df_holidays])


def get_dataset_forecasts(source, df, dfr, forecast_due_date):
    """Generate forecasts for all data questions from `source`."""
    logger.info(f"Getting forecasts for {source}")

    def remove_newer_dates_from_dfr(dfr, day_before_forecast_due_date):
        return dfr[dfr["date"] <= day_before_forecast_due_date].copy()

    day_before_forecast_due_date = get_day_before_forecast_due_date(forecast_due_date)
    forecast_due_date_plus_max_horizon = (
        forecast_due_date + timedelta(days=MAX_FORECAST_HORIZON)
    ).date()
    if source in [
        "dbnomics",
        "fred",
        "yfinance",
    ]:
        dfr = remove_newer_dates_from_dfr(dfr, day_before_forecast_due_date)

    if source in ["dbnomics", "fred", "yfinance"]:
        if source == "fred":
            prophet_args = {
                "yearly_seasonality": True,
                "weekly_seasonality": True,
                "daily_seasonality": False,
            }
        elif source == "dbnomics":
            prophet_args = {
                "changepoint_prior_scale": 0.05,
                "seasonality_mode": "multiplicative",
                "yearly_seasonality": True,
                "weekly_seasonality": False,
                "daily_seasonality": False,
            }
        elif source == "yfinance":
            prophet_args = {
                "changepoint_prior_scale": 0.1,
                "holidays": get_market_holidays(
                    start_date=dfr["date"].min(),
                    end_date=forecast_due_date_plus_max_horizon,
                ),
                "yearly_seasonality": True,
                "weekly_seasonality": True,
                "daily_seasonality": False,
            }

        return get_prophet_forecast(
            source=source,
            df=df,
            dfr=dfr,
            day_before_forecast_due_date=day_before_forecast_due_date,
            prophet_args=prophet_args,
            forecast_due_date_plus_max_horizon=forecast_due_date_plus_max_horizon,
        )
    elif source == "acled":
        return get_acled_forecast(
            df=df,
            dfr=dfr,
            day_before_forecast_due_date=day_before_forecast_due_date,
            forecast_due_date_plus_max_horizon=forecast_due_date_plus_max_horizon,
        )
    elif source == "wikipedia":
        dfr = wikipedia.ffill_dfr(dfr=dfr)
        dfr = remove_newer_dates_from_dfr(dfr, day_before_forecast_due_date)
        return get_wikipedia_forecast(
            df=df,
            dfr=dfr,
            forecast_due_date_plus_max_horizon=forecast_due_date_plus_max_horizon,
        )

    msg = f"Unknown source: {source}"
    logger.error(msg)
    raise ValueError(msg)


def prepare_df_and_set_null_values(df, forecast_due_date, last_date_for_data):
    """Prepare the df by setting default values and expanding."""
    df["reasoning"] = ""
    df["forecast"] = None
    df["forecast_due_date"] = forecast_due_date

    # Use the forecast date - 1 day because that's the data available ON the forecast date.
    df["last_date_for_data"] = last_date_for_data
    df = resolution.make_columns_hashable(df)

    # Expand resolution dates
    df["resolution_dates"] = df.apply(
        lambda x: ([] if x["source"] in resolution.MARKET_SOURCES else x["resolution_dates"]),
        axis=1,
    )
    df = df.explode("resolution_dates", ignore_index=True)
    df.rename(columns={"resolution_dates": "resolution_date"}, inplace=True)
    df["resolution_date"] = pd.to_datetime(df["resolution_date"]).dt.date

    df = df.sort_values(by=["source", "resolution_date"], ignore_index=True)
    return df


def write_and_upload_forecast_file(data, df, model_name):
    """Write and upload forecast file."""
    data["model"] = model_name
    data["forecasts"] = df.reset_index(drop=True).to_dict(orient="records")
    forecast_due_date = data["forecast_due_date"]
    model_name_for_file = model_name.lower().replace(" ", "-")
    forecast_filename = f"{forecast_due_date}.{constants.BENCHMARK_NAME}.{model_name_for_file}.json"
    local_filename = f"/tmp/{forecast_filename}"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))

    if not env.RUNNING_LOCALLY:
        gcp.storage.upload(
            bucket_name=env.FORECAST_SETS_BUCKET,
            local_filename=local_filename,
            filename=f"{forecast_due_date}/{forecast_filename}",
        )


def create_dummy_files(data, df):
    """Create dummy files for the llm question set as it's a superset of the human question set."""
    logger.info("Creating dummy forecasts.")

    dummy_file_info = {
        "Always 0.5": {
            "func": lambda df: 0.5,
        },
        "Always 1": {
            "func": lambda df: 1.0,
        },
        "Always 0": {
            "func": lambda df: 0.0,
        },
        "Random Uniform": {
            "func": lambda df: np.random.rand(len(df)),
        },
        "Imputed Forecaster": {
            "func": lambda df: None,
        },
    }

    for key, value in dummy_file_info.items():
        df_dummy = df.copy()
        df_dummy["forecast"] = value["func"](df_dummy)
        write_and_upload_forecast_file(data=data, df=df_dummy, model_name=key)


@decorator.log_runtime
def driver(_):
    """Generate the naive forecast."""
    df = question_sets.download_and_read_latest_question_set_file()
    forecast_due_date = question_sets.get_field_from_latest_question_set_file("forecast_due_date")
    question_set_filename = question_sets.get_field_from_latest_question_set_file("question_set")

    forecast_due_date = pd.to_datetime(forecast_due_date)
    last_date_for_data = pd.to_datetime(forecast_due_date) - pd.to_timedelta(1, unit="D")
    logger.info(f"Forecast due date: {forecast_due_date}")
    logger.info(f"Last date for data: {last_date_for_data}")

    df = prepare_df_and_set_null_values(
        df=df[
            [
                "id",
                "source",
                "resolution_dates",
                "freeze_datetime_value",
            ]
        ].copy(),
        forecast_due_date=forecast_due_date,
        last_date_for_data=last_date_for_data,
    )

    logger.info("Downloading latest resolution data...")
    resolution_values = resolution.get_resolution_values()
    logger.info("Done downloading resolution data.")

    # truncate resolution values to last date of data to consider for the forecast
    for source in resolution_values:
        date_col = "event_date" if source == "acled" else "date"
        dfr = resolution_values[source]["dfr"]
        dfr = dfr[dfr[date_col] <= last_date_for_data].reset_index(drop=True)
        resolution_values[source]["dfr"] = dfr.copy()

    logger.info(f"Generating naive forecast for {forecast_due_date.strftime('%Y-%m-%d')}...")
    for source in resolution.DATA_SOURCES:
        df = get_dataset_forecasts(
            source=source,
            df=df.copy(),
            dfr=resolution_values[source]["dfr"].copy(),
            forecast_due_date=forecast_due_date,
        )

    df = df[
        [
            "id",
            "source",
            "forecast",
            "resolution_date",
            "reasoning",
        ]
    ]
    df["resolution_date"] = df["resolution_date"].astype(str).replace("NaT", None)

    forecast_due_date = forecast_due_date.strftime("%Y-%m-%d")

    data = {
        "organization": constants.BENCHMARK_NAME,
        "model_organization": constants.BENCHMARK_NAME,
        "question_set": question_set_filename,
        "forecast_due_date": forecast_due_date,
    }
    write_and_upload_forecast_file(data=data, df=df, model_name="Naive Forecaster")
    create_dummy_files(data=data, df=df)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
