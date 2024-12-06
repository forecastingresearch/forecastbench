"""Generate the naive forecast."""

import itertools
import json
import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    acled,
    constants,
    dates,
    decorator,
    env,
    resolution,
    wikipedia,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

N_WINDOWS_FOR_FORECAST = 100
SHORT_WINDOW_LENGTH_FOR_FORECAST = 60
LONG_WINDOW_LENGTH_FOR_FORECAST = 60


def _helper_split_df(df, source):
    """Split df into source df and standard & combo question df."""
    df_source, df = resolution.split_dataframe_on_source(df=df, source=source)

    combo_mask = df_source["id"].apply(lambda x: resolution.is_combo(x))
    df_standard = df_source[~combo_mask].copy()
    df_combo = df_source[combo_mask].copy()

    return df, df_standard, df_combo


def _helper_fill_combo(df_standard, df_combo):
    """Fill in forecasts for df_combo given forecasts in df_standard."""

    def update_col(index, id0, id1, dir0, dir1, col):
        value_id0 = df_standard.loc[df_standard["id"] == id0, col].iloc[0]
        value_id1 = df_standard.loc[df_standard["id"] == id1, col].iloc[0]
        df_combo.at[index, col] = resolution.combo_change_sign(
            value_id0, dir0
        ) * resolution.combo_change_sign(value_id1, dir1)

    for index, row in df_combo.iterrows():
        id0, id1 = row["id"]
        dir0, dir1 = row["direction"]
        update_col(
            index=index,
            id0=id0,
            id1=id1,
            dir0=dir0,
            dir1=dir1,
            col="forecast",
        )

    return df_combo


def get_wikipedia_forecast(df, dfr):
    """Return the forecasts for all wikipedia questions in df."""
    wikipedia.populate_hash_mapping()
    df, df_standard, df_combo = _helper_split_df(df=df, source="wikipedia")
    dfr = wikipedia.ffill_dfr(dfr=dfr)

    yesterday = dates.get_date_yesterday()
    resolution_dates = [yesterday - timedelta(days=i) for i in range(N_WINDOWS_FOR_FORECAST)]
    for mid in df_standard["id"].unique():
        forecasts = [
            wikipedia.resolve(
                mid,
                dfr,
                resolution_date - timedelta(days=LONG_WINDOW_LENGTH_FOR_FORECAST),
                resolution_date,
            )
            for resolution_date in resolution_dates
        ]
        df_standard.loc[df_standard["id"] == mid, "forecast"] = get_forecast(
            sum(forecasts) / len(forecasts)
        )

    df_combo = _helper_fill_combo(df_standard=df_standard, df_combo=df_combo)
    df = pd.concat([df, df_standard, df_combo], ignore_index=True)
    return df


def get_acled_forecast(df, dfr):
    """Return the forecasts for all acled questions in df."""
    acled.populate_hash_mapping()
    df, df_standard, df_combo = _helper_split_df(df=df, source="acled")

    yesterday = dates.get_date_yesterday()
    resolution_dates = [yesterday - timedelta(days=7 * i) for i in range(N_WINDOWS_FOR_FORECAST)]
    for mid in df_standard["id"].unique():
        d = acled.id_unhash(mid)
        forecasts = []
        for resolution_date in resolution_dates:
            forecast = acled.resolve(
                **d,
                dfr=dfr,
                forecast_due_date=resolution_date - timedelta(days=LONG_WINDOW_LENGTH_FOR_FORECAST),
                resolution_date=resolution_date,
            )
            forecasts += [forecast]
        df_standard.loc[df_standard["id"] == mid, "forecast"] = get_forecast(
            sum(forecasts) / len(forecasts)
        )

    df_combo = _helper_fill_combo(df_standard=df_standard, df_combo=df_combo)
    df = pd.concat([df, df_standard, df_combo], ignore_index=True)
    return df


def get_fred_forecast(df, dfr):
    """Return the forecasts for all fred questions in df."""
    df, df_standard, df_combo = _helper_split_df(df=df, source="fred")

    dfr_tmp = dfr.copy()
    dfr_tmp["value"] = pd.to_numeric(dfr_tmp["value"], errors="coerce")

    def get_potentially_missing_value(dfr_mid, date):
        tmp = dfr_mid[dfr_mid["date"] == date]["value"]
        if len(tmp) == 0:
            raise
        return tmp.iloc[0]

    for mid in df_standard["id"].unique():
        is_updated_daily_or_weekly = len(df_standard[df_standard["id"] == mid]) == len(
            constants.FORECAST_HORIZONS_IN_DAYS
        )
        if is_updated_daily_or_weekly:
            retval = (
                dfr_tmp[dfr_tmp["id"] == mid]["value"]
                .rolling(
                    window=LONG_WINDOW_LENGTH_FOR_FORECAST,
                    min_periods=LONG_WINDOW_LENGTH_FOR_FORECAST,
                )
                .apply(lambda x: x[-1] > x[0], raw=True)
                .dropna()
                .tail(N_WINDOWS_FOR_FORECAST)
            )
        else:
            # This is monthly data
            dfr_mid = dfr_tmp[dfr_tmp["id"] == mid].copy()
            date_max = dfr_mid["date"].max()
            retval = np.array([])
            for i in range(N_WINDOWS_FOR_FORECAST):
                date = date_max - timedelta(days=LONG_WINDOW_LENGTH_FOR_FORECAST * i)
                try:
                    resolution_val = get_potentially_missing_value(dfr_mid=dfr_mid, date=date)
                    forecast_val = get_potentially_missing_value(
                        dfr_mid=dfr_mid, date=date - timedelta(days=LONG_WINDOW_LENGTH_FOR_FORECAST)
                    )
                    retval = np.append(retval, resolution_val > forecast_val)
                    if len(retval) == N_WINDOWS_FOR_FORECAST:
                        break
                except Exception:
                    break
        df_standard.loc[df_standard["id"] == mid, "forecast"] = get_forecast(retval.mean())

    df_combo = _helper_fill_combo(df_standard=df_standard, df_combo=df_combo)
    df = pd.concat([df, df_standard, df_combo], ignore_index=True)
    return df


def get_dbnomics_yfinance_forecast(df, dfr, source):
    """Return the forecasts for dbnomics and yfinance questions in df."""
    df, df_standard, df_combo = _helper_split_df(df=df, source=source)

    dfr_tmp = dfr.copy()
    dfr_tmp["value"] = pd.to_numeric(dfr_tmp["value"], errors="coerce")

    for mid in df_standard["id"].unique():
        retval = (
            dfr_tmp[dfr_tmp["id"] == mid]["value"]
            .rolling(
                window=LONG_WINDOW_LENGTH_FOR_FORECAST, min_periods=SHORT_WINDOW_LENGTH_FOR_FORECAST
            )
            .apply(lambda x: x[-1] > x[0], raw=True)
            .dropna()
            .tail(N_WINDOWS_FOR_FORECAST)
        )
        df_standard.loc[df_standard["id"] == mid, "forecast"] = get_forecast(retval.mean())

    df_combo = _helper_fill_combo(df_standard=df_standard, df_combo=df_combo)
    df = pd.concat([df, df_standard, df_combo], ignore_index=True)
    return df


def get_forecast(mean):
    """
    Cap the min and max possible forecasts.

    Force the min possible forecast to be 0.05 and the max possible forecast to be 0.95.
    """
    if pd.isna(mean):
        return 0.5
    return 0.05 if mean < 0.05 else 0.95 if mean > 0.95 else float(mean)


def get_dataset_forecasts(source, df, dfr):
    """Generate forecasts for all data questions from `source`."""
    logger.info(f"Getting forecasts for {source}")
    if source == "acled":
        return get_acled_forecast(df=df, dfr=dfr)
    elif source == "fred":
        return get_fred_forecast(df=df, dfr=dfr)
    elif source == "wikipedia":
        return get_wikipedia_forecast(df=df, dfr=dfr)
    elif source in ["dbnomics", "yfinance"]:
        return get_dbnomics_yfinance_forecast(df=df, dfr=dfr, source=source)
    else:
        msg = f"Unknown source: {source}"
        logger.error(msg)
        raise ValueError(msg)


# def get_market_forecasts(source, df, dfr):
#     """Generate forecasts for all market questions from `source`.

#     We could just set these to null and have them imputed at resolution.
#     """
#     df_market, df = resolution.split_dataframe_on_source(df=df, source=source)
#     unique_ids_for_resolved_markets = dfr["id"].unique()

#     def check_id(mid):
#         if resolution.is_combo(mid):
#             for midi in mid:
#                 check_id(midi)
#         elif mid not in unique_ids_for_resolved_markets:
#             msg = f"Missing resolution values in dfr for (source: {source}, id: {mid})!!!"
#             logger.error(msg)
#             raise ValueError(msg)

#     df_market["id"].apply(lambda x: check_id(x))

#     # Handle single markets first: split into standard and combo questions
#     combo_mask = df_market["id"].apply(lambda x: resolution.is_combo(x))
#     df_standard = df_market[~combo_mask].copy()
#     df_combo = df_market[combo_mask].copy()

#     # Convert freeze datetime value into floats. This only exists for standard questions. Hence,
#     # raise errors for standard questions, use coerce to set NaN values for combo questions
#     df_standard["freeze_datetime_value"] = pd.to_numeric(
#         df_standard["freeze_datetime_value"], errors="raise"
#     )
#     df_combo["freeze_datetime_value"] = pd.to_numeric(
#         df_standard["freeze_datetime_value"], errors="coerce"
#     )

#     # Resolve forecasts at all horizons to yesterday's market value.
#     df_standard = pd.merge(
#         df_standard,
#         dfr,
#         left_on=["id", "last_date_for_data"],
#         right_on=["id", "date"],
#         how="left",
#     )
#     df_standard["forecast"] = df_standard["value"]
#     df_standard = df_standard.drop(columns=["date", "value"])

#     # Any null values were resolved before the forecast due date and won't actually be considered
#     # when scoring. Hence, just fill thees with the freeze value.
#     df_standard.loc[df_standard["forecast"].isna(), "forecast"] = df_standard[
#         "freeze_datetime_value"
#     ]
#     df_standard.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)

#     # Setup combo resolutions given df_standard
#     def update_col(index, id0, id1, dir0, dir1, col):
#         value_id0 = df_standard.loc[df_standard["id"] == id0, col].iloc[0]
#         value_id1 = df_standard.loc[df_standard["id"] == id1, col].iloc[0]
#         df_combo.at[index, col] = resolution.combo_change_sign(
#             value_id0, dir0
#         ) * resolution.combo_change_sign(value_id1, dir1)

#     for index, row in df_combo.iterrows():
#         id0, id1 = row["id"]
#         dir0, dir1 = row["direction"]
#         update_col(
#             index=index,
#             id0=id0,
#             id1=id1,
#             dir0=dir0,
#             dir1=dir1,
#             col="forecast",
#         )
#     df_combo.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)
#     df = pd.concat([df, df_standard, df_combo], ignore_index=True)
#     return df


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

    # Expand directions for combo questions
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

    return df


@decorator.log_runtime
def driver(_):
    """Generate the naive forecast."""
    question_set_filename = "latest-llm.json"
    df = resolution.download_and_read_question_set_file(question_set_filename)
    forecast_due_date = resolution.get_field_from_question_set_file(
        filename=question_set_filename, field="forecast_due_date"
    )
    question_set_filename = resolution.get_field_from_question_set_file(
        filename=question_set_filename, field="question_set"
    )
    forecast_due_date = pd.to_datetime(forecast_due_date)
    last_date_for_data = pd.to_datetime(forecast_due_date) - pd.to_timedelta(1, unit="D")

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

    logger.info("Downloading latest data...")
    resolution_values = resolution.get_and_pickle_resolution_values(
        filename="resolution_values.pkl",
        sources_to_get=resolution.DATA_SOURCES,
        save_pickle_file=False,
    )
    logger.info("Done downloading data.")

    # truncate resolution values to last date of data to consider for the forecast
    for source in resolution_values:
        date_col = "event_date" if source == "acled" else "date"
        dfr = resolution_values[source]["dfr"]
        dfr = dfr[dfr[date_col] <= last_date_for_data].reset_index(drop=True)
        resolution_values[source]["dfr"] = dfr.copy()

    logger.info(f"Generating naive forecast for {forecast_due_date.strftime('%Y-%m-%d')}...")
    # for source in resolution.MARKET_SOURCES:
    #     df = get_market_forecasts(
    #         source=source, df=df.copy(), dfr=resolution_values[source]["dfr"].copy()
    #     )

    for source in resolution.DATA_SOURCES:
        df = get_dataset_forecasts(
            source=source, df=df.copy(), dfr=resolution_values[source]["dfr"].copy()
        )

    df = df[["id", "source", "forecast", "resolution_date", "reasoning", "direction"]]
    df["direction"] = df["direction"].mask(df["direction"].apply(lambda x: x == ()), None)
    df["resolution_date"] = df["resolution_date"].astype(str).replace("NaT", None)

    forecast_due_date = forecast_due_date.strftime("%Y-%m-%d")
    data = {
        "organization": constants.BENCHMARK_NAME,
        "model": "Naive Forecaster",
        "question_set": question_set_filename,
        "forecast_due_date": forecast_due_date,
        "forecasts": df.reset_index(drop=True).to_dict(orient="records"),
    }

    forecast_filename = f"{forecast_due_date}.{constants.BENCHMARK_NAME}.naive-forecaster.json"
    local_filename = f"/tmp/{forecast_filename}"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))

    gcp.storage.upload(
        bucket_name=env.FORECAST_SETS_BUCKET,
        local_filename=local_filename,
        filename=f"{forecast_due_date}/{forecast_filename}",
    )

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
