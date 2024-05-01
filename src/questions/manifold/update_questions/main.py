"""Generate questions from Manifold API."""

import logging
import os
import sys
from datetime import timedelta

import backoff
import certifi
import numpy as np
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "manifold"
filenames = data_utils.generate_filenames(source=source)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=70,
    on_backoff=data_utils.print_error_info_handler,
)
def _get_market(market_id):
    """Get the market description and close time for the specified market."""
    logger.info(f"Calling market endpoint for {market_id}")
    endpoint = f"https://api.manifold.markets/v0/market/{market_id}"
    response = requests.get(endpoint, verify=certifi.where())
    if not response.ok:
        logger.error(f"Request to market endpoint failed for {market_id}.")
        response.raise_for_status()
    return response.json()


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=70,
    on_backoff=data_utils.print_error_info_handler,
)
def _get_market_forecasts(market_id):
    """Get the market description and close time for the specified market."""
    logger.info(f"Calling bets endpoint for {market_id}")
    endpoint = "https://api.manifold.markets/v0/bets"
    params = {
        "contractId": market_id,
        "order": "asc",
    }

    all_bets = []
    n_requests = 0
    while True:
        n_requests += 1
        logger.info(f"Request number {n_requests}.")
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(f"Request to bets endpoint failed for {market_id}.")
            response.raise_for_status()
        if len(response.json()) == 0:
            break
        all_bets += [m for m in response.json()]
        params["after"] = all_bets[-1]["id"]
    return all_bets


def _update_questions_and_resolved_values(dfq, dff):
    """Update the dataframes that hold the questions and the resolution values.

    For Manifold, store resolution values by market id to decrease calls to endpoint. First check
    the file to see if an entry exists for today. If so, skip. Otherwise, recreate the file. When
    done, return dfq.

    dfq: Manifold questions in the question bank
    dff: Today's fetched markets
    """
    # Use yesterday because we run at midnight UTC so have complete info for yesterday.
    YESTERDAY = dates.get_date_today() - timedelta(days=1)

    def _get_resolved_market_value(market):
        """Get the market value based on the resolution.

        A market that has resolved should return the resolved value. The possible values for
        market["resolution"] and the associated return values are:
        * YES -> 1
        * NO -> 0
        * MKT -> market probability
        * CANCEL (i.e. N/A) -> NaN
        """
        return {"YES": 1, "NO": 0, "MKT": market["resolutionProbability"]}.get(
            market["resolution"], np.nan
        )

    def _create_resolution_file(dfq, index, market):

        basename = f"{market['id']}.jsonl"
        remote_filename = f"{source}/{basename}"
        local_filename = "/tmp/tmp.jsonl"
        gcp.storage.download_no_error_message_on_404(
            bucket_name=constants.BUCKET_NAME,
            filename=remote_filename,
            local_filename=local_filename,
        )
        df = pd.read_json(
            local_filename,
            lines=True,
            dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
            convert_dates=False,
        )
        if not df.empty and pd.to_datetime(df["date"].iloc[-1]).date() >= YESTERDAY:
            # Check last datetime to see if we've already gotten the resolution value for today
            # If we have, return to avoid unnecessary API calls
            return df["value"].iloc[-1]

        # Get the last market value for the day and make this the value for the day
        forecasts = _get_market_forecasts(market["id"])
        df = pd.DataFrame(
            [
                {
                    "datetime": dates.convert_epoch_time_in_ms_to_iso(forecast["createdTime"]),
                    "value": forecast["probAfter"],
                }
                for forecast in forecasts
                if forecast.get("isFilled")
            ]
        )
        if df.empty:
            return None

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values(by="datetime")
        df["date"] = df["datetime"].dt.date
        df = df[df["date"] <= YESTERDAY]
        df = df.groupby(by="date").last().reset_index()
        df = df[["date", "value"]]

        date_range = pd.date_range(start=df["date"].min(), end=YESTERDAY, freq="D")
        if market["isResolved"]:
            # If the market has been resolved, add the market value and resolution date
            resolved_date = pd.Timestamp(dfq.at[index, "source_resolution_datetime"]).date()
            df = df[df["date"] <= resolved_date]
            df.loc[len(df)] = {
                "date": resolved_date,
                "value": _get_resolved_market_value(market),
            }
            date_range = pd.date_range(start=df["date"].min(), end=resolved_date, freq="D")

        df_dates = pd.DataFrame(date_range, columns=["date"])
        df_dates["date"] = df_dates["date"].dt.date
        df = pd.merge(left=df_dates, right=df, on="date", how="left")
        df = df.ffill()

        df["id"] = market["id"]
        df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

        # Save and Upload
        df.to_json(local_filename, orient="records", lines=True, date_format="iso")
        gcp.storage.upload(
            bucket_name=constants.BUCKET_NAME,
            local_filename=local_filename,
            filename=remote_filename,
        )

        # Return the last market value for the series
        return df["value"].iloc[-1]

    def _assign_market_values_to_df(df, index, market):
        url = market["url"]
        df.at[index, "question"] = market["question"]
        df.at[index, "background"] = market["textDescription"]
        df.at[index, "source_resolution_criteria"] = "N/A"
        df.at[index, "source_begin_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
            market["createdTime"]
        )
        df.at[index, "source_close_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
            market["closeTime"]
        )
        df.at[index, "url"] = url
        if market["isResolved"]:
            df.at[index, "resolved"] = True
            df.at[index, "source_resolution_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
                market["resolutionTime"]
            )
        df.at[index, "continual_resolution"] = False
        df.at[index, "forecast_horizons"] = data_utils.get_horizons(
            dates.convert_epoch_in_ms_to_datetime(market["closeTime"])
        )
        return df

    # Find rows in dff not in dfq: These are the new markets to add to dfq
    col_to_append = dff[~dff["id"].isin(dfq["id"])]["id"]

    # Set all non-id columns to `None` for the new markets
    df_ids_to_append = pd.DataFrame(col_to_append).assign(
        **{col: None for col in dfq.columns if col != "id"}
    )
    df_ids_to_append["resolved"] = False
    df_ids_to_append["value_at_freeze_datetime_explanation"] = "The market value."
    df_ids_to_append["source_resolution_datetime"] = "N/A"
    dfq = pd.concat([dfq, df_ids_to_append], ignore_index=True)

    # Update all unresolved questions in dfq. Update resolved, resolution_datetime, and background.
    # Recreate all rows of resolution files for unresolved questions
    for index, row in dfq[dfq["resolved"] == False].iterrows():  # noqa: E712
        market = _get_market(row["id"])
        dfq = _assign_market_values_to_df(dfq, index, market)
        dfq.at[index, "value_at_freeze_datetime"] = _create_resolution_file(dfq, index, market)

    return dfq


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    dff = data_utils.download_and_read(
        filename=filenames["jsonl_fetch"],
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=["id"]),
        dtype={"id": str},
    )
    dfq = data_utils.get_data_from_cloud_storage(
        source=source,
        return_question_data=True,
    )

    # Update the existing questions and resolution values
    dfq = _update_questions_and_resolved_values(dfq, dff)

    # Save and upload
    data_utils.upload_questions(dfq, source)

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
