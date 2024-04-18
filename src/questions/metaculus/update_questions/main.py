"""Generate questions from Metaculus API."""

import logging
import os
import sys

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

source = "metaculus"
filenames = data_utils.generate_filenames(source=source)
API_KEY = os.environ.get("API_KEY_METACULUS")
# The Metaculus rate limit is 1,000 queries per hour, so we limit the number of questions we use
# to 1,000 - number of queries executed by the `fetch` function.
QUESTION_LIMIT = 1000 - (len(constants.METACULUS_CATEGORIES) + 1)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    on_backoff=data_utils.print_error_info_handler,
)
def _get_market(market_id):
    """Get the market description and resolution criteria for the specified market."""
    logger.info(f"Calling market endpoint for {market_id}")
    endpoint = f"https://www.metaculus.com/api2/questions/{market_id}"
    headers = {"Authorization": f"Token {API_KEY}"}
    response = requests.get(endpoint, headers=headers, verify=certifi.where())
    if not response.ok:
        logger.error(
            f"Request to market endpoint failed for {market_id}: {response.status_code} Error. "
            f"{response.text}"
        )
        response.raise_for_status()
    return response.json()


def _update_questions_and_resolved_values(dfq, dff):
    """Update the dataframes that hold the questions and the resolution values.

    dfq: Metaculus questions in the question bank
    dff: Today's fetched markets
    """
    TODAY = pd.Timestamp(dates.get_datetime_today_midnight()).normalize()

    def _get_resolution_entry(market_id, utc_datetime_str, value):
        return {
            "id": market_id,
            "datetime": utc_datetime_str,
            "value": value,
        }

    def _entry_exists_for_today(resolution_values, utc_date_str):
        return resolution_values["datetime"].str.startswith(utc_date_str).any()

    def _extract_probability(market):
        """Parse the forecasts for the community prediction presented on Metaculus.

        Modifying the API data here because it's too much to keep in git and we can always backout
        the Metaculus forecasts using the API if there's an error here.
        """
        market_value = market["community_prediction"]["full"]
        return market_value.get("q2") if isinstance(market_value, dict) else np.nan

    def _get_resolved_market_value(market):
        """Get the market value based on the resolution.

        A market that has resolved should return the resolved value. The possible values for
        market["resolution"] and the associated return values are:
        * 0.0 (i.e. No) -> 0
        * 1.0 (i.e. Yes) -> 1
        * -1.0 (i.e. Ambiguous) -> NaN
        * -2.0 (i.e. Annulled) -> NaN

        A market that hasn't resolved returns the current market probability. This includes closed markets.
        """
        return int(market["resolution"]) if market["resolution"] > 0 else np.nan

    def _create_resolution_file(dfq, index, market):

        basename = f"{market['id']}.jsonl"
        remote_filename = f"{source}/{basename}"
        local_filename = "/tmp/tmp.jsonl"
        df = pd.DataFrame(
            [
                {
                    "datetime": dates.convert_zulu_to_iso(forecast["time"]),
                    "value": forecast["raw"],
                }
                for forecast in market.get("simplified_history", {}).get("community_prediction", {})
            ]
        )
        if df.empty:
            return pd.DataFrame(columns=constants.RESOLUTION_FILE_COLUMNS)

        df["datetime"] = pd.to_datetime(df["datetime"]) + pd.DateOffset(days=1)
        df = df.sort_values(by="datetime")

        if dfq.at[index, "resolved"]:
            # If the market has been resolved, add the market value and resolution datetime
            resolved_datetime = pd.to_datetime(dfq.at[index, "resolution_datetime"])
            df = df[df["datetime"] <= resolved_datetime]
            df.loc[len(df)] = {
                "datetime": resolved_datetime,
                "value": _get_resolved_market_value(market),
            }
        else:
            # Add a value for today if not present
            last_date_in_df = df["datetime"].max()
            if TODAY > last_date_in_df:
                df.loc[len(df)] = {
                    "datetime": TODAY,
                    "value": df["value"].iloc[-1],
                }

        df.set_index("datetime", inplace=True)
        df = df.resample("D").last().ffill()
        df = df.reset_index(names="datetime")

        df["id"] = market["id"]
        df["datetime"] = df["datetime"].apply(lambda x: x.isoformat())
        df = df[["id", "datetime", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

        # Save and Upload
        df.to_json(local_filename, orient="records", lines=True, date_format="iso")
        gcp.storage.upload(
            bucket_name=constants.BUCKET_NAME,
            local_filename=local_filename,
            filename=remote_filename,
        )

        return df

    def _assign_market_values_to_df(df, index, market):
        df.at[index, "question"] = market["title"] if "title" in market else market["title_short"]
        df.at[index, "background"] = market.get("description", "N/A")
        df.at[index, "source_resolution_criteria"] = market.get("resolution_criteria", "N/A")
        df.at[index, "begin_datetime"] = (
            dates.convert_zulu_to_iso(market["publish_time"]) if "publish_time" in market else "N/A"
        )
        df.at[index, "close_datetime"] = (
            dates.convert_zulu_to_iso(market["close_time"]) if "close_time" in market else "N/A"
        )
        df.at[index, "url"] = "https://www.metaculus.com" + market["page_url"]
        if market["active_state"] == "RESOLVED":
            df.at[index, "resolved"] = True
            df.at[index, "resolution_datetime"] = dates.convert_zulu_to_iso(market["resolve_time"])
        df.at[index, "continual_resolution"] = False
        df.at[index, "forecast_horizons"] = (
            data_utils.get_horizons(dates.convert_zulu_to_datetime(market["close_time"]))
            if "close_time" in market
            else constants.FORECAST_HORIZONS_IN_DAYS
        )
        return df

    # Find rows in dff not in dfq: These are the new markets to add to dfq
    col_to_append = dff[~dff["id"].isin(dfq["id"])]["id"]

    # Set all non-id columns to `None` for the new markets
    df_ids_to_append = pd.DataFrame(col_to_append).assign(
        **{col: None for col in dfq.columns if col != "id"}
    )
    df_ids_to_append["resolved"] = False
    df_ids_to_append["resolution_datetime"] = "N/A"

    # Limit the number of new questions to avoid rate limit
    max_to_add = QUESTION_LIMIT - len(dfq[dfq["resolved"] == False])  # noqa: E712
    if max_to_add > 0:
        df_ids_to_append = df_ids_to_append.head(max_to_add)
        dfq = pd.concat([dfq, df_ids_to_append], ignore_index=True)

    # Update all unresolved questions in dfq. Update resolved, resolution_datetime, and background.
    # Recreate all rows of resolution files for unresolved questions
    for index, row in dfq[dfq["resolved"] == False].iterrows():  # noqa: E712
        market = _get_market(row["id"])
        dfq = _assign_market_values_to_df(dfq, index, market)
        _create_resolution_file(dfq, index, market)

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
