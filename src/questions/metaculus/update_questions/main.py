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
from helpers import (  # noqa: E402
    constants,
    data_utils,
    dates,
    decorator,
    env,
    keys,
    metaculus,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "metaculus"
filenames = data_utils.generate_filenames(source=source)

# The Metaculus rate limit is 1,000 queries per hour, so we limit the number of questions we use
# to 1,000 - number of queries executed by the `fetch` function.
# Edit: bump limit to 10k as it seems Metaculus has removed the limit. Maintain code for a while to
#       in case it is reinstated.
QUESTION_LIMIT = 10000 - (len(metaculus.CATEGORIES) + 1)
N_API_CALLS = 0

# The maximum timestamp pandas can handle
# Set this as the max possible date, even when questions resolve later:
# * e.g. https://www.metaculus.com/questions/1535/
MAX_PANDAS_TS = pd.Timestamp.max.tz_localize("UTC")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    on_backoff=data_utils.print_error_info_handler,
)
def _get_market(market_id):
    """Get the market description and resolution criteria for the specified market."""
    global N_API_CALLS
    N_API_CALLS += 1
    logger.info(f"Calling market endpoint for {market_id}. This is API call number {N_API_CALLS}.")
    endpoint = f"https://www.metaculus.com/api/posts/{market_id}"
    headers = {"Authorization": f"Token {keys.API_KEY_METACULUS}"}
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
        resolution = market["question"]["resolution"].lower()
        assert resolution in {
            "yes",
            "no",
            "ambiguous",
            "annulled",
        }, f"Assertion failed: Problem getting resolution value for market {market['id']}"

        if resolution in ["yes", "no"]:
            return 1 if market["question"]["resolution"].lower() == "yes" else 0
        return np.nan

    def _create_resolution_file(dfq, index, market):
        """Write the resolution file for the market.

        Overwrite the resolution file entirely every day.
        """
        df = pd.DataFrame(
            [
                {
                    "start_datetime": dates.convert_epoch_time_in_sec_to_datetime(
                        forecast["start_time"]
                    ),
                    "end_datetime": (
                        min(
                            pd.Timestamp(
                                dates.convert_epoch_time_in_sec_to_datetime(forecast["end_time"])
                            ),
                            MAX_PANDAS_TS,
                        ).to_pydatetime()
                        if forecast["end_time"] is not None
                        else dates.get_datetime_today()
                    ),
                    "value": forecast["centers"][0],
                }
                for forecast in market.get("question", {})
                .get("aggregations", {})
                .get("recency_weighted", {})
                .get("history", {})
            ]
        )

        if df.empty:
            # No one has forecast on the market yet
            return None

        # Remove all rows where the start date is the same as the end date, except when the end date is
        # the last millisecond of the day (in which case the value is the valid last value of the day).
        # This effectively removes all dates where this is the first day of forecasting since the start
        # date and end date would be the same.
        def is_last_millisecond_of_day(dt):
            return (
                dt.time()
                == pd.Timestamp(dt.date())
                .replace(hour=23, minute=59, second=59, microsecond=999999)
                .time()
            )

        df = df[
            ~(
                (df["start_datetime"].dt.date == df["end_datetime"].dt.date)
                & ~df["end_datetime"].apply(is_last_millisecond_of_day)
            )
        ]

        if df.empty:
            # All forecasts are from today
            return None

        # It should already be sorted but it doesn't hurt to ensure that's the case
        df = df.sort_values(by="end_datetime", ignore_index=True)

        # Set the date to be the end_datetime as a date - 1 day, as we're capturing the last value
        # of the day. Do NOT subtract a day if the end_datetime is the last millisecond of the day
        # (low probability but need to check), as then that's the last value of the day.
        def set_date(end_datetime):
            end_date = end_datetime.date()
            return (
                end_date
                if end_datetime.time()
                == pd.Timestamp(end_date)
                .replace(hour=23, minute=59, second=59, microsecond=999999)
                .time()
                else end_date - pd.Timedelta(days=1)
            )

        df["date"] = df["end_datetime"].apply(set_date)

        # There shouldn't be any duplicates; just doing this in case
        df = df.drop_duplicates(subset="date", keep="last", ignore_index=True)

        # Backfill values. Get every day from the first date to the last and backfill the values.
        date_range = pd.date_range(
            start=df["start_datetime"].min().date(), end=df["date"].max(), freq="D"
        )
        df_dates = pd.DataFrame(date_range, columns=["date"])
        df_dates["date"] = df_dates["date"].dt.date
        df = pd.merge(df_dates, df[["date", "value"]], on="date", how="left")
        df["value"] = df["value"].bfill()

        # If the market has resolved, add the market value and resolution datetime
        if dfq.at[index, "resolved"]:
            resolved_date = pd.Timestamp(dfq.at[index, "market_info_resolution_datetime"]).date()
            df = df[df["date"] < resolved_date]
            df.loc[len(df)] = {
                "date": resolved_date,
                "value": _get_resolved_market_value(market),
            }

        # Prepare for writing
        df["id"] = market["id"]
        df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

        basename = f"{market['id']}.jsonl"
        remote_filename = f"{source}/{basename}"
        local_filename = f"/tmp/{market['id']}.jsonl"
        df.to_json(local_filename, orient="records", lines=True, date_format="iso")

        # Upload
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
            filename=remote_filename,
        )

        # Return the last market value for the series
        return df["value"].iloc[-1]

    def _assign_market_values_to_df(df, index, market):
        df.at[index, "question"] = market["title"]
        df.at[index, "background"] = market["question"].get("description", "N/A")
        df.at[index, "market_info_resolution_criteria"] = market["question"].get(
            "resolution_criteria", "N/A"
        )
        df.at[index, "market_info_open_datetime"] = dates.convert_zulu_to_iso(
            market["question"]["open_time"]
        )
        df.at[index, "market_info_close_datetime"] = dates.convert_zulu_to_iso(
            market["question"]["actual_close_time"]
        )
        df.at[index, "url"] = f"https://www.metaculus.com/questions/{market['id']}"
        if market["resolved"]:
            df.at[index, "resolved"] = True
            df.at[index, "market_info_resolution_datetime"] = dates.convert_datetime_to_iso(
                min(
                    dates.convert_zulu_to_datetime(market["question"]["actual_close_time"]),
                    dates.convert_zulu_to_datetime(market["question"]["actual_resolve_time"]),
                )
            )
        df.at[index, "forecast_horizons"] = "N/A"
        return df

    # Find rows in dff not in dfq: These are the new markets to add to dfq
    col_to_append = dff[~dff["id"].isin(dfq["id"])]["id"]

    # Set all non-id columns to `None` for the new markets
    df_ids_to_append = pd.DataFrame(col_to_append).assign(
        **{col: None for col in dfq.columns if col != "id"}
    )
    df_ids_to_append["resolved"] = False
    df_ids_to_append["freeze_datetime_value_explanation"] = "The community prediction."
    df_ids_to_append["market_info_resolution_datetime"] = "N/A"

    # Limit the number of new questions to avoid rate limit
    max_to_add = QUESTION_LIMIT - len(dfq[dfq["resolved"] == False])  # noqa: E712
    if max_to_add > 0:
        df_ids_to_append = df_ids_to_append.head(max_to_add)
        dfq = pd.concat([dfq, df_ids_to_append], ignore_index=True)

    # Update all unresolved questions in dfq. Update resolved, resolution_datetime, and background.
    # Recreate all rows of resolution files for unresolved questions
    dfq["resolved"] = dfq["resolved"].astype(bool)
    for index, row in dfq[~dfq["resolved"]].iterrows():
        market = _get_market(row["id"])
        dfq = _assign_market_values_to_df(dfq, index, market)
        last_val = _create_resolution_file(dfq, index, market)
        dfq.at[index, "freeze_datetime_value"] = last_val if last_val else "N/A"

    # Save and upload
    # Upload dfq before checking resolved questions in case we hit rate limit
    data_utils.upload_questions(dfq, source)

    for index, row in dfq[dfq["resolved"]].iterrows():
        # Regenerate resolution files in case they've been deleted
        resolved_files = gcp.storage.list_with_prefix(
            bucket_name=env.QUESTION_BANK_BUCKET, prefix=source
        )
        filename = f"{row['id']}.jsonl"
        if filename not in resolved_files:
            market = _get_market(row["id"])
            _create_resolution_file(dfq, index, market)


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
    _update_questions_and_resolved_values(dfq, dff)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
