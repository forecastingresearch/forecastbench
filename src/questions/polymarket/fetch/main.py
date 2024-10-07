"""POLYMARKET fetch new questions script."""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import backoff
import certifi
import numpy as np
import pandas as pd
import requests
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator, env, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "polymarket"


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=20,
    on_backoff=data_utils.print_error_info_handler,
)
def fetch_price_history(market_id):
    """
    Retrieve the price history of a market from the Polymarket API.

    Note: polymarket API seem to only provide the history up to the last 6 month.

    Args:
    market_id (str): The unique identifier of the market.

    Returns:
    list: A list of dictionaries containing the price history data, or an empty list
          if the data retrieval fails.
    """
    time.sleep(0.1)
    logger.info(f"Getting price history for {market_id}...")

    endpoint = "https://clob.polymarket.com/prices-history"

    params = {
        "interval": "max",
        "market": market_id,
        "fidelity": 1440,
        "startTs": constants.BENCHMARK_START_DATE_EPOCHTIME,
    }

    try:
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(
                f"Request to endpoint failed for {endpoint}: {response.status_code} {response.reason}. "
                f"Headers: {response.headers}. "
                f"Elapsed time: {response.elapsed}."
            )
            response.raise_for_status()

        data = response.json()
        history_data = data.get("history", [])
        return history_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch price history for market {market_id}: {e}")
        return None


def filter_first_midnight_only(price_history):
    """Remove duplicate dates and keep only the first value for each day."""
    unique_dates = {}
    for record in price_history:
        date_only = record["date"].split("T")[0]  # Extract the date part (YYYY-MM-DD)
        if date_only not in unique_dates:
            unique_dates[date_only] = record  # Keep the first occurrence of the date
    return list(unique_dates.values())


def subtract_one_day(price_history):
    """Subtract one day from all dates."""
    for record in price_history:
        record_datetime = datetime.fromisoformat(record["date"])
        record_datetime -= timedelta(days=1)
        record["date"] = record_datetime.isoformat()
    return price_history


def fetch_all_questions(dfq):
    """
    Fetch all questions.

    Combine new questions from the API with unresolved questions
    from existing data. It filters out questions that have less than 10 price history points, and
    formats the data for further processing. It ensures no duplicate questions are fetched if they
    are already marked as unresolved in the current dataset.

    Parameters:
    - dfq (DataFrame): A pandas DataFrame containing the current unresolved questions with
    columns "id" and "resolved".

    Returns:
    - List[Dict]: A list of dictionaries with details of all unresolved questions, including
    their resolutions.
    """
    all_new_questions = []
    today = dates.get_date_today()
    next_cursor = None
    page_cnt = 1
    drop_cnt = 0
    bet_cnt = 725
    closed_cnt = 0
    client = ClobClient(
        "https://clob.polymarket.com", key=keys.API_KEY_POLYMARKET, chain_id=POLYGON
    )
    while True:
        time.sleep(0.1)
        resp = client.get_markets(next_cursor=next_cursor) if next_cursor else client.get_markets()
        new_questions = []
        for q in resp["data"]:
            closed_cnt += 1
            if not q["closed"] and not q["archived"] and q["active"]:
                closed_cnt -= 1
                outcomes = {token["outcome"] for token in q["tokens"]}
                if len(outcomes) == 2 and outcomes == {"Yes", "No"}:
                    yes_token_index = 0 if q["tokens"][0]["outcome"] == "Yes" else 1
                    # If there are no bets, fetch_price_history will get 400 status error
                    if q["tokens"][yes_token_index]["token_id"]:
                        price_history = fetch_price_history(
                            q["tokens"][yes_token_index]["token_id"]
                        )
                        if price_history is not None and len(price_history) >= bet_cnt:
                            # only save questions with at least `bet_cnt` predictions
                            logger.info(
                                "Binary question satisfying criteria: "
                                f"https://polymarket.com/market/{q['market_slug']}"
                            )
                            q["price_history"] = price_history
                            new_questions.append(q)
                        else:
                            drop_cnt += 1
                    else:
                        drop_cnt += 1
                else:
                    drop_cnt += 1

        all_new_questions.extend(new_questions)
        next_cursor = resp.get("next_cursor", None)

        # Check if there are no more pages of data
        if next_cursor is None or next_cursor == "LTE=":
            logger.info("No more pages to fetch.")
            break

        logger.info(
            f"Current page is {page_cnt:,}."
            + f" Current question count is {len(all_new_questions):,}."
            + f" Current drop count is {drop_cnt:,}."
            + f" Current closed market count is {closed_cnt:,}."
        )
        page_cnt += 1

    resolved_ids = set(dfq.loc[dfq["resolved"], "id"]) if not dfq.empty else set()
    unresolved_ids = set(dfq.loc[~dfq["resolved"], "id"]) if not dfq.empty else set()

    # Check all data is complete for resolved questions
    # * Download resolution file
    # * Check that data exists for every day between first date and the resolution date
    # * If it doesn't exist, add to unresloved_ids to fetch market info again
    dfr_tmp = pd.DataFrame(columns=constants.RESOLUTION_FILE_COLUMNS)
    for mid in resolved_ids:
        dfr = data_utils.download_and_read(
            filename=f"{SOURCE}/{mid}.jsonl",
            local_filename="/tmp/tmp.jsonl",
            df_tmp=dfr_tmp,
            dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
        )
        dfr["date"] = pd.to_datetime(dfr["date"])
        dfr = dfr.sort_values(by="date")
        date_diff = dfr["date"].diff().dt.days
        contiguous_dates = date_diff.iloc[1:].eq(1).all()
        if not contiguous_dates:
            unresolved_ids.add(mid)

    logger.info(f"Number of unresolved questions in dfq: {len(unresolved_ids)}")

    all_newly_fetched_ids = {q["condition_id"] for q in all_new_questions}
    logger.info(f"Number of newly fetched questions: {len(all_newly_fetched_ids)}")

    unresolved_ids.difference_update(all_newly_fetched_ids)
    unresolved_ids = list(unresolved_ids)
    logger.info(f"Total (removing duplicates): {len(unresolved_ids)}")

    all_existing_unresolved_questions = []
    invalid_question_ids = set()
    for id_ in unresolved_ids:
        time.sleep(0.1)
        q = client.get_market(condition_id=id_)
        outcomes = {token["outcome"] for token in q["tokens"]}
        if not (len(outcomes) == 2 and outcomes == {"Yes", "No"}):
            # Questions that were not Yes/No questions should be marked as resolved/closed so
            # they're not selected in question sets.
            invalid_question_ids.add(q["condition_id"])
            q["closed"] = True

        yes_token_index = 0 if q["tokens"][0]["outcome"] == "Yes" else 1
        price_history = fetch_price_history(q["tokens"][yes_token_index]["token_id"])

        if price_history is None:
            logger.error(f"PRICE HISTORY was NONE for {q['market_slug']}")
            # Add dummy entry with NaN value for last possible date so we don't accidentally
            # resolve these qusetions later or pull from them for the question set. Use today
            # because we remove one day from the converted_price_history in the next loop.
            price_history = [{"p": np.nan, "t": dates.convert_iso_date_to_epoch_time(today)}]

        q["price_history"] = price_history
        all_existing_unresolved_questions.append(q)

    logger.info("Finished fetching unresolved questions!")

    # Handle invalid questions.
    # Set all values in the price history to np.nan because it should never be resolved if they
    # were already included in a question set.
    if len(invalid_question_ids) > 0:
        logger.warning(f"Invalid questions found: {invalid_question_ids}")
        for q in all_existing_unresolved_questions:
            if q["condition_id"] in invalid_question_ids:
                for item in q["price_history"]:
                    item["p"] = np.nan

    all_questions_to_add = all_new_questions + all_existing_unresolved_questions
    all_questions_to_add = [q for q in all_questions_to_add if q["price_history"]]

    all_complete_questions = []
    for q in tqdm(all_questions_to_add, "Compiling questions."):
        price_history = q["price_history"]

        final_resolutions_df = pd.DataFrame([], columns=["date", "value"])

        converted_price_history = [
            {"date": dates.convert_epoch_time_in_sec_to_iso(r["t"]), "value": r["p"]}
            for r in price_history
        ]
        converted_price_history = filter_first_midnight_only(converted_price_history)
        converted_price_history = subtract_one_day(converted_price_history)

        final_resolutions_df = pd.DataFrame(converted_price_history)
        final_resolutions_df["date"] = pd.to_datetime(final_resolutions_df["date"].str[:10])

        # Reindex to fill in missing dates including weekends
        all_dates = pd.date_range(
            start=final_resolutions_df["date"].min(),
            end=final_resolutions_df["date"].max(),
            freq="D",
        )
        final_resolutions_df = (
            final_resolutions_df.set_index("date").reindex(all_dates, method="ffill").reset_index()
        )

        final_resolutions_df.rename(columns={"index": "date"}, inplace=True)
        final_resolutions_df = final_resolutions_df[["date", "value"]]
        final_resolutions_df["date"] = pd.to_datetime(final_resolutions_df["date"])

        current_prob = price_history[-1]["p"] if len(price_history) > 1 else 0
        resolved_datetime = resolved_datetime_str = "N/A"
        if q["end_date_iso"]:
            resolved_datetime_str = dates.convert_zulu_to_iso(q["end_date_iso"])
            resolved_datetime = datetime.fromisoformat(resolved_datetime_str).replace(tzinfo=None)

        # Get the resolution if the question is closed (but not if it's invalid so we maintain the
        # NaN values above)
        if q["closed"] and not q["condition_id"] in invalid_question_ids:
            current_prob = 1
            if (q["tokens"][0]["outcome"] == "No" and q["tokens"][0]["winner"]) or (
                q["tokens"][1]["outcome"] == "No" and q["tokens"][1]["winner"]
            ):
                current_prob = 0

            if resolved_datetime == "N/A":
                # For some reason the resolved datetime was not found. In this case, just insert
                # the resolution on the last date
                final_resolutions_df.loc[len(final_resolutions_df)] = [
                    final_resolutions_df["date"].max() + timedelta(days=1),
                    current_prob,
                ]
            else:
                # Insert the resolution value on the resolved date. Truncate all data after that date.
                # Forward fill data until that date

                # Truncate any data after resolved_datetime
                final_resolutions_df = final_resolutions_df[
                    final_resolutions_df["date"].dt.date <= resolved_datetime.date()
                ]

                # Insert resolved date and resolution value
                if resolved_datetime.date() in final_resolutions_df["date"].dt.date.values:
                    final_resolutions_df.loc[
                        final_resolutions_df["date"].dt.date == resolved_datetime.date(), "value"
                    ] = current_prob
                else:
                    # If the date does not exist, add a new row
                    final_resolutions_df.loc[len(final_resolutions_df)] = [
                        resolved_datetime,
                        current_prob,
                    ]

                # Forward fill in case the resolution date is more than one day after the last day
                # for which data is available
                all_dates = pd.date_range(
                    start=final_resolutions_df["date"].min(),
                    end=final_resolutions_df["date"].max(),
                    freq="D",
                )
                final_resolutions_df = (
                    final_resolutions_df.set_index("date")
                    .reindex(all_dates, method="ffill")
                    .reset_index()
                    .rename(columns={"index": "date"})
                )

        final_resolutions_df = final_resolutions_df[["date", "value"]]
        final_resolutions_df["date"] = final_resolutions_df["date"].astype(str)

        all_complete_questions.append(
            {
                "id": q["condition_id"],
                "question": q["question"],
                "background": q["description"],
                "market_info_resolution_criteria": "N/A",
                "market_info_open_datetime": "N/A",
                "market_info_close_datetime": resolved_datetime_str,
                "url": "https://polymarket.com/market/" + q["market_slug"],
                "resolved": q["closed"],
                "market_info_resolution_datetime": resolved_datetime_str,
                "fetch_datetime": dates.get_datetime_now(),
                "probability": "N/A" if np.isnan(current_prob) else current_prob,
                "forecast_horizons": "N/A",
                "freeze_datetime_value": "N/A" if np.isnan(current_prob) else current_prob,
                "freeze_datetime_value_explanation": "The market price.",
                "historical_prices": final_resolutions_df.to_dict(orient="records"),
            }
        )

    logger.info(f"Fetched {len(all_complete_questions)} questions.")

    return pd.DataFrame(all_complete_questions)


@decorator.log_runtime
def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    filenames = data_utils.generate_filenames(SOURCE)

    # Get the latest data
    all_questions_to_add = fetch_all_questions(dfq)

    # Save and upload
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        for record in all_questions_to_add.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")

    # Upload
    logger.info("Uploading to GCP...")
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
