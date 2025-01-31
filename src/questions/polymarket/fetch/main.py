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
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "polymarket"

MIN_MARKET_LIQUIDITY = 25000


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
    fetch_datetime = dates.get_datetime_now()
    endpoint = "https://gamma-api.polymarket.com/markets"
    offset = 0
    limit = 500  # max page size: 500
    params = {
        "limit": limit,
        "archived": False,
        "active": True,
        "closed": False,
        "order": "liquidity",
        "ascending": False,
    }
    n_markets_fetched = 0

    def get_market(condition_id):
        """Return a market given the condition id."""
        params_market = {
            "condition_ids": condition_id,
        }
        response = requests.get(endpoint, params=params_market)
        response.raise_for_status()
        markets = response.json()
        if len(markets) != 1:
            message = f"Problem getting market for condition id {condition_id}."
            logger.error(message)
            raise ValueError(message)
        return markets[0]

    def get_yes_index(market):
        """Return the index associated with a "Yes" bid."""
        return 0 if json.loads(market["outcomes"])[0].lower() == "yes" else 1

    def get_yes_token(market):
        """Return the index of the token associated with a "Yes" bid."""
        yes_token_index = get_yes_index(market)
        yes_token = json.loads(market["clobTokenIds"])[yes_token_index]
        return yes_token

    def is_market_binary(market):
        """Return true if this is a binary market."""
        return {s.lower() for s in json.loads(market["outcomes"])} == {"yes", "no"}

    while True:
        params["offset"] = offset
        try:
            logger.info(f"Fetching markets with offset {offset}.")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            markets = response.json()
            if not markets:
                logger.info(
                    f"Fetched total of {n_markets_fetched} markets, "
                    f"{len(all_new_questions)} satisfy criteria."
                )
                break

            n_markets_fetched += len(markets)
            for market in markets:
                binary_market = is_market_binary(market=market)
                # Avoids questions like the following, which don't make sense without the other
                # questions in the event:
                # * Will any other Republican Politician win the popular vote in the 2024
                #   Presidential Election?
                catch_all_market = "other" in market["slug"]  # no need to test "another" also
                liquid_market = (
                    "liquidityNum" in market.keys()
                    and market["liquidityNum"] > MIN_MARKET_LIQUIDITY
                )
                if binary_market and liquid_market and not catch_all_market:
                    price_history = fetch_price_history(market_id=get_yes_token(market=market))
                    if price_history is not None:
                        logger.info(
                            "Binary question satisfying criteria: https://polymarket.com/market/"
                            f"{market['slug']}"
                        )
                        market["price_history"] = price_history
                        all_new_questions.append(market)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching markets: {e}")
            break

        time.sleep(1)
        offset += limit

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

    all_newly_fetched_ids = {q["conditionId"] for q in all_new_questions}
    logger.info(f"Number of newly fetched questions: {len(all_newly_fetched_ids)}")

    unresolved_ids.difference_update(all_newly_fetched_ids)
    unresolved_ids = list(unresolved_ids)
    logger.info(f"Total (removing duplicates): {len(unresolved_ids)}")

    all_existing_unresolved_questions = []
    invalid_question_ids = set()
    for id_ in unresolved_ids:
        time.sleep(0.1)
        q = get_market(condition_id=id_)
        if not is_market_binary(q):
            # Questions that were not Yes/No questions should be marked as resolved/closed so
            # they're not selected in question sets.
            invalid_question_ids.add(q["conditionId"])
            q["closed"] = True

        price_history = fetch_price_history(market_id=get_yes_token(market=q))
        if price_history is None:
            logger.error(f"PRICE HISTORY was NONE for {q['slug']}")
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
            if q["conditionId"] in invalid_question_ids:
                for item in q["price_history"]:
                    item["p"] = np.nan

    all_questions_to_add = all_new_questions + all_existing_unresolved_questions
    all_questions_to_add = [q for q in all_questions_to_add if q["price_history"]]

    all_complete_questions = []
    for q in tqdm(all_questions_to_add, "Compiling questions."):
        logger.info(f"Adding {q['conditionId']}")
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

        current_prob = price_history[-1]["p"] if len(price_history) > 1 else np.nan
        resolved_datetime = resolved_datetime_str = "N/A"

        end_date = q["endDate"] if "endDate" in q else q["events"][0]["endDate"]
        market_closed_datetime_str = dates.convert_zulu_to_iso(end_date)
        market_closed_datetime = datetime.fromisoformat(market_closed_datetime_str).replace(
            tzinfo=None
        )

        use_uma_date = False
        if q.get("umaEndDate"):
            # UMA Oracle
            uma_datetime_str = dates.convert_zulu_to_iso(q["umaEndDate"])
            uma_datetime = datetime.fromisoformat(uma_datetime_str).replace(tzinfo=None)
            use_uma_date = uma_datetime < market_closed_datetime

        resolved_datetime_str = uma_datetime_str if use_uma_date else market_closed_datetime_str
        resolved_datetime = uma_datetime if use_uma_date else market_closed_datetime
        resolved_datetime = resolved_datetime.replace(hour=0, minute=0, second=0)

        # Get the resolution if the question is closed (but not if it's invalid so we maintain the
        # NaN values above)
        resolved = q.get("umaResolutionStatus", "") == "resolved"
        if resolved and not q["conditionId"] in invalid_question_ids:
            yes_index = get_yes_index(q)
            current_prob = float(json.loads(q["outcomePrices"])[yes_index])

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
                "id": q["conditionId"],
                "question": q["question"],
                "background": q["description"],
                "market_info_resolution_criteria": "N/A",
                "market_info_open_datetime": q["startDateIso"],
                "market_info_close_datetime": market_closed_datetime_str,
                "url": "https://polymarket.com/market/" + q["slug"],
                "resolved": resolved,
                "market_info_resolution_datetime": resolved_datetime_str,
                "fetch_datetime": fetch_datetime,
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
