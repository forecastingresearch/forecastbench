"""Yfinance update question script."""

import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"

benchmark_start_date = datetime.strptime(constants.BENCHMARK_START_DATE, "%Y-%m-%d").date()


def select_time_range(days_difference):
    """
    Select the appropriate time range based on days_difference.

    Possible time ranges in:
    ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    """
    if days_difference <= 1:
        return "1d"
    elif days_difference <= 5:
        return "5d"
    elif days_difference <= 30:
        return "1mo"
    elif days_difference <= 90:
        return "3mo"
    elif days_difference <= 180:
        return "6mo"
    elif days_difference <= 365:
        return "1y"
    elif days_difference <= 365 * 2:
        return "2y"
    elif days_difference <= 365 * 5:
        return "5y"
    elif days_difference <= 365 * 10:
        return "10y"
    else:
        return "max"


def fetch_one_stock(ticker, period):
    """
    Fetch historical stock price data for a given ticker.

    Retrieve the closing prices of a stock from the Yahoo Finance API. The function handles any
    exceptions during the data retrieval process and returns an empty DataFrame if the data fetch
    fails.

    Parameters:
    - ticker (str): The stock symbol for which to retrieve price data.
    - period (str): One of: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

    Returns:
    - DataFrame: A pandas DataFrame containing the historical closing prices of the stock with
      columns 'date' and 'value', where 'date' is the date of the closing price and
      'value' is the closing price itself. If the data fetch fails, returns an empty DataFrame.
    """
    try:
        ticker = yf.Ticker(ticker)
        hist = ticker.history(period=period)
        return hist[["Close"]].reset_index().rename(columns={"Date": "date", "Close": "value"})
    except Exception as e:
        logger.error(f"Failed to fetch data for {ticker}: {e}")
        return pd.DataFrame()


def get_historical_prices(current_df, ticker, period):
    """
    Update a DataFrame with the latest historical stock prices for a given ticker.

    Determine the period to fetch based on the most recent date in the provided DataFrame and fill
    any missing days, including weekends and holidays, by carrying forward the last available price.
    If the input DataFrame is empty, initialize it and fetch data for at least the last day. The
    resulting DataFrame includes a complete series of dates and corresponding stock prices,
    indexed daily from the earliest date available in the input or from the last day if the DataFrame
    was initially empty.

    Parameters:
    - current_df (DataFrame): A pandas DataFrame containing columns ['id', 'date', 'value'],
      where 'id' is the stock ticker, 'date' is the date, and 'value' is the stock price.
    - ticker (str): The stock symbol for which to retrieve and update prices.
    - period (str): One of: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

    Returns:
    - DataFrame: A pandas DataFrame updated with daily historical stock prices from the most recent
      date in 'current_df' to the present day. The DataFrame is sorted by 'date' and includes
      the columns ['id', 'date', 'value'].
    """
    if current_df.empty:
        current_df = pd.DataFrame(columns=constants.RESOLUTION_FILE_COLUMNS)

    df = fetch_one_stock(ticker, period)
    if df.empty:
        return current_df

    yesterday = dates.get_date_today() - timedelta(days=1)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["date"] >= benchmark_start_date) & (df["date"] <= yesterday)]

    # forward fill for weekends
    full_date_range = pd.date_range(start=df["date"].min(), end=yesterday)
    df = df.set_index("date").reindex(full_date_range).ffill().rename_axis("date").reset_index()
    df["id"] = ticker
    return df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)


def create_resolution_file(question, period):
    """
    Create or update a resolution file based on the question ID provided. Download the existing file, if any.

    Check the last entry date, and update with new data if there's no entry for today. Upload the updated file
    back to the specified Google Cloud Platform bucket.
    Parameters:
    - question (dict): A dictionary containing at least the 'id' of the question.
    - period (str): One of: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

    Returns:
    - DataFrame: Return the current state of the resolution file as a DataFrame if no update is needed.
      If an update occurs, the function returns None after uploading the updated file.
    """
    basename = f"{question['id']}.jsonl"
    remote_filename = f"{SOURCE}/{basename}"
    local_filename = "/tmp/tmp.jsonl"

    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=remote_filename,
        local_filename=local_filename,
    )
    df = pd.read_json(
        local_filename,
        lines=True,
        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
        convert_dates=False,
    )

    yesterday = dates.get_date_today() - timedelta(days=1)
    if not df.empty and pd.to_datetime(df["date"].iloc[-1]).date() >= yesterday:
        logger.info(f"{question['id']} is skipped because it's already up-to-date!")
        # Check last date to see if we've already gotten the resolution value for today
        # If we have it already, return to avoid unnecessary API calls
        return

    df_new = get_historical_prices(df, question["id"], period)
    if not df.equals(df_new):
        # Only upload dataframes that changed.
        logger.info(f"Uploading resolution file for {question['id']}")
        df_new.to_json(local_filename, orient="records", lines=True, date_format="iso")
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
            filename=remote_filename,
        )


def update_questions(dfq, dff):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dff (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    dff_list = dff.to_dict("records")
    day_diff = (dates.get_date_today() - benchmark_start_date).days
    period = select_time_range(day_diff)

    for question in dff_list:
        create_resolution_file(question, period)

        del question["fetch_datetime"]
        del question["probability"]

        # Check if the question exists in dfq
        if question["id"] in dfq["id"].values:
            # Case 1: Update existing question
            dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
            for key, value in question.items():
                dfq.at[dfq_index, key] = value
        else:
            # Case 2: Append new question
            new_q_row = pd.DataFrame([question])
            new_q_row = new_q_row.astype(constants.QUESTION_FILE_COLUMN_DTYPE)
            dfq = pd.concat([dfq, new_q_row], ignore_index=True)

    return dfq


@decorator.log_runtime
def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    # Update the existing questions
    dfq = update_questions(dfq, dff)

    logger.info("Uploading to GCP...")
    # Save and upload
    data_utils.upload_questions(dfq, SOURCE)
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
