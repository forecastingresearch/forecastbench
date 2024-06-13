"""Yfinance update question script."""

import logging
import os
import sys
from datetime import timedelta

import pandas as pd
import yfinance as yf

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"


def fetch_one_stock(ticker, day_diff):
    """
    Fetch historical stock price data for a given ticker over a specified number of days.

    Retrieve the closing prices of a stock from the Yahoo Finance API, spanning the number of days
    specified by 'day_diff' counting backwards from today. The function handles any exceptions
    during the data retrieval process and returns an empty DataFrame if the data fetch fails.

    Parameters:
    - ticker (str): The stock symbol for which to retrieve price data.
    - day_diff (int): The number of days from today for which to retrieve historical data.

    Returns:
    - DataFrame: A pandas DataFrame containing the historical closing prices of the stock with
      columns 'date' and 'value', where 'date' is the date of the closing price and
      'value' is the closing price itself. If the data fetch fails, returns an empty DataFrame.
    """
    try:
        ticker = yf.Ticker(ticker)
        hist = ticker.history(period=f"{day_diff}d")
        return hist[["Close"]].reset_index().rename(columns={"Date": "date", "Close": "value"})
    except Exception as e:
        print(f"Failed to fetch data for {ticker}: {e}")
        return pd.DataFrame()


def get_historical_prices(current_df, ticker):
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

    Returns:
    - DataFrame: A pandas DataFrame updated with daily historical stock prices from the most recent
      date in 'current_df' to the present day. The DataFrame is sorted by 'date' and includes
      the columns ['id', 'date', 'value'].
    """
    current_time = dates.get_date_today()
    yesterday = current_time - timedelta(days=1)
    if current_df.empty:
        # If the dataframe is empty, only fetch last date's stock price
        current_df = pd.DataFrame(columns=["id", "date", "value"])
        hist_data = fetch_one_stock(ticker, 5).reset_index()
        hist_data["date"] = pd.to_datetime(hist_data["date"])
        hist_data = hist_data[hist_data["date"].dt.date <= yesterday].tail(1)
    else:
        # If not empty, fetch all stock prices up until yesterday's
        last_date = pd.to_datetime(current_df["date"]).max().date()
        day_diff = (current_time - last_date).days
        hist_data = fetch_one_stock(ticker, day_diff)

    hist_data["date"] = pd.to_datetime(hist_data["date"]).dt.date
    if not current_df.empty:
        current_df["date"] = pd.to_datetime(current_df["date"]).dt.date

    # Check if there is any data to process
    if not current_df.empty or (not hist_data.empty and hist_data["date"].iloc[0] < yesterday):
        # Concatenate and handle duplicates based on 'date' only if there's current data to add
        if not current_df.empty:
            all_data = pd.concat([current_df, hist_data])
        else:
            all_data = hist_data.copy()

        # Sort and remove duplicates to keep the latest entry per date
        all_data.sort_values(by="date", inplace=True)
        all_data.drop_duplicates(subset=["date"], keep="last", inplace=True)

        # Reindex to fill in missing dates including weekends
        all_dates = pd.date_range(start=all_data["date"].min(), end=yesterday, freq="D")
        all_data = all_data.set_index("date").reindex(all_dates, method="ffill").reset_index()

        # Ensure the reset index is named 'date' and convert to datetime.date if not already
        if current_df.empty:
            all_data.rename(columns={"level_0": "date"}, inplace=True)
        else:
            all_data.rename(columns={"index": "date"}, inplace=True)
            all_data["date"] = all_data["date"].dt.date
    else:
        all_data = hist_data.copy()

    all_data["id"] = ticker
    all_data.drop_duplicates(subset=["date"], keep="last", inplace=True)

    return all_data[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)


def create_resolution_file(question, get_historical_forecasts_func, source):
    """
    Create or update a resolution file based on the question ID provided. Download the existing file, if any.

    Check the last entry date, and update with new data if there's no entry for today. Upload the updated file
    back to the specified Google Cloud Platform bucket.
    Args:
    - question (dict): A dictionary containing at least the 'id' of the question.
    - get_historical_forecasts_func (function, optional): A function to retrieve historical forecasts.
      Defaults to `get_historical_prices`.
    - source (str): The source directory path within the bucket where files are stored.
    Returns:
    - DataFrame: Return the current state of the resolution file as a DataFrame if no update is needed.
      If an update occurs, the function returns None after uploading the updated file.
    """
    basename = f"{question['id']}.jsonl"
    remote_filename = f"{source}/{basename}"
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
        # If we have it alreeady, return to avoid unnecessary API calls
        return df

    df = get_historical_forecasts_func(df, question["id"])

    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
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
    - dff  (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    dff_list = dff.to_dict("records")
    for question in dff_list:
        create_resolution_file(question, get_historical_prices, SOURCE)

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
