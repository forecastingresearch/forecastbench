"""Yfinance fetch new questions script."""

import json
import logging
import os
import sys

import pandas as pd
import yfinance as yf
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Project_ID = os.environ.get("CLOUD_PROJECT")
SOURCE = "yfinance"
BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")


def fetch_one_stock(ticker):
    """
    Fetch the company name and the latest historical stock data for a given stock ticker.

    This function attempts to retrieve the company name and its most recent stock data for
    the specified ticker using the yfinance library. It returns the company name and a
    pandas DataFrame containing the historical stock data. If the ticker is invalid or
    data retrieval fails, it returns None for both.

    Parameters:
    - ticker (str): The stock ticker for which to fetch the data.

    Returns:
    - tuple: A tuple containing the company name (str) and the historical stock data
    (pandas.DataFrame) for the given ticker. Returns (None, None) if unable to fetch data.
    """
    try:
        ticker = yf.Ticker(ticker)
        company_name = ticker.info["longName"]
        hist = ticker.history(period="1d")
        return company_name, hist

    except Exception:
        return None, None


def fetch_all_stock():
    """
    Fetch and compile stock information for a list of stock tickers.

    Iterates over a provided list of stock tickers, fetching the company name and the most
    recent stock data for each. Constructs a detailed output string for each stock ticker,
    along with additional metadata, and appends it to a list. This list is returned at the
    end of the function. If data for a specific stock cannot be fetched, it is skipped.

    Parameters:
    - tickers (list): A list of stock tickers (str) for which to fetch the data.

    Returns:
    - list: A list of dictionaries. Each dictionary contains detailed information and
    metadata about a stock, including its current price, if available.
    """
    stock_list = []
    for ticker in tqdm(constants.TOP_100_STOCK_TICKERS, desc="Fetching stock data"):
        company_name, hist = fetch_one_stock(ticker)
        current_time = (dates.get_datetime_now(),)

        if company_name and not hist.empty:
            current_price = round(hist["Close"].iloc[-1], 2)

            stock_list.append(
                {
                    "id": ticker,
                    "background": "N/A",
                    "source_resolution_criteria": "N/A",
                    "begin_datetime": "N/A",
                    "close_datetime": "N/A",
                    "url": "https://finance.yahoo.com/quote/" + ticker + "?guccounter=1",
                    "resolved": "N/A",
                    "resolution_datetime": "N/A",
                    "fetch_datetime": current_time,
                    "probability": current_price,
                }
            )

    return pd.DataFrame(stock_list)


def driver(_):
    """Fetch all stock and then upload to gcp."""
    all_stock = fetch_all_stock()
    filenames = data_utils.generate_filenames(SOURCE)

    # Save and upload
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in all_stock.to_dict("records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")

    logger.info("Uploading to GCP...")
    # Upload
    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=filenames["local_fetch"],
    )
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
