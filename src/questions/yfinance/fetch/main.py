"""Yfinance fetch new questions script."""

import json
import logging
import os
import sys
import time
from datetime import timedelta

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"


def get_sp500_tickers():
    """
    Retrieve the list of S&P 500 index constituent tickers.

    Access the S&P 500 Wikipedia page and parse the HTML content to extract
    the tickers of the constituents. Return a list of the tickers found
    in the designated table on the page.

    Returns:
        list of str: A list containing the tickers of the S&P 500 index constituents.
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        tickers = [
            row.find_all("td")[0].text.strip() for row in table.find_all("tr")[1:]
        ]  # Skip header row
        logger.info(f"Retrieved S&P 500 stock tickers: {tickers}")
        return tickers
    except Exception as e:
        logger.error(f"Failed to retrieve stock tickers due to: {e}")
        return []  # Return an empty list if there's any error


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
        hist = ticker.history(period="5d").reset_index()
        yesterday = dates.get_date_today() - timedelta(days=1)
        hist["Date"] = pd.to_datetime(hist["Date"])
        hist = hist[hist["Date"].dt.date <= yesterday].tail(1)
        return company_name, hist

    except Exception:
        return None, None


def fetch_all_stock(dfq):
    """
    Fetch and compile stock information for a list of stock tickers.

    Iterates over a provided list of stock tickers, fetching the company name and the most
    recent stock data for each. Constructs a detailed output string for each stock ticker,
    along with additional metadata, and appends it to a list. This list is returned at the
    end of the function. If data for a specific stock cannot be fetched, it is skipped.

    Parameters:
    - dfq (list of dict): A list of dicts for all stocks information in questions.jsonl.

    Returns:
    - list: A list of dictionaries. Each dictionary contains detailed information and
    metadata about a stock, including its current price, if available.
    """
    stock_list = []

    top_500_stocks = get_sp500_tickers()
    current_stocks = dfq["id"].unique() if "id" in dfq.columns else []
    set_top_500 = set(top_500_stocks)
    set_current = set(current_stocks)
    union_stocks_list = list(set_top_500.union(set_current))
    logger.info(
        f"Stock tickers not in top 500 but in current stocks: {set_current.difference(set_top_500)}"
    )

    for ticker in tqdm(union_stocks_list, desc="Fetching stock data"):
        # Avoid YFRateLimitError
        time.sleep(1)
        company_name, hist = fetch_one_stock(ticker)
        current_time = dates.get_datetime_now()

        if company_name and not hist.empty:
            current_price = round(hist["Close"].iloc[-1], 2)
            background = yf.Ticker(ticker).info.get("longBusinessSummary", "N/A")

            stock_list.append(
                {
                    "id": ticker,
                    "question": (
                        f"Will {ticker}'s market close price on "
                        "{resolution_date} be higher than its market close price on "
                        "{forecast_due_date}?\n\n"
                        "Stock splits and reverse splits will be accounted for in resolving this "
                        "question. Forecasts on questions about companies that have been delisted "
                        "(through mergers or bankruptcy) will resolve to their final close price."
                    ),
                    "background": background,
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "url": f"https://finance.yahoo.com/quote/{ticker}",
                    "resolved": False,
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": current_time,
                    "probability": current_price,
                    "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                    "freeze_datetime_value": current_price,
                    "freeze_datetime_value_explanation": f"The latest market close price of {ticker}.",
                }
            )

            logger.info(company_name)

    return pd.DataFrame(stock_list)


@decorator.log_runtime
def driver(_):
    """Fetch all stock and then upload to gcp."""
    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    all_stock = fetch_all_stock(dfq)
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
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
