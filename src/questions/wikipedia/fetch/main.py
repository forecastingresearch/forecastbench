"""Fetch data from Wikipedia."""

import logging
import os
import sys
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator, env, wikipedia  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"
filenames = data_utils.generate_filenames(source=source)


def get_edit_history_urls(page_title):
    """Get the edit history of a wikipedia page.

    Get the last edit of the day for each day between today and
    constants.QUESTION_BANK_DATA_STORAGE_START_DATE
    """
    base_history_url = (
        f"https://en.wikipedia.org/w/index.php?title={page_title}&action=history&limit=20"
    )
    offset = ""
    edit_history = []
    last_seen_dates = set()

    while True:
        history_url = base_history_url + offset
        response = requests.get(history_url)
        soup = BeautifulSoup(response.text, "html.parser")
        edits = soup.find_all(
            "li", class_=["mw-tag-wikieditor", "mw-tag-mobile_edit", "mw-tag-mobile_web_edit"]
        )

        for edit in edits:
            edit_date_str = edit.find("a", class_="mw-changeslist-date").text
            edit_date = datetime.strptime(edit_date_str, "%H:%M, %d %B %Y")
            edit_url = (
                "https://en.wikipedia.org" + edit.find("a", class_="mw-changeslist-date")["href"]
            )

            if edit_date.date() not in last_seen_dates:
                edit_history.append((edit_date, edit_url))
                last_seen_dates.add(edit_date.date())

            if edit_date.date() <= constants.QUESTION_BANK_DATA_STORAGE_START_DATE:
                return edit_history

        next_page = soup.find("a", {"class": "mw-nextlink"})
        if not next_page:
            break
        offset = "&offset=" + next_page["href"].split("offset=")[1]

    return edit_history


def download_wikipedia_table(url, table_index):
    """Download tables from url.

    If `table_index` is an int, download just that table from the url.
    Otherwise, if `table_index` is a list, download those tables and concatenate.
    """
    tables = pd.read_html(url)
    return (
        tables[table_index]
        if isinstance(table_index, int)
        else pd.concat([tables[i] for i in table_index])
    )


def download_tables(page):
    """Download all historical changes for the tables on the page."""
    page_title = page.get("page_title")
    n_rows_to_keep = page.get("table_keep_first_n_rows")
    table_index = page.get("table_index", 0)
    columns = list(page.get("fields").values())

    edit_history = get_edit_history_urls(page_title=page_title)
    edit_history.sort(reverse=True, key=lambda x: x[0])

    df = None
    for edit_date, url in tqdm(edit_history, f"Downloading edit histories for {page_title}"):
        try:
            dfw = download_wikipedia_table(url=url, table_index=table_index)
            if n_rows_to_keep is not None:
                dfw = dfw.iloc[:n_rows_to_keep]
            dfw = dfw[columns]
            dfw["date"] = edit_date.date().isoformat()
            dfw = dfw.dropna()
            df = dfw if df is None else pd.concat([df, dfw], ignore_index=True)
        except Exception as e:
            logger.error(e)
    return df


def download_and_store_wikipedia_tables():
    """Fetch and upload data for each page object in wikipedia.PAGES."""
    for page in wikipedia.PAGES:
        question_id_root = page.get("id_root")
        filename = wikipedia.get_fetch_filename(question_id_root)
        local_filename = f"/tmp/{filename}"
        logger.info(f"Downloading data for {question_id_root}.")

        df = download_tables(page=page)
        df.to_json(local_filename, orient="records", lines=True, force_ascii=False)
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
            destination_folder=wikipedia.fetch_directory,
        )


@decorator.log_runtime
def driver(_):
    """Fetch Wikipedia data and store in GCP Cloud Storage."""
    # Get the latest Wikipedia data
    logger.info("Downloading Wikipedia data.")

    download_and_store_wikipedia_tables()

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
