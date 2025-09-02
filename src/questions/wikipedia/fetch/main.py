"""Fetch data from Wikipedia."""

import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from email.utils import parsedate_to_datetime
from io import BytesIO
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, decorator, env, wikipedia  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"
filenames = data_utils.generate_filenames(source=source)


def make_session():
    """Make a session for requests."""
    session = requests.Session()
    session.headers.update(wikipedia.HEADERS)
    _retry = Retry(total=3, backoff_factor=0.25, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=_retry))
    return session


def get_edit_history(page_title):
    """Get the edit history of a wikipedia page.

    Get the last edit of the day for each day between today and
    wikipedia.WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE
    """
    base_history_url = (
        f"https://en.wikipedia.org/w/index.php?title={page_title}&action=history&limit=200"
    )
    offset = ""
    edit_history = []
    last_seen_dates = set()

    session = make_session()
    while True:
        history_url = base_history_url + offset
        response = session.get(history_url, timeout=30)
        soup = BeautifulSoup(response.text, "html.parser")
        edits = soup.find_all("li", attrs={"data-mw-revid": True})

        for edit in edits:
            edit_date_str = edit.find("a", class_="mw-changeslist-date").text
            edit_date = datetime.strptime(edit_date_str, "%H:%M, %d %B %Y")
            edit_url = (
                "https://en.wikipedia.org" + edit.find("a", class_="mw-changeslist-date")["href"]
            )
            oldid = parse_qs(urlparse(edit_url).query).get("oldid", [None])[0]

            if edit_date.date() not in last_seen_dates:
                edit_history.append((edit_date, oldid))
                last_seen_dates.add(edit_date.date())

            if edit_date.date() <= wikipedia.WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE:
                return [
                    (dt, rev)
                    for dt, rev in edit_history
                    if dt.date() >= wikipedia.WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE
                ]

        next_page = soup.find("a", {"class": "mw-nextlink"})
        if not next_page:
            break
        offset = "&offset=" + next_page["href"].split("offset=")[1]

    return edit_history


def download_wikipedia_table(page_title, edit_date, revid, table_index, session):
    """Download tables from url."""
    url = f"https://en.wikipedia.org/api/rest_v1/page/html/{page_title}/{revid}"
    while True:
        response = session.get(url, timeout=30)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            delay = 5
            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    try:
                        retry_dt = parsedate_to_datetime(retry_after)
                        now = datetime.now(tz=retry_dt.tzinfo)
                        delay = max(0, int((retry_dt - now).total_seconds()))
                    except Exception:
                        delay = 5

            print(f"\n{delay} seconds\n")
            time.sleep(delay)
            continue
        response.raise_for_status()
        break

    tables = pd.read_html(BytesIO(response.content))
    table_index_to_use = max(
        [e for e in table_index if e["start_date"] <= edit_date.date()],
        key=lambda e: e["start_date"],
    )
    ti = table_index_to_use["table_index"]
    return tables[ti] if isinstance(ti, int) else pd.concat([tables[i] for i in ti])


def download_tables(page):
    """Download all historical changes for the tables on the page."""
    session = make_session()

    page_title = page.get("page_title")
    n_rows_to_keep = page.get("table_keep_first_n_rows")
    table_index = page.get("table_index", 0)
    columns = list(page.get("fields").values())

    edit_history = get_edit_history(page_title=page_title)
    edit_history.sort(reverse=True, key=lambda x: x[0])

    value_col = page["fields"]["value"]
    value_col_dtype = page["resolution_file_value_column_dtype"]

    df_list = []
    for edit_date, revid in tqdm(edit_history, f"Downloading edit histories for {page_title}"):
        try:
            dfw = download_wikipedia_table(
                page_title=page_title,
                edit_date=edit_date,
                revid=revid,
                table_index=table_index,
                session=session,
            )
            if n_rows_to_keep is not None:
                dfw = dfw.iloc[:n_rows_to_keep]
            dfw = dfw[columns]
            dfw["date"] = edit_date.date().isoformat()
            if value_col_dtype in (int, float):
                dfw[value_col] = pd.to_numeric(dfw[value_col], errors="coerce")
            elif value_col_dtype is str:
                pass
            else:
                raise ValueError(f"`{value_col_dtype}` dytpe not yet supported.")
            dfw = dfw.dropna()
            dfw[value_col] = dfw[value_col].astype(value_col_dtype)
            df_list.append(dfw.dropna())
        except Exception as e:
            logger.error(f"In {edit_date} {revid}\n{e}\n")
    df = pd.concat(df_list, ignore_index=True) if df_list else None
    return df


def download_and_store_wikipedia_tables(page):
    """Fetch and upload data for each page object in wikipedia.PAGES."""
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

    with ProcessPoolExecutor(max_workers=min(env.NUM_CPUS, len(wikipedia.PAGES))) as ex:
        list(
            tqdm(
                ex.map(download_and_store_wikipedia_tables, wikipedia.PAGES),
                total=len(wikipedia.PAGES),
                desc="Downloading pages",
            )
        )

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
