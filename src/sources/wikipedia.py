"""Wikipedia question source."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import Enum
from io import BytesIO
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from _fb_types import UpdateResult, WikipediaFetchResult
from _schemas import QuestionFrame
from helpers import constants, dates

from ._dataset import DatasetSource
from ._metadata import SOURCE_METADATA

if TYPE_CHECKING:
    import requests

logger = logging.getLogger(__name__)


class QuestionType(Enum):
    """Comparison types for Wikipedia questions."""

    SAME = 0
    SAME_OR_MORE = 1
    MORE = 2
    ONE_PERCENT_MORE = 3
    SAME_OR_LESS = 4


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_HEADERS = {"User-Agent": constants.BENCHMARK_USER_AGENT}

_WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME = (
    constants.QUESTION_BANK_DATA_STORAGE_START_DATETIME - timedelta(days=360 * 4)
)
_WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE = (
    _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATETIME.date()
)

_FIDE_BACKGROUND = (
    "The International Chess Federation (FIDE) governs international chess "
    "competition. Each month, FIDE publishes the lists 'Top 100 Players', 'Top 100 "
    "Women', 'Top 100 Juniors' and 'Top 100 Girls' and rankings of countries according "
    "to the average rating of their top 10 players and top 10 female players.\n"
    "To create the rankings, FIDE uses the Elo rating system, which is a method for "
    "calculating the relative skill levels of players in zero-sum games such as chess. "
    "The difference in the ratings between two players serves as a predictor of the "
    "outcome of a match. Two players with equal ratings who play against each other "
    "are expected to score an equal number of wins. A player whose rating is 100 "
    "points greater than their opponent's is expected to score 64%; if the difference "
    "is 200 points, then the expected score for the stronger player is 76%.\n"
    "A player's Elo rating is a number which may change depending on the outcome of "
    "rated games played. After every game, the winning player takes points from the "
    "losing one. The difference between the ratings of the winner and loser determines "
    "the total number of points gained or lost after a game. If the higher-rated "
    "player wins, then only a few rating points will be taken from the lower-rated "
    "player. However, if the lower-rated player scores an upset win, many rating "
    "points will be transferred. The lower-rated player will also gain a few points "
    "from the higher rated player in the event of a draw. This means that this rating "
    "system is self-correcting. Players whose ratings are too low or too high should, "
    "in the long run, do better or worse correspondingly than the rating system "
    "predicts and thus gain or lose rating points until the ratings reflect their true "
    "playing strength.\n"
    "Elo ratings are comparative only, and are valid only within the rating pool in "
    "which they were calculated, rather than being an absolute measure of a player's "
    "strength."
)

_PAGES = [
    {
        "id_root": "FIDE_rankings_elo_rating",
        "page_title": "FIDE_rankings",
        "table_index": [
            {
                "start_date": _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [1, 3],
            },
        ],
        "question_type": QuestionType.ONE_PERCENT_MORE,
        "key": {
            "id",
        },
        "fields": {
            "id": "Player",
            "value": "Rating",
        },
        "resolution_file_value_column_dtype": int,
        "question": (
            (
                "According to Wikipedia, will {id} have an Elo rating on {resolution_date} that's "
                "at least 1% higher than on {forecast_due_date}?"
            ),
            ("id",),
        ),
        "background": (_FIDE_BACKGROUND, tuple()),
        "freeze_datetime_value_explanation": (
            "{id}'s ELO rating.",
            ("id",),
        ),
        "clean_func": "clean_FIDE_rankings",
    },
    {
        "id_root": "FIDE_rankings_ranking",
        "page_title": "FIDE_rankings",
        "table_index": [
            {
                "start_date": _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [1, 3],
            },
        ],
        "question_type": QuestionType.SAME_OR_LESS,
        "key": {
            "id",
        },
        "fields": {
            "id": "Player",
            "value": "Rank",
        },
        "resolution_file_value_column_dtype": int,
        "question": (
            (
                "According to Wikipedia, will {id} have a FIDE ranking on {resolution_date} as "
                "high or higher than their ranking on {forecast_due_date}?"
            ),
            ("id",),
        ),
        "background": (_FIDE_BACKGROUND, tuple()),
        "freeze_datetime_value_explanation": (
            "{id}'s FIDE ranking.",
            ("id",),
        ),
        "clean_func": "clean_FIDE_rankings",
    },
    {
        "id_root": "List_of_world_records_in_swimming",
        "page_title": "List_of_world_records_in_swimming",
        "table_index": [
            {
                "start_date": _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": [0, 2],
            },
            {
                "start_date": datetime(2025, 5, 4).date(),
                "table_index": [0, 1],
            },
        ],
        "question_type": QuestionType.SAME,
        "key": {
            "id",
            "value",
        },
        "fields": {
            "id": "Name",
            "value": "Event",
        },
        "resolution_file_value_column_dtype": str,
        "question": (
            (
                "According to Wikipedia, will {id} still hold the world record for {value} in "
                "long course (50 metres) swimming pools on {resolution_date}?"
            ),
            ("id", "value"),
        ),
        "background": (
            (
                "The world records in swimming are ratified by World Aquatics (formerly known as "
                "FINA), the international governing body of swimming. Records can be set in long "
                "course (50 metres) or short course (25 metres) swimming pools.\n"
                "The ratification process is described in FINA Rule SW12, and involves submission "
                "of paperwork certifying the accuracy of the timing system and the length of the "
                "pool, satisfaction of FINA rules regarding swimwear and a negative doping test by "
                "the swimmer(s) involved. Records can be set at intermediate distances in an "
                "individual race and for the first leg of a relay race. Records which have not yet "
                "been fully ratified are marked with a '#' symbol in these lists."
            ),
            tuple(),
        ),
        "freeze_datetime_value_explanation": (
            "{id} is a record holder in the {value}.",
            (
                "id",
                "value",
            ),
        ),
        "clean_func": "clean_List_of_world_records_in_swimming",
    },
    {
        "id_root": "List_of_infectious_diseases",
        "page_title": "List_of_infectious_diseases",
        "table_index": [
            {
                "start_date": _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE,
                "table_index": 0,
            },
        ],
        "question_type": QuestionType.MORE,
        "key": {
            "id",
        },
        "fields": {
            "id": "Common name",
            "value": "Vaccine(s)",
        },
        "resolution_file_value_column_dtype": str,
        "question": (
            (
                "According to Wikipedia, will a vaccine have been developed for {id} by "
                "{resolution_date}?"
            ),
            ("id",),
        ),
        "background": (
            (
                "According to Wikipedia, {id} is the common name of an infectious disease. A "
                "vaccine is a biological preparation that provides active acquired immunity to a "
                "particular infectious or malignant disease. The safety and effectiveness of "
                "vaccines has been widely studied and verified. A vaccine typically contains an "
                "agent that resembles a disease-causing microorganism and is often made from "
                "weakened or killed forms of the microbe, its toxins, or one of its surface "
                "proteins. The agent stimulates the body's immune system to recognize the agent "
                "as a threat, destroy it, and recognize further and destroy any of the "
                "microorganisms associated with that agent that it may encounter in the future."
            ),
            ("id",),
        ),
        "freeze_datetime_value_explanation": (
            "Vaccine status for {id}. 'No' means that a vaccine has not yet been created. "
            "'Yes' means that it has.",
            ("id",),
        ),
        "clean_func": "clean_List_of_infectious_diseases",
        "is_resolved_func": "is_resolved_List_of_infectious_diseases",
        "value_func": "get_value_List_of_infectious_diseases",
    },
]

for _page in _PAGES:
    _page["table_index"].sort(key=lambda e: e["start_date"])


class WikipediaSource(DatasetSource):
    """Wikipedia dataset source with custom row-by-row resolution logic."""

    name: ClassVar[str] = "wikipedia"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    def fetch(self, **kwargs) -> WikipediaFetchResult:
        """Fetch Wikipedia table data for all configured pages.

        Returns a dict mapping id_root -> DataFrame of raw table data.
        """

        def _download_page(page):
            session = self._make_session()
            return page["id_root"], self._download_tables(page, session)

        results: WikipediaFetchResult = {}
        with ThreadPoolExecutor(max_workers=len(_PAGES)) as ex:
            for id_root, df in ex.map(_download_page, _PAGES):
                if df is None or df.empty:
                    raise ValueError(f"No Wikipedia data was downloaded for {id_root}.")
                results[id_root] = df

        return results

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: WikipediaFetchResult,
        **kwargs,
    ) -> UpdateResult:
        """Process fetched Wikipedia data into questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (WikipediaFetchResult): dict mapping id_root -> fetched table DataFrame.
        """
        resolution_files: dict[str, pd.DataFrame] = {}

        for page in _PAGES:
            id_root = page["id_root"]
            page_dff = dff.get(id_root)
            if page_dff is None or page_dff.empty:
                continue

            page_dff = page_dff.copy()
            page_dff["date"] = pd.to_datetime(page_dff["date"])
            if "clean_func" in page:
                page_dff = eval(f"WikipediaSource.{page['clean_func']}(page_dff)")

            dfq, page_res = self._update_page_questions(page=page, dfq=dfq, dff=page_dff)
            resolution_files.update(page_res)

        dfq = self._resolve_questions_for_dropped_pages(dfq)
        dfq = self._resolve_questions_for_id_transformations(dfq)

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
            hash_mapping=self.hash_mapping,
        )

    # ------------------------------------------------------------------
    # Private: fetch helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_session() -> requests.Session:
        """Create an HTTP session with retry logic."""
        # NB: requests/bs4 are imported lazily here (and in _get_edit_history) rather than at module
        # top level so that importing `sources.wikipedia` stays light. `helpers.wikipedia` and
        # `sources.registry` both import this module, and those are pulled (directly or via
        # `helpers.question_curation`) by ~13 jobs that never scrape Wikipedia (resolve, metaculus,
        # metadata, curate, leaderboard, nightly, base_eval). A top-level import would force
        # beautifulsoup4/lxml into all of their images.
        # TODO: revisit once requirements are refactored — if those consumers stop importing this
        # module at load time (e.g. question_curation rewired to sources._metadata), these can move
        # back to module-level imports.
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        session.headers.update(_HEADERS)
        _retry = Retry(total=3, backoff_factor=0.25, status_forcelist=[429, 500, 502, 503, 504])
        session.mount(
            "https://", HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=_retry)
        )
        return session

    @staticmethod
    def _get_edit_history(page_title: str, session: requests.Session) -> list[tuple]:
        """Get the edit history of a Wikipedia page.

        Get the last edit of the day for each day between today and
        _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE.
        """
        # Lazy import (see _make_session for rationale); TODO: revisit when requirements refactor.
        from bs4 import BeautifulSoup

        base_history_url = (
            f"https://en.wikipedia.org/w/index.php?title={page_title}&action=history&limit=200"
        )
        offset = ""
        edit_history = []
        last_seen_dates = set()

        while True:
            history_url = base_history_url + offset
            response = session.get(history_url, timeout=30)
            soup = BeautifulSoup(response.text, "html.parser")
            edits = soup.find_all("li", attrs={"data-mw-revid": True})

            for edit in edits:
                edit_date_str = edit.find("a", class_="mw-changeslist-date").text
                edit_date = datetime.strptime(edit_date_str, "%H:%M, %d %B %Y")
                edit_url = (
                    "https://en.wikipedia.org"
                    + edit.find("a", class_="mw-changeslist-date")["href"]
                )
                oldid = parse_qs(urlparse(edit_url).query).get("oldid", [None])[0]

                if edit_date.date() not in last_seen_dates:
                    edit_history.append((edit_date, oldid))
                    last_seen_dates.add(edit_date.date())

                if edit_date.date() <= _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE:
                    return [
                        (dt, rev)
                        for dt, rev in edit_history
                        if dt.date() >= _WIKIPEDIA_QUESTION_BANK_DATA_STORAGE_START_DATE
                    ]

            next_page = soup.find("a", {"class": "mw-nextlink"})
            if not next_page:
                break
            offset = "&offset=" + next_page["href"].split("offset=")[1]

        return edit_history

    @staticmethod
    def _download_wikipedia_table(
        page_title: str,
        edit_date: datetime,
        revid: str,
        table_index: list,
        session: requests.Session,
    ) -> pd.DataFrame:
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

                logger.info(f"Rate limited, waiting {delay}s")
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

    @staticmethod
    def _download_tables(page: dict, session: requests.Session) -> pd.DataFrame | None:
        """Download all historical changes for the tables on the page."""
        page_title = page.get("page_title")
        n_rows_to_keep = page.get("table_keep_first_n_rows")
        table_index = page.get("table_index", 0)
        columns = list(page.get("fields").values())

        edit_history = WikipediaSource._get_edit_history(page_title=page_title, session=session)
        edit_history.sort(reverse=True, key=lambda x: x[0])

        value_col = page["fields"]["value"]
        value_col_dtype = page["resolution_file_value_column_dtype"]

        df_list = []
        for edit_date, revid in edit_history:
            try:
                dfw = WikipediaSource._download_wikipedia_table(
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
                    raise ValueError(f"`{value_col_dtype}` dtype not yet supported.")
                dfw = dfw.dropna()
                dfw[value_col] = dfw[value_col].astype(value_col_dtype)
                df_list.append(dfw.dropna())
            except Exception as e:
                logger.error(f"In {edit_date} {revid}\n{e}\n")
        df = pd.concat(df_list, ignore_index=True) if df_list else None
        return df

    # ------------------------------------------------------------------
    # Private: update helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fill_template(page: dict, page_key: str, values: dict) -> str:
        """Fill a question/background/explanation template."""
        fill_values = {field: values[field] for field in page[page_key][1]}
        # Always maintain resolution_date and forecast_due_date when formatting the string.
        default_values = {
            "resolution_date": "{resolution_date}",
            "forecast_due_date": "{forecast_due_date}",
        }
        combined_fill_values = {**default_values, **fill_values}
        return page[page_key][0].format(**combined_fill_values)

    @staticmethod
    def _build_resolution_df(
        dff: pd.DataFrame, page: dict, wid: str, question_key: pd.Series
    ) -> pd.DataFrame | None:
        """Build the per-question resolution DataFrame. Returns a DataFrame or None.

        Validation is intentionally left to UpdateResult.__post_init__ (which validates every
        resolution file against ResolutionFrame); here we only cast the id/date dtypes, mirroring
        the other sources' resolution-building helpers.

        Args:
            dff (pd.DataFrame): Fetched data DataFrame for a page.
            page (dict): Page config dict.
            wid (str): Hashed question ID.
            question_key (pd.Series): Series with key field values identifying the question.
        """
        id_field = page["fields"]["id"]
        value_field = page["fields"]["value"]

        mask = pd.Series(True, index=dff.index)
        for field_name in question_key.index:
            mask &= dff[field_name] == question_key[field_name]

        df = dff[mask].copy()
        if df["date"].max().date() < constants.QUESTION_BANK_DATA_STORAGE_START_DATE:
            # Fetching more data than we need for naive forecasts. Don't need to create resolution
            # files for events that are no longer current.
            return None

        df.rename(columns={id_field: "id", value_field: "value"}, inplace=True)
        df["id"] = wid

        def fill_missing_with_nan(df, dff):
            """Fill in nan where the item has dropped out of the table.

            Sometimes values drop out of the table then reappear. This could be for valid reasons,
            e.g. someone had a world record, lost it, then got it again. Either way, fill these with
            nan. Invalid reasons (e.g. name changes) need to be caught by hand and nullified.
            """
            all_dates = dff["date"].sort_values().unique()
            all_dates = all_dates[all_dates >= constants.QUESTION_BANK_DATA_STORAGE_START_DATETIME]
            next_after_df_max_date = all_dates[all_dates > df["date"].max()]
            max_cutoff = (
                next_after_df_max_date.min()
                if len(next_after_df_max_date) > 0
                else df["date"].max()
            )
            all_dates = all_dates[(all_dates <= max_cutoff) & (all_dates >= df["date"].min())]
            drop_out_dates = []
            for drop_out_date in [date for date in all_dates if date not in df["date"].unique()]:
                drop_out_dates.append(
                    {
                        "id": wid,
                        "value": None,
                        "date": drop_out_date,
                    }
                )
            df = pd.concat([df, pd.DataFrame(drop_out_dates)], ignore_index=True)
            return df

        df = fill_missing_with_nan(df=df, dff=dff)

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values(by="date", ignore_index=True)

        return df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    def _add_to_dfq(
        self,
        dfq: pd.DataFrame,
        dfr: pd.DataFrame,
        page: dict,
        wid: str,
        id_field_value: str,
    ) -> pd.DataFrame:
        """Add the question to dfq."""
        dfr = dfr.sort_values(by="date")
        value = dfr.iloc[-1]["value"]

        resolved = value is None
        if "is_resolved_func" in page.keys():
            resolved = eval(f"WikipediaSource.{page['is_resolved_func']}(value)")

        if "value_func" in page.keys():
            value = eval(f"WikipediaSource.{page['value_func']}(value)")

        values = {
            "id": id_field_value,
            "value": value,
        }
        question = self._fill_template(page=page, page_key="question", values=values)
        freeze_datetime_value_explanation = self._fill_template(
            page=page, page_key="freeze_datetime_value_explanation", values=values
        )

        background = self._fill_template(page=page, page_key="background", values=values)

        row = {
            "id": wid,
            "question": question,
            "background": background,
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": "N/A",
            "market_info_close_datetime": "N/A",
            "url": f"https://en.wikipedia.org/wiki/{page['page_title']}",
            "market_info_resolution_datetime": "N/A",
            "resolved": resolved,
            "forecast_horizons": [] if resolved else constants.FORECAST_HORIZONS_IN_DAYS,
            "freeze_datetime_value": value,
            "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
        }

        df_question = pd.DataFrame([row])
        if row["id"] not in dfq["id"].values:
            return df_question if dfq.empty else pd.concat([dfq, df_question], ignore_index=True)

        # Update the row where `dfq["id"] == df_question["id"]`.
        dfq = dfq.set_index("id")
        df_question = df_question.set_index("id")
        dfq.update(df_question)
        return dfq.reset_index()

    def _update_page_questions(
        self, page: dict, dfq: pd.DataFrame, dff: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Update questions and resolutions for the provided Wikipedia page.

        Returns (dfq, resolution_files_dict).
        """
        question_id_root = page.get("id_root")
        logger.info(f"Updating questions for {question_id_root}.")

        # The `key` field of each page contains the unique entry/entries that make a question.
        # See issue #123.
        id_fields = [page["fields"][key] for key in page["key"]]
        resolution_files = {}

        for _, row in dff[id_fields].drop_duplicates().iterrows():
            id_field_value_for_wid = str(row.iloc[0]) if len(row) == 1 else str(sorted(row))
            wid = self._id_hash(id_root=question_id_root, id_field_value=id_field_value_for_wid)
            try:
                dfr = self._build_resolution_df(dff=dff, page=page, wid=wid, question_key=row)
                if dfr is not None:
                    resolution_files[wid] = dfr
                    dfq = self._add_to_dfq(
                        dfq=dfq,
                        dfr=dfr,
                        page=page,
                        wid=wid,
                        id_field_value=row[page["fields"]["id"]],
                    )
            except Exception as e:
                logger.warning(f"Couldn't add {question_id_root} {wid}: {row}")
                logger.warning(f"Exception encountered: {e}")

        return dfq, resolution_files

    def _resolve_questions_for_dropped_pages(self, dfq: pd.DataFrame) -> pd.DataFrame:
        """Resolve questions for pages that have been removed from _PAGES.

        If we ever remove pages, we want to stop sampling from those questions. Simply resolve them.
        """
        id_roots = [d["id_root"] for d in _PAGES]
        for index, row in dfq.iterrows():
            d = self._id_unhash(hash_key=row["id"])
            if d is None or d.get("id_root") not in id_roots:
                dfq.loc[index, "resolved"] = True
        return dfq

    @staticmethod
    def _resolve_questions_for_id_transformations(dfq: pd.DataFrame) -> pd.DataFrame:
        """Resolve questions for keys in `_TRANSFORM_ID_MAPPING`.

        `_TRANSFORM_ID_MAPPING` contains keys of questions that were erroneously made for one reason
        or another. Those keys point to the correct IDs for those questions. When the correct ID is
        resolved, ensure the original question ID is resolved too.
        """
        for key, value in _TRANSFORM_ID_MAPPING.items():
            resolved_series = dfq[dfq["id"] == value]["resolved"]
            if not resolved_series.empty and resolved_series.iloc[0]:
                dfq.loc[dfq["id"] == key, "resolved"] = True
                logger.info(f"Resolving: {key}")
        return dfq

    # ------------------------------------------------------------------
    # Clean / value / resolved functions (referenced by _PAGES via eval)
    # ------------------------------------------------------------------

    @staticmethod
    def clean_FIDE_rankings(df: pd.DataFrame) -> pd.DataFrame:
        """Clean fetched data for FIDE_rankings.

        Fix inconsistent player names.
        """
        df = df[~df["Player"].str.contains("Change from the previous month")].copy()
        replacements = {
            "Gukesh D.": "Gukesh Dommaraju",
            "Gukesh D": "Gukesh Dommaraju",
            "Leinier Dominguez": "Leinier Domínguez Pérez",
            "Leinier Dominguez Pérez": "Leinier Domínguez Pérez",
            "Nana Dzagnidze]": "Nana Dzagnidze",
        }
        df["Player"] = df["Player"].replace(replacements)
        return df

    @staticmethod
    def clean_List_of_world_records_in_swimming(df: pd.DataFrame) -> pd.DataFrame:
        """Clean fetched data for List_of_world_records_in_swimming.

        Drop any rows that contain parens.
        """
        df = df[~df["Name"].str.contains(r"[()]")].reset_index(drop=True)
        df = df[~df["Name"].str.contains("eventsort")].reset_index(drop=True)
        df = df[~df["Name"].str.contains("recordinfo")].reset_index(drop=True)
        return df

    @staticmethod
    def clean_List_of_infectious_diseases(df: pd.DataFrame) -> pd.DataFrame:
        """Clean fetched data for List_of_infectious_diseases.

        * Remove rows with multiple answers.
        * Change all `Under research[x]` to `No`
        * Change all `No` to 0
        * Change all `Yes` to 1
        """
        duplicates = df[df.duplicated(subset=["date", "Common name"], keep=False)]
        df = df.drop(duplicates.index).reset_index(drop=True)
        # On and before this date the `"Vaccine(s)"` field had other info in it.
        df = df[df["date"] > pd.Timestamp("2021-07-07")]
        df["Vaccine(s)"] = df["Vaccine(s)"].replace(
            {
                r"Under research.*": "No",
                r"Under Development.*": "No",
                r"Yes.*": "Yes",
                r"No.*": "No",
            },
            regex=True,
        )
        df.loc[df["Vaccine(s)"] == "No", "Vaccine(s)"] = 0
        df.loc[df["Vaccine(s)"] == "Yes", "Vaccine(s)"] = 1
        df["Vaccine(s)"] = df["Vaccine(s)"].astype(int)
        df = df.dropna(ignore_index=True)
        return df

    @staticmethod
    def is_resolved_List_of_infectious_diseases(value) -> bool:
        """Return true if the vaccine has been developed."""
        return value == 1 or str(value).lower() == "yes"

    @staticmethod
    def get_value_List_of_infectious_diseases(value) -> str:
        """Return Yes/No instead of 1/0."""
        return "Yes" if value else "No"

    # ------------------------------------------------------------------
    # Resolve
    # ------------------------------------------------------------------

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve Wikipedia questions row by row."""
        logger.info("Resolving Wikipedia questions.")

        dfr = self._ffill_dfr(dfr)

        yesterday = pd.Timestamp(dates.get_date_yesterday())
        mask = df["resolution_date"] <= yesterday
        for index, row in df[mask].iterrows():
            forecast_due_date = row["forecast_due_date"].date()
            resolution_date = row["resolution_date"].date()
            if not self._is_combo(row):
                value = self._resolve_single_question(
                    mid=row["id"],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
            else:
                value1 = self._resolve_single_question(
                    mid=row["id"][0],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
                value2 = self._resolve_single_question(
                    mid=row["id"][1],
                    dfr=dfr,
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                )
                value = self._combo_change_sign(
                    value1, row["direction"][0]
                ) * self._combo_change_sign(value2, row["direction"][1])
            df.at[index, "resolved_to"] = float(value)
        df.loc[mask, "resolved"] = True
        return df, []

    def _resolve_single_question(self, mid, dfr, forecast_due_date, resolution_date):
        """Resolve an individual Wikipedia question by comparing values at two dates.

        Nullification is handled by
        BaseSource.resolve() which strips nullified rows before calling _resolve().
        """
        mid = self._transform_id(mid)
        d = self._id_unhash(mid)
        if d is None:
            logger.error(f"Wikipedia: could NOT unhash {mid}")
            return np.nan

        def get_value(dfr, mid, date):
            value = dfr[(dfr["id"] == mid) & (dfr["date"].dt.date == date)]["value"]
            return value.iloc[0] if not value.empty else None

        forecast_due_date_value = get_value(dfr, mid, forecast_due_date)
        resolution_date_value = get_value(dfr, mid, resolution_date)

        if pd.isna(forecast_due_date_value):
            logger.info(
                f"Nullifying Wikipedia market {mid}. "
                "The forecast question resolved between the freeze date and the forecast due date."
            )
            return np.nan

        question_type = [q["question_type"] for q in _PAGES if q["id_root"] == d["id_root"]]
        if len(question_type) != 1:
            logger.error(
                f"Nullifying Wikipedia market {mid}. Couldn't find comparison type "
                "(should not arrive here)."
            )
            return np.nan

        return self._compare_values(
            question_type=question_type[0],
            resolution_date_value=resolution_date_value,
            forecast_due_date_value=forecast_due_date_value,
        )

    @staticmethod
    def _compare_values(question_type, resolution_date_value, forecast_due_date_value):
        """Compare resolution-date and due-date values according to the question type."""
        if question_type == QuestionType.SAME:
            return resolution_date_value == forecast_due_date_value
        elif question_type == QuestionType.SAME_OR_MORE:
            return resolution_date_value >= forecast_due_date_value
        elif question_type == QuestionType.SAME_OR_LESS:
            return resolution_date_value <= forecast_due_date_value
        elif question_type == QuestionType.MORE:
            return resolution_date_value > forecast_due_date_value
        elif question_type == QuestionType.ONE_PERCENT_MORE:
            return resolution_date_value >= forecast_due_date_value * 1.01
        else:
            raise ValueError("Invalid QuestionType")

    @staticmethod
    def _ffill_dfr(dfr):
        """Forward-fill resolution values to yesterday for all IDs."""
        dfr = dfr.sort_values(by=["id", "date"])
        dfr = dfr.drop_duplicates(subset=["id", "date"])
        yesterday = dates.get_date_yesterday()
        yesterday = pd.Timestamp(yesterday)
        chunks = []
        for unique_id in dfr["id"].unique():
            temp_df = (
                dfr[dfr["id"] == unique_id].set_index("date").resample("D").ffill().reset_index()
            )
            if temp_df["date"].max() < yesterday:
                last_value = temp_df.iloc[-1]["value"]
                additional_days = pd.date_range(
                    start=temp_df["date"].max() + timedelta(days=1), end=yesterday
                )
                additional_df = pd.DataFrame(
                    {"date": additional_days, "id": unique_id, "value": last_value}
                )
                temp_df = pd.concat([temp_df, additional_df])
            chunks.append(temp_df)
        dfr = pd.concat(chunks).sort_values(by=["id", "date"]).reset_index(drop=True)
        return dfr

    @staticmethod
    def _transform_id(wid):
        """Map deprecated question IDs to their replacement IDs."""
        new_id = _TRANSFORM_ID_MAPPING.get(wid)
        if new_id is not None:
            logger.info(f"In wikipedia._transform_id(): Transforming {wid} --> {new_id}.")
            return new_id
        return wid

    # ------------------------------------------------------------------
    # Hash mapping
    # ------------------------------------------------------------------

    def populate_hash_mapping(self, raw_json: str) -> None:
        """Parse hash mapping from raw JSON string."""
        self.hash_mapping = json.loads(raw_json) if raw_json else {}

    def dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON, removing deprecated keys first."""
        for k in _TRANSFORM_ID_MAPPING:
            self.hash_mapping.pop(k, None)
        return json.dumps(self.hash_mapping, indent=4)

    def _id_hash(self, id_root: str, id_field_value: str) -> str:
        """Encode wikipedia Ids and store in hash_mapping."""
        d = {"id_root": id_root, "id_field_value": id_field_value}
        dictionary_json = json.dumps(d, sort_keys=True)
        hash_key = hashlib.sha256(dictionary_json.encode()).hexdigest()
        self.hash_mapping[hash_key] = d
        return hash_key

    def _id_unhash(self, hash_key: str):
        """Look up the original question dict, applying ID transform first."""
        hash_key = self._transform_id(hash_key)
        return self.hash_mapping.get(hash_key)


# flake8: noqa: B950
_TRANSFORM_ID_MAPPING = {
    # Below is a list of IDs that have changed since question sets were released for reasons
    # explained below.
    #
    # The IDs listed as keys in this list are no longer sampled from.
    #
    # If they were asked previously, they resolution values from the value variable are used. Hence
    # to be included in this list, the value must have been consistently used since the ID present
    # in the `key` was first included in a question set. If not, then put the key in the
    # `_IDS_TO_NULLIFY` list.
    #
    # *******
    #
    # FIDE rankings: I noticed that there were inconsistent edits to the FIDE
    # rankings. As a result, the ranking history was not complete for several players (e.g.
    # Gukesh D, Gukesh D. and Gukesh Dommaraju were considered to be different people. Map the
    # old players to the new id after having fixed the history.
    #
    # Gukesh Dommaraju FIDE_rankings_elo_rating
    "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92": "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f",
    "eb5bcf6a467ca2b850a28b95d51c5b58d314f909d72afdd66935f2e28d8334a3": "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f",
    # Gukesh Dommaraju FIDE_rankings_ranking
    "8702851a2593fcd3d2587481a2fcb623e775e3cbfe86abad2641bb34a13138ce": "c097d5216ff8068a20a5c9592860a003ccc06dd4eb7da842d86c3816a68c3ab6",
    "91dd441b57571c8d23b83a40d11c4a9a87d55eb3948f034e3e5e778f1f0b98c6": "c097d5216ff8068a20a5c9592860a003ccc06dd4eb7da842d86c3816a68c3ab6",
    # Nana Dzagnidze FIDE_rankings_elo_rating
    "0bce98434da73edce73e9570b99dac301d39b224d49946908ee34673b5e0e4d1": "793b2cd84b35aaf26c07464c21690ad171f2168f639513b9883d63234e515e03",
    # Nana Dzagnidze FIDE_rankings_ranking
    "7a100cc5019c37fd083618aa560229e4ce1011f5cace5b6d0e6817b6a40b3ffa": "c28f340263644425dd87c3cf915351620e452358d2118a20e27fb20ba76cfa64",
    # Map Praggnanandhaa R to R Praggnanandhaa on FIDE_rankings_elo_rating
    # Do _not_ map for FIDE_rankings_ranking since the first question was asked on 2024-07-21 but
    # the current name, R Praggnanandhaa did not take effect until 2024-10-15
    "d2cfcce09363ddad01df31559624e330557f69eabcab39ed3734c11a60f153c7": "a987eef385663d96115ba6c113ffb3dc7e83affdcaa8c53421220e4e9e1f95f8",
    # Map Erigaisi Arjun to Arjun Erigaisi. the ID in the key was first asked on 2025-03-02, long
    # after the ID in the value became the standard on the table
    "3ff636ffa947b8f0f3adb55964cd75294716abea2c27933ad89d7abff42d633e": "127f33fb2530ea03d3af0420afc5e0f283b23503e3dc7ff0ccd8e84dfd241f49",
    #
    # List_of_world_records_in_swimming following issue #123, swimming records needed to
    # modified to include the race type in the key identifying the question. This was because a
    # single person could have records in different events. As the old keys may have been asked
    # in previous question sets, all ids need to pass through the transformation function here
    # before being resolved.
    #
    # The following have exactly 1 world record. their ids change because now we save both name
    # and the WR event.
    "7c17d34e37d8cea481d3933f4e1c2c091bd523c3980043e539cde90fbc08f29a": "828ceafd45d4bef413280614944d4e2d579fc83e089592d8d9e363c3b58b44d2",
    "b2d7953344ca7b2fd37c2ef0d9664053da9a26d774fcd4c6e315e74340bd6bf0": "e79a0c5058411513ad3fa8c65448fdf41c27f83f9fbafe4d1ac58bebfd713bde",
    "9fc87840aa0bebfcd6c03cdfbcdb1b6a11120bdb0419d0d9334301cf536a88fd": "ba4008b18e2ef8ad82fc8ea5f066d464ce8e45133d0e620bae27b2ce740d4c1d",
    "7558c5b4f539cc922552c4f18a9a5cdaccbc100d6108acf117e886bd9dc67857": "04bfcc27745a1813367fcb5aad43423db616dccff54c1cc929bd32de3f43a38a",
    "5ca04a3a78c7ffe5817f080f95e883a83edd6a1471caba48d435448a2d879b52": "fe29236219f59bc35575d70c4b8f2897eaaac32a87f31c9abe56e0a251de0663",
    "72a33a32e409997da4782833a3893e503ff84ad71005d422f9c2e00dd193350a": "cc6923cbc7b882a64ae1d99ab4bda02ed0268393dbeab1b5b173bd05aab73a57",
    "61c0fb3703e68cee2439afd5c2d71522bc6649a1fa154491f58981456fa8ab68": "42f335ea171402fb761bd367e7ca94292a52b2cbebe4f2edacf23b87552bd5d6",
    "926083f9ae268e48beea5516d8b48024d0a4d5ae7b5ef0c7d18f205dfc831b90": "1e5f84dd79c1a731f71ef1c7100fd66791275e78f266c27ae6c1568847c087db",
    "c90a910e5ea0ef3bdaedf23ce591e20a8da2df5b88c5b04e6264761959ddfcc0": "9d14f6afd960fcf12aaeaf741bab57e7f2e5002c5a72d3035f7db8cf98fbdee9",
    "7be763022f7a4e8a84a4c78d8934b9f47dd708514a9373a810892a34a679254e": "12af44da7b699297e8be3140315e693ca414ef693010ceb078ef92700ce6d998",
    "3c37fef353460bfd130fde0117638badaee913ee8c79b8cdf4c35e2c5710126a": "690617e9a2ad8ed147767aa6dd0220d0c05026291f2ad92eaf42dee14f0873e2",
    "12486c21df689124f8fdad70760247dffe2b7696599748bcb5c7a738735285d5": "c6ee39b4504603aa5ddbe73f378d48d94ab128406e5dd1bbb70ead0207a43840",
    "cb3336ed4ee8ebe8364f97814f75d9777ef8dd30a8f775c29ac727372fcd14be": "203bdb0b73fd156109cbb2227e92d16a0e84ee5d86f71b22bf7ded1d9bd8a924",
    "ebb4e1e85bed81266e94dda8e84eafe1479d5697f850792d84b5fab7251f483f": "b4c4989ac25edfbb8510e8ffa9aeee70c0de0d82e22a360faac590304f67c575",
    "7221cc24a88774591bc4c40046c92d692c12d8bc1b63c39c3f295522e9181c57": "3835c0448587f4c28471e27f597c6f7ae89d4060a8e634cca21f899dcd057925",
    "c846c6eb73a939076d4054972dfb1149cc8b3bac31526882171d6b9ff87a7adb": "6cd2092339ebef0efad5610588facdec6c4d0f9c2607791034f44dbc7ae86f4c",
    "558aebef0c3c95c9559e54f27e8fa908da02917d9833abb9a2330d10b1b2b953": "6e5c5efbd814430094697396276bd121a7f941eede08d4f108b4c5ecb3590458",
    "d563befa9ded6fdc765857663fceeb546cd5b983b2de7850615c037880a25390": "48728daeb0c53c76232dc0d9a1c8f8efaeafd9dd2d725feb2bd5d81b5dd5af10",
    "d5e1fda224104cd3bac9ce8ab4b4face291793306f3bcf515c4be96b4fef9f7e": "5b5cca41a01c8cbec95d84aac92bd4f4c91dd4627bfd0ceeff720f5a53c8ba31",
    "50ff16043ba629140f82ada741c6f24245bb98b8eefd7d55aac10f750d2d43ad": "37d0cf293cc84f9388f9f3a032f90ef55c8549c0f7bbcdab6a426553af31f128",
    "4651d16e683bc20fe0c0a04dcbb52fddf2982ec3df016a52643e5aee291b37ce": "5096dbb513988ae3252e7621e1754277d56b95703074341c54a080a0b7821571",
    "eaf10e98fdc5ddd2227b212f1e446a1937a2e0529b8f89c9a2528cb469e7cc27": "c539c3ef6d2534204b4fc67a94b14eebc7c51f141fea3c30f337cb3ede390b11",
    "646cd3619a16c273007816e559834682e19754dcaf7d0ecb6ffebe64d351f177": "0e0f5a6cf1ac926657d43b909af4d2fb27ba975dfe3a274fbe0930dcf667d499",
    "851337578d0bf07dc60b233f5ef2a49d0309c1728621dd7b4ac0724414887fde": "56e00c66d9d2bfa3dd3ad0656c81701e04033438f90320ba96a63b62e61a4ea5",
    "245eb0146484bad467bbdb3d0c871f30390fb1a902105f86c85ec4637c52a9f4": "e222aa0998ad2e53a4cbfbdb11f3d80dfd13a263b4748e4a6cd8f4b965f0506f",
    "5b078ec5a0d0a51c3668c62fe93441bd177ad4c58a1ff1d50b62a8bf6bc609fe": "afd040f28eb27f973ba1dc2cfeb3f613a7c29a543b14cbab4ba8d44ca8eb0d36",
    "eea4cb0741c001c18ec28a58f64fb02bfba72e776f2d9ef2257309269b119526": "234175128275d109b5ffe5f8a30f863f150051e892e56566f88936b961be1f2f",
    "6e295dc29db5dce0672097160d432e7a3af469317298cb3153d745b2270041f1": "f0054684e6c6c24c5595e5cdf8498ffc5479e82d26a8b0318af35a26cd9b9ce7",
    "e4afa18eb3d8d08fbc37c114f876a93ddceac453da415512ef5d73c7d26f391d": "747aa3406023deab8175b051bac64b55c061d38c2aebc73c1ded759de7b0477a",
    "cf02d516cc8b14b7b2880baae0ca4d520b167fe271123e6adfeedaefb83a3ec5": "6358ab9dab0aa4b6fc2abe8aacf1b31c8cbed08d54557eb4982c230fe19fe774",
    "25891a351e97154028edc8075558470a6ec21d6d37dbd75f74268ee1b48253bf": "94297b75a6d18445c35a179a860b810bf0be7b6f296c502cec7caab24c8c1775",
    # The following hold 2 or more world records; map the old question to the new one
    # corresponding to the same race. The other questions did not exist before so just use the
    # new ids they created.
    # Sarah Sjöström         4
    "1d0989190ba1a2a4b3f3738f02e6dd5f463afc712d7507c8a89d7f971d4c27e4": "2c42ee57b6879cdf61bb608b564eef91d4b3a2642392527bbc8532502029e906",  # ['50m freestyle', 'Sarah Sjöström']
    # Kaylee McKeown         3
    "831a289e8d494cce6ac96eb97eadd8a2d80ec3d7e406ae440bad864583a12adb": "6e13696b1502516e89fa7bea8d3df930959ecd772498363bb019ea562b70533f",  # ['50m backstroke', 'Kaylee McKeown']
    # Paul Biedermann        2
    "20efdec28913ecfb1e3a3e26ad2c99e1b4d7ad3f43b5a6202c46f9c277c17406": "0ba788cac9fef02dae3b7b3713a085306f9c7e1d321ad7dc9e2473666f65b6c3",  # ['200m freestyle', 'Paul Biedermann']
    # Katie Ledecky          2
    "6797a1d0a791aa20ab4de7d1a465a06b24e6c8100ec1a796c306f47e612923be": "ad589ed82fb268fd2dec1dda10d211b2e82fcfa86cd526e44ba8e20e81265176",  # ['800m freestyle', 'Katie Ledecky']
    # Ariarne Titmus         2
    "7233fa748a364e7e93f1899c23ff71571fb3e78b55a1bd951648209211af3cc7": "0742587fdf80b228a9f77c97b4dc0aea2ef60598138b2d08a161d153ae59c9b7",  # ['200m freestyle', 'Ariarne Titmus']
    # Katinka Hosszú         2
    "2e88b046538e239140043da9471c2b4894615a12173c3a52ee707321acf2ed8d": "c4db6cf85ef3ef4165705b863f1491f2903df3a2534e2d4e25f57edcbdfaac4b",  # ['200m individual medley', 'Katinka Hosszú']
    # Adam Peaty             2
    "2b6d5c38b8ee7751461358ec55a5fa80040f996b824eee281e00ac6593133cef": "211fd2e3e651f5c5de584e5b3ec89049347d6ca1f5ff4a15440835f105a6047c",  # ['50m breaststroke', 'Adam Peaty']
    # Summer McIntosh        2
    "d8ef6ba516706b350a1a40149914034def70217a6152904b4b7be5b9c4c64ce5": "d32864887ea4fba0a850c9da3588265b82b23098d8fdded2be8f2b8cd584329d",  # ['400m individual medley', 'Summer McIntosh']
    #
    # In List_of_infectious_diseases, the virus name Monkeypox was changed to Mpox
    #
    "f9323386a651ce67fc0da31285bee22a4ec53b8a2ea5220431ecb4560fb44c77": "3f04d0cfccd38b26e86c0939516c483eb31edf6aaa3a1eaaabe38a48f7a0996a",
}


_IDS_TO_NULLIFY = [
    {"id": nq.id, "nullify_start_date": nq.nullification_start_date}
    for nq in SOURCE_METADATA["wikipedia"]["nullified_questions"]
]
