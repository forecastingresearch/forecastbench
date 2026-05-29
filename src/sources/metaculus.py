"""Metaculus question source."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, ClassVar

import backoff
import certifi
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import MetaculusFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates, question_curation

from ._market import MarketSource

logger = logging.getLogger(__name__)

_METACULUS_API_BASE = "https://www.metaculus.com/api"
_MIN_NUM_FORECASTERS = 5
_MAX_RESOLUTION_DATE_IN_DAYS = 365 * 2
_QUESTION_LIMIT = 2000
_MAX_PANDAS_TS = pd.Timestamp.max.tz_localize("UTC")

_CATEGORIES = [
    "artificial-intelligence",
    "computing-and-math",
    "elections",
    "environment-climate",
    "geopolitics",
    "health-pandemics",
    "law",
    "natural-sciences",
    "nuclear",
    "politics",
    "social-sciences",
    "space",
    "sports-entertainment",
    "technology",
]


class MetaculusSource(MarketSource):
    """Metaculus prediction market source."""

    name: ClassVar[str] = "metaculus"

    # ------------------------------------------------------------------
    # Public: fetch  (search endpoint only — discovers IDs)
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(self, *, today: date | None = None, **kwargs: Any) -> DataFrame[MetaculusFetchFrame]:
        """Discover eligible Metaculus question IDs via the search endpoint.

        Calls the search endpoint once without a category and once per category,
        filters by forecaster count and cp_reveal_time, and returns a DataFrame
        of IDs.

        Args:
            today (date | None): Reference date for the resolution window and
                cp_reveal filters. Defaults to today, computed once here and threaded
                through so every endpoint call shares the same reference instead of
                each recomputing "today" (keeps a run that straddles midnight idempotent).
        """
        self._require_api_key()
        if today is None:
            today = dates.get_date_today()

        discovered_ids = self._call_search_endpoint(today=today)
        for topic in _CATEGORIES:
            discovered_ids = discovered_ids.union(
                self._call_search_endpoint(today=today, additional_params={"categories": topic})
            )
        logger.info(f"Discovered {len(discovered_ids)} question IDs from search.")

        return pd.DataFrame({"id": sorted(discovered_ids)})

    # ------------------------------------------------------------------
    # Public: update  (per-question API calls + resolution file building)
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[MetaculusFetchFrame],
        *,
        existing_resolution_files: dict[str, DataFrame[ResolutionFrame]] | None = None,
        files_in_storage: list[str] | None = None,
    ) -> UpdateResult:
        """Fetch full question data and build resolution files.

        1. Append new IDs from dff to dfq (with resolved=False).
        2. For each unresolved question: call the per-question API, update dfq,
           build the resolution file.
        3. For resolved questions whose resolution file is missing from storage:
           regenerate it.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[MetaculusFetchFrame]): Discovered question IDs from fetch().
            existing_resolution_files (dict | None): Accepted for interface symmetry with
                other market sources; unused here because Metaculus rebuilds every
                resolution file from the full aggregation history on each run.
            files_in_storage (list[str] | None): Existing resolution file paths in storage,
                used to decide which resolved questions need regenerating.
        """
        self._require_api_key()
        resolution_files: dict[str, pd.DataFrame] = {}

        # Append new IDs to dfq
        new_ids = dff[~dff["id"].isin(dfq["id"])]["id"]
        if not new_ids.empty:
            df_new = pd.DataFrame({"id": new_ids}).assign(
                **{col: None for col in dfq.columns if col != "id"}
            )
            df_new["resolved"] = False
            df_new["freeze_datetime_value_explanation"] = "The community prediction."
            df_new["market_info_resolution_datetime"] = "N/A"

            # Cap new additions so the unresolved pool stays under _QUESTION_LIMIT
            max_to_add = _QUESTION_LIMIT - len(dfq[dfq["resolved"] == False])  # noqa: E712
            if max_to_add > 0:
                df_new = df_new.head(max_to_add)
                dfq = pd.concat([dfq, df_new], ignore_index=True)

        # Update all unresolved questions
        dfq["resolved"] = dfq["resolved"].astype(bool)
        for index, row in dfq[~dfq["resolved"]].iterrows():
            market = self._get_market(row["id"])

            # Update question fields in dfq
            dfq.at[index, "question"] = market["title"]
            dfq.at[index, "background"] = market["question"].get("description", "N/A")
            dfq.at[index, "market_info_resolution_criteria"] = market["question"].get(
                "resolution_criteria", "N/A"
            )
            dfq.at[index, "market_info_open_datetime"] = dates.convert_zulu_to_iso(
                market["question"]["open_time"]
            )
            dfq.at[index, "market_info_close_datetime"] = dates.convert_zulu_to_iso(
                market["question"]["actual_close_time"]
            )
            dfq.at[index, "url"] = f"https://www.metaculus.com/questions/{market['id']}"
            if market["resolved"]:
                dfq.at[index, "resolved"] = True
                dfq.at[index, "market_info_resolution_datetime"] = dates.convert_datetime_to_iso(
                    min(
                        dates.convert_zulu_to_datetime(market["question"]["actual_close_time"]),
                        dates.convert_zulu_to_datetime(market["question"]["actual_resolve_time"]),
                    )
                )
            dfq.at[index, "forecast_horizons"] = "N/A"

            # Build resolution file
            df_res = self._create_resolution_file(dfq, index, market)
            if df_res is not None:
                resolution_files[str(row["id"])] = df_res
                dfq.at[index, "freeze_datetime_value"] = (
                    df_res["value"].iloc[-1] if not df_res.empty else "N/A"
                )
            else:
                dfq.at[index, "freeze_datetime_value"] = "N/A"

        # Regenerate missing resolution files for resolved questions
        files_in_storage = files_in_storage or []
        for index, row in dfq[dfq["resolved"]].iterrows():
            question_id = str(row["id"])
            filename = f"{self.name}/{question_id}.jsonl"
            if filename not in files_in_storage and question_id not in resolution_files:
                market = self._get_market(row["id"])
                df_res = self._create_resolution_file(dfq, index, market)
                if df_res is not None:
                    resolution_files[question_id] = df_res

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: API calls
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=300,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _call_search_endpoint(
        self,
        *,
        today: date,
        additional_params: dict | None = None,
    ) -> set[str]:
        """Discover eligible question IDs via the Metaculus search endpoint.

        Calls GET /api/posts/ with filters for open binary questions in the
        resolution window. Filters results by forecaster count and cp_reveal_time.

        Args:
            today (date): Reference date for the resolution window and cp_reveal filters.
            additional_params (dict | None): Extra query params (e.g. {"categories": "..."}).
        """
        api_key = self._require_api_key()
        min_resolution_date = today + timedelta(days=question_curation.FREEZE_WINDOW_IN_DAYS)
        max_resolution_date = today + timedelta(days=_MAX_RESOLUTION_DATE_IN_DAYS)

        endpoint = f"{_METACULUS_API_BASE}/posts/"
        params = {
            "statuses": "open",
            "scheduled_resolve_time__gt": min_resolution_date.strftime("%Y-%m-%d"),
            "scheduled_resolve_time__lt": max_resolution_date.strftime("%Y-%m-%d"),
            "include_cp_history": "false",
            "include_descriptions": "false",
            "forecast_type": "binary",
            "order_by": "-hotness",
            "limit": 200,
        }
        if additional_params:
            params.update(additional_params)
            logger.info(f"Calling {endpoint} with additional params {additional_params}")

        headers = {"Authorization": f"Token {api_key}"}
        response = requests.get(endpoint, params=params, headers=headers, verify=certifi.where())
        if not response.ok:
            logger.error("Request to Metaculus API endpoint failed.")
            response.raise_for_status()

        ids: set[str] = set()
        for market in response.json()["results"]:
            if market["nr_forecasters"] > _MIN_NUM_FORECASTERS:
                if "cp_reveal_time" in market["question"]:
                    cp_reveal_date = market["question"]["cp_reveal_time"]
                    cp_reveal_date = datetime.strptime(cp_reveal_date[:10], "%Y-%m-%d").date()
                    if cp_reveal_date < today:
                        ids.add(str(market["id"]))

        return ids

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> int | None:
        """Parse Retry-After header, returning seconds or None."""
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None
        try:
            return int(retry_after)
        except ValueError:
            return None

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_market(self, market_id: str) -> dict:
        """Fetch full question data from the per-question endpoint.

        Handles 429 rate limiting with Retry-After header parsing.

        Args:
            market_id (str): Metaculus question/post ID.
        """
        api_key = self._require_api_key()
        endpoint = f"{_METACULUS_API_BASE}/posts/{market_id}/"
        headers = {"Authorization": f"Token {api_key}"}

        max_rate_limit_retries = 5
        for attempt in range(max_rate_limit_retries):
            logger.info(f"Calling market endpoint for {market_id}.")
            response = requests.get(endpoint, headers=headers, verify=certifi.where())

            if response.status_code != 429:
                break

            wait = self._parse_retry_after(response) or 10
            logger.warning(
                f"Received 429 for market {market_id} (attempt {attempt + 1}). "
                f"Sleeping for {wait} seconds."
            )
            time.sleep(wait)
        else:
            response.raise_for_status()

        if not response.ok:
            logger.error(
                f"Request to market endpoint failed for {market_id}: "
                f"{response.status_code} Error. {response.text}"
            )
            response.raise_for_status()

        return response.json()

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    @staticmethod
    def _get_resolved_market_value(market: dict) -> float:
        """Get the resolved value from a market's resolution field.

        "yes" -> 1, "no" -> 0, "ambiguous"/"annulled" -> NaN.

        Args:
            market (dict): Raw market response from Metaculus API.
        """
        resolution = market["question"]["resolution"].lower()
        assert resolution in {
            "yes",
            "no",
            "ambiguous",
            "annulled",
        }, f"Problem getting resolution value for market {market['id']}"

        if resolution == "yes":
            return 1
        if resolution == "no":
            return 0
        return np.nan

    def _create_resolution_file(
        self,
        dfq: pd.DataFrame,
        index: int,
        market: dict,
    ) -> pd.DataFrame | None:
        """Build the resolution file for a market from its aggregation history.

        Overwrites the resolution file entirely on each run (Metaculus returns the
        full aggregation history in one call, so there is no incremental fetch).

        Args:
            dfq (pd.DataFrame): Question bank (used for resolved status and resolution datetime).
            index (int): Row index in dfq for this question.
            market (dict): Raw market response from the per-question endpoint.
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
                            _MAX_PANDAS_TS,
                        ).to_pydatetime()
                        if forecast["end_time"] is not None
                        else dates.get_datetime_today()
                    ),
                    "value": forecast["centers"][0],
                }
                for forecast in market.get("question", {})
                .get("aggregations", {})
                .get("recency_weighted", {})
                .get("history", [])
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
            start=df["start_datetime"].min().date(),
            end=df["date"].max(),
            freq="D",
        )
        df_dates = pd.DataFrame(date_range, columns=["date"])
        df_dates["date"] = df_dates["date"].dt.date
        df = pd.merge(df_dates, df[["date", "value"]], on="date", how="left")
        df["value"] = df["value"].bfill()

        # The API can return forecast periods with end_times in the future. Drop future dates.
        df = df[df["date"] <= dates.get_date_yesterday()]
        if df.empty and not dfq.at[index, "resolved"]:
            return None

        # If resolved, add final resolution value
        if dfq.at[index, "resolved"]:
            resolved_date = pd.Timestamp(dfq.at[index, "market_info_resolution_datetime"]).date()
            df = df[df["date"] < resolved_date]
            df.loc[len(df)] = {
                "date": resolved_date,
                "value": self._get_resolved_market_value(market),
            }

        df["id"] = str(market["id"])
        df = df[["id", "date", "value"]]

        return self._finalize_resolution_df(df)

    @staticmethod
    def _finalize_resolution_df(df: pd.DataFrame) -> DataFrame[ResolutionFrame]:
        """Cast types and return as a validated ResolutionFrame.

        Unlike infer/manifold, Metaculus does not filter to the benchmark start date:
        the aggregation history is already bounded by the question's open window, and
        this preserves the legacy job's output exactly.

        Args:
            df (pd.DataFrame): Raw resolution data with id, date, value columns.
        """
        df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)
        return ResolutionFrame.validate(df)
