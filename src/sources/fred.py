"""FRED question source."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, ClassVar

import backoff
import pandas as pd
import pandera.pandas as pa
import requests
from dateutil.relativedelta import relativedelta
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import FredFetchFrame, QuestionFrame
from helpers import constants, data_utils, dates

from ._dataset import DatasetSource

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.stlouisfed.org"

# FRED throttles each API key at ~2 requests/second (120/minute); exceeding it
# returns HTTP 429. Space requests out to stay safely under that limit.
# See https://fred.stlouisfed.org/docs/api/fred/v2/errors.html
_MIN_REQUEST_INTERVAL = 0.6  # seconds between requests => ~1.6 req/s


class FredSource(DatasetSource):
    """Federal Reserve Economic Data source."""

    name: ClassVar[str] = "fred"

    def __init__(self) -> None:
        """Initialize with request-throttling state."""
        super().__init__()
        self._last_request_time = 0.0

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(
        self,
        *,
        dfq: DataFrame[QuestionFrame] | None = None,
    ) -> DataFrame[FredFetchFrame]:
        """Fetch FRED series data from the API.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
        """
        self._require_api_key()
        current_time = dates.get_datetime_now()
        yesterday = dates.get_date_today() - timedelta(days=1)
        nullified_ids = self.get_nullified_ids()

        # Build combined question dict: predefined series list + existing bank-only series.
        # self.questions is auto-populated from SOURCE_METADATA["fred"]["questions"].
        fred_questions = {q["id"]: q for q in self.questions}

        questions_bank_dict: dict[str, dict] = {}
        if dfq is not None and not dfq.empty:
            # Drop nullified series so no future question sets are built on them.
            # Pre-cutoff forecasts already submitted on these ids still resolve via dfr
            # in the dataset resolve path, which does not depend on dfq membership.
            dfq_filtered = dfq[~dfq["id"].isin(nullified_ids)]
            dfq_dict = {q["id"]: q for q in dfq_filtered.to_dict(orient="records")}
            questions_bank_dict = {id: dfq_dict[id] for id in dfq_dict if id not in fred_questions}

        logger.info(f"# of questions in the new list: {len(fred_questions)}")
        logger.info(
            f"# of questions in the bank but not in the new list: {len(questions_bank_dict)}"
        )

        combined_questions = self._combine_dicts(fred_questions, questions_bank_dict)
        logger.info(f"# of combined questions: {len(combined_questions)}")

        # Fetch API data for each series.
        ids_to_delete = []
        for series_id in combined_questions:
            combined_questions[series_id]["release"] = self._fetch_release(series_id)
            combined_questions[series_id]["series"] = self._fetch_series_info(series_id)
            combined_questions[series_id]["observations"] = self._fetch_observations(series_id)

            if not combined_questions[series_id]["observations"]:
                ids_to_delete.append(series_id)
            else:
                combined_questions[series_id]["observations"] = self._forward_fill_observations(
                    combined_questions[series_id]["observations"], yesterday
                )

        logger.info(
            f"questions-to-delete cnt because no observations fetched: {len(ids_to_delete)}"
        )
        for id in ids_to_delete:
            del combined_questions[id]

        # Transform to FredFetchFrame rows.
        rows = [
            self._transform_series(
                series_id=series_id,
                combined_question=combined_questions[series_id],
                current_time=current_time,
            )
            for series_id in combined_questions
        ]

        logger.info(f"Final questions count: {len(rows)}")
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[FredFetchFrame],
        **kwargs: Any,
    ) -> UpdateResult:
        """Process fetched data into updated questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[FredFetchFrame]): Freshly fetched data.
        """
        # Drop nullified series so no future question sets are built on them.
        # Pre-cutoff forecasts already submitted on these ids still resolve via dfr
        # in the dataset resolve path, which does not depend on dfq membership.
        nullified_ids = self.get_nullified_ids()
        dfq = dfq[~dfq["id"].isin(nullified_ids)].copy()
        resolution_files: dict[str, pd.DataFrame] = {}

        for question in dff.to_dict("records"):
            question_id = str(question["id"])

            # Build resolution file from embedded observations.
            df_res = pd.DataFrame(question["resolutions"])
            df_res = df_res[["id", "date", "value"]]
            resolution_files[question_id] = df_res

            # Strip transient fields (not part of QuestionFrame).
            del question["fetch_datetime"]
            del question["probability"]
            del question["resolutions"]

            # Upsert into dfq.
            if question["id"] in dfq["id"].values:
                dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
                for key, value in question.items():
                    dfq.at[dfq_index, key] = value
            else:
                dfq = pd.concat([dfq, pd.DataFrame([question])], ignore_index=True)

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: request throttling
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep if needed so consecutive FRED requests stay under ~2 req/s."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Private: API calls
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=300,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _fetch_paginated_data(
        self,
        url: str,
        params: dict,
        field_name: str,
        pagination: bool | int,
    ) -> list:
        """Fetch data from a paginated FRED API endpoint.

        Args:
            url (str): The API endpoint URL.
            params (dict): Parameters for the request.
            field_name (str): Key in response JSON containing the data.
            pagination (bool | int): False=1 page, True=all, int=up to N pages.
        """
        all_data = []
        params["offset"] = 0
        pages_fetched = 0

        while True:
            self._throttle()
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data.get(field_name, []):
                break

            all_data.extend(data[field_name])
            pages_fetched += 1

            if pagination is False:
                break
            elif isinstance(pagination, int) and pagination > 1 and pages_fetched >= pagination:
                break

            params["offset"] += params["limit"]

        return all_data

    def _fetch_release(self, series_id: str) -> dict:
        """Fetch release info for a single series.

        Args:
            series_id (str): FRED series ID.
        """
        api_key = self._require_api_key()
        params = {
            "api_key": api_key,
            "file_type": "json",
            "series_id": series_id,
        }
        results = self._fetch_paginated_data(
            url=f"{_BASE_URL}/fred/series/release?",
            params=params,
            field_name="releases",
            pagination=False,
        )
        return results[0]

    def _fetch_series_info(self, series_id: str) -> list[dict]:
        """Fetch series metadata for a single series.

        Args:
            series_id (str): FRED series ID.
        """
        api_key = self._require_api_key()
        params = {
            "api_key": api_key,
            "file_type": "json",
            "series_id": series_id,
        }
        return self._fetch_paginated_data(
            url=f"{_BASE_URL}/fred/series?",
            params=params,
            field_name="seriess",
            pagination=False,
        )

    def _fetch_observations(self, series_id: str) -> list[dict] | None:
        """Fetch all observations for a series.

        Returns the cleaned observations, or None when the series' most recent
        observation is more than a month stale (some monthly series lag).

        Args:
            series_id (str): FRED series ID.
        """
        api_key = self._require_api_key()
        params = {
            "api_key": api_key,
            "file_type": "json",
            "series_id": series_id,
            "limit": 10000,
        }
        observations = self._fetch_paginated_data(
            url=f"{_BASE_URL}/fred/series/observations?",
            params=params,
            field_name="observations",
            pagination=True,
        )

        current_dt = datetime.strptime(str(dates.get_date_today()), "%Y-%m-%d")
        fetch_dt = datetime.strptime(observations[-1]["date"], "%Y-%m-%d")

        # Safety check: the latest record must be at least from last month.
        one_month_ago = current_dt - relativedelta(months=1)
        is_at_least_last_month = one_month_ago <= fetch_dt

        if is_at_least_last_month and len(observations) > 0:
            # Filter out missing values and convert to float.
            return [
                {
                    "id": series_id,
                    "date": observation["date"],
                    "value": float(observation["value"]),
                }
                for observation in observations
                if observation["value"] != "."
            ]

        return None

    # ------------------------------------------------------------------
    # Private: data transformation
    # ------------------------------------------------------------------

    @staticmethod
    def _combine_dicts(dict1: dict, dict2: dict) -> dict:
        """Combine two dicts of dicts, merging nested values.

        Args:
            dict1 (dict): Primary dict (e.g. new hardcoded fred_questions).
            dict2 (dict): Secondary dict (e.g. bank-only questions).
        """
        combined: dict = {}
        for key, value in dict1.items():
            if key not in combined:
                combined[key] = {}
            combined[key].update(value)
        for key, value in dict2.items():
            if key not in combined:
                combined[key] = {}
            combined[key].update(value)
        return combined

    @staticmethod
    def _forward_fill_observations(observations: list[dict], yesterday) -> list[dict]:
        """Forward-fill missing dates in observations up to yesterday.

        Args:
            observations (list[dict]): Raw observations with {id, date, value}.
            yesterday (date): Fill dates up to this date.
        """
        df = pd.DataFrame(observations)
        df["date"] = pd.to_datetime(df["date"].str[:10])
        df.drop_duplicates(subset=["date"], keep="last", inplace=True)

        all_dates = pd.date_range(start=df["date"].min(), end=yesterday, freq="D")
        df = df.set_index("date").reindex(all_dates, method="ffill").reset_index()
        df.rename(columns={"index": "date"}, inplace=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        return df[["id", "date", "value"]].to_dict(orient="records")

    @staticmethod
    def _transform_series(
        series_id: str,
        combined_question: dict,
        current_time: str,
    ) -> dict:
        """Transform fetched series data into a FredFetchFrame row.

        Args:
            series_id (str): FRED series ID.
            combined_question (dict): Merged question data with release, series, observations.
            current_time (str): ISO timestamp for fetch_datetime.
        """
        observations = combined_question["observations"]
        current_value = observations[-1]["value"]
        release = combined_question["release"]
        series = combined_question["series"][0]

        # Question text: use series_name from the hardcoded list, else existing question text.
        if "series_name" in combined_question:
            series_name = combined_question["series_name"]
            question = (
                f"Will {series_name} have increased by "
                "{resolution_date} as compared to its value on {forecast_due_date}?"
            )
        else:
            question = combined_question["question"]

        return {
            "id": series_id,
            "question": question,
            "background": (
                f"The notes from the release: {release.get('notes', 'N/A')}. "
                f" The notes from the series: {series.get('notes', 'N/A')}. "
                " Additional background of the series: "
                f" 1. the units of the series: {series.get('units', 'N/A')}. "
                " 2. the seasonal adjustments of the series: "
                f" {series.get('seasonal_adjustment', 'N/A')} "
                f" 3. the update frequency: {series.get('frequency', 'N/A')} "
            ),
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": "N/A",
            "market_info_close_datetime": "N/A",
            "url": f"https://fred.stlouisfed.org/series/{series_id}",
            "resolved": False,
            "market_info_resolution_datetime": "N/A",
            "fetch_datetime": current_time,
            "probability": current_value,
            "forecast_horizons": (
                constants.FORECAST_HORIZONS_IN_DAYS
                if series["frequency_short"] != "M"
                else constants.FORECAST_HORIZONS_IN_DAYS[1:]
            ),
            "freeze_datetime_value": current_value,
            "freeze_datetime_value_explanation": (
                f"The latest value released in "
                f"{series['title']} from the "
                f"release {release['name']}."
            ),
            "resolutions": observations,
        }
