"""INFER question source."""

from __future__ import annotations

import logging
import time
from datetime import timedelta, timezone
from typing import Any, ClassVar

import backoff
import certifi
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import InferFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates

from ._market import MarketSource

logger = logging.getLogger(__name__)

_INFER_URL = "https://www.randforecastinginitiative.org"


class InferSource(MarketSource):
    """INFER Public prediction market source."""

    name: ClassVar[str] = "infer"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(
        self,
        *,
        dfq: DataFrame[QuestionFrame] | None = None,
        files_in_storage: list[str] | None = None,
    ) -> DataFrame[InferFetchFrame]:
        """Fetch questions from the INFER API.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
            files_in_storage (list[str] | None): Existing resolution file paths.
        """
        self._require_api_key()
        files_in_storage = files_in_storage or []

        # Determine which existing questions need re-fetching
        resolved_ids: list[str] = []
        unresolved_ids: list[str] = []
        if dfq is not None and not dfq.empty:
            resolved_ids = dfq[dfq["resolved"]]["id"].tolist()
            unresolved_ids = dfq[~dfq["resolved"]]["id"].tolist()

        logger.info(f"Number resolved_ids: {len(resolved_ids)}")
        logger.info(f"Number unresolved_ids: {len(unresolved_ids)}")

        resolved_ids_without_files = [
            id for id in resolved_ids if f"{self.name}/{id}.jsonl" not in files_in_storage
        ]
        logger.info(f"resolved_ids_without_resolution_files: {resolved_ids_without_files}")

        all_existing_ids_to_fetch = unresolved_ids + resolved_ids_without_files

        # Fetch existing (potentially closed) questions
        all_existing_questions = (
            self._fetch_questions_from_api(status="all", question_ids=all_existing_ids_to_fetch)
            if all_existing_ids_to_fetch
            else []
        )

        # Fetch all active questions
        all_active_questions = self._fetch_questions_from_api()

        # Filter active to binary questions with predictions
        all_active_binary_questions = [
            q
            for q in all_active_questions
            if q["state"] == "active"
            and q["type"] == "Forecast::YesNoQuestion"
            and q["answers"][0]["predictions_count"] > 0
        ]

        # Deduplicate: active takes precedence
        active_ids = {q["id"] for q in all_active_binary_questions}
        all_existing_questions = [q for q in all_existing_questions if q["id"] not in active_ids]

        all_questions = all_active_binary_questions + all_existing_questions
        logger.info(f"Number of questions fetched: {len(all_questions)}")

        # Transform to InferFetchFrame schema
        current_time = dates.get_datetime_now()
        rows = [self._transform_question(q, current_time) for q in all_questions]

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[InferFetchFrame],
        *,
        existing_resolution_files: dict[str, DataFrame[ResolutionFrame]] | None = None,
    ) -> UpdateResult:
        """Process fetched data into updated questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[InferFetchFrame]): Freshly fetched data.
            existing_resolution_files (dict | None): Per-question existing resolution data.
        """
        self._require_api_key()
        existing_resolution_files = existing_resolution_files or {}
        resolution_files: dict[str, pd.DataFrame] = {}

        for question in dff.to_dict("records"):
            question_id = str(question["id"])

            # Build/update resolution file
            existing_df = existing_resolution_files.get(question_id)
            df_res = self._build_resolution_file(
                question=question,
                resolved=question["resolved"],
                existing_df=existing_df,
            )
            resolution_files[question_id] = df_res

            # Mark nullified questions as resolved
            if question["nullify_question"]:
                question["resolved"] = True

            # Strip transient fields (not part of QuestionFrame)
            del question["fetch_datetime"]
            del question["probability"]
            del question["nullify_question"]

            # Upsert into dfq
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
    # Private: API calls
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=300,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _fetch_questions_from_api(
        self,
        *,
        status: str = "active",
        question_ids: list[str] | None = None,
    ) -> list[dict]:
        """Fetch paginated questions from the INFER API.

        Args:
            status (str): "active" or "all".
            question_ids (list[str] | None): If provided, fetch these specific IDs.
        """
        api_key = self._require_api_key()
        endpoint = _INFER_URL + "/api/v1/questions"
        headers = {"Authorization": f"Bearer {api_key}"}
        params: dict[str, Any] = {"page": 0, "status": status}
        if question_ids is not None:
            params.update({"status": "all", "ids": ",".join(sorted(question_ids))})

        questions: list[dict] = []
        seen_ids: set = set()
        while True:
            response = requests.get(
                endpoint, params=params, headers=headers, verify=certifi.where()
            )
            if not response.ok:
                logger.error(f"Request to Infer questions endpoint failed with params: {params}")
                response.raise_for_status()

            new_questions = response.json().get("questions", [])
            if not new_questions:
                break

            for q in new_questions:
                if q["id"] not in seen_ids:
                    questions.append(q)
                    seen_ids.add(q["id"])

            params["page"] += 1

        return questions

    def _get_historical_forecasts(
        self,
        current_df: DataFrame[ResolutionFrame] | None,
        question_id: str,
    ) -> DataFrame[ResolutionFrame]:
        """Fetch historical prediction time series for a question.

        Args:
            current_df (DataFrame[ResolutionFrame] | None): Existing resolution data.
            question_id (str): INFER question ID.
        """
        api_key = self._require_api_key()
        endpoint = _INFER_URL + "/api/v1/prediction_sets"
        params = {"question_id": question_id, "page": 0}
        headers = {"Authorization": f"Bearer {api_key}"}
        all_responses: list[dict] = []
        current_time = dates.get_datetime_today_midnight()

        # Determine cutoff: only fetch predictions newer than what we have
        has_existing = current_df is not None and not current_df.empty
        last_date = (
            pd.to_datetime(current_df["date"].iloc[-1]).tz_localize("UTC")
            if has_existing
            else constants.BENCHMARK_START_DATE_DATETIME.replace(tzinfo=timezone.utc)
        )

        while True:
            try:
                logger.info(f"Fetched page: {params['page']}, for question ID: {question_id}")
                response = requests.get(
                    endpoint, params=params, headers=headers, verify=certifi.where()
                )
                response.raise_for_status()
                new_responses = response.json().get("prediction_sets", [])
                all_responses.extend(new_responses)
                if (
                    not new_responses
                    or pd.to_datetime(new_responses[-1]["created_at"], utc=True) <= last_date
                ):
                    break
                params["page"] += 1
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 429:
                    raise
                logger.error("Rate limit reached, waiting 10s before retrying...")
                time.sleep(10)

        # Extract (date, probability) from each prediction set
        all_forecasts: list[tuple] = []
        for forecast in all_responses:
            if not has_existing or pd.to_datetime(forecast["created_at"], utc=True) > last_date:
                if len(forecast["predictions"]) == 2:
                    forecast_yes = forecast["predictions"][0]
                    if forecast_yes["answer_name"] == "No":
                        forecast_yes = forecast["predictions"][1]
                elif len(forecast["predictions"]) == 1:
                    forecast_yes = forecast["predictions"][0]

                all_forecasts.append(
                    (
                        dates.convert_zulu_to_iso(forecast["created_at"]),
                        forecast_yes["final_probability"],
                    )
                )

        df = pd.DataFrame(all_forecasts, columns=["date", "value"])
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.date < current_time.date()]
        df["value"] = df["value"].astype(float)
        df["id"] = question_id

        # Sort and convert to date-only
        df_sorted = df.sort_values("date").reset_index(drop=True)
        df_sorted["date"] = df_sorted["date"].dt.date
        df_final = df_sorted[["id", "date", "value"]]

        # Merge with existing data
        if not has_existing:
            result_df = df_final.drop_duplicates(subset=["id", "date"], keep="last")
        else:
            current_df = current_df.copy()
            current_df["date"] = pd.to_datetime(current_df["date"]).dt.date
            current_df_final = current_df[["id", "date", "value"]]
            result_df = (
                pd.concat([current_df_final, df_final], axis=0)
                .sort_values(by=["date"], ascending=True)
                .drop_duplicates(subset=["id", "date"], keep="last")
                .reset_index(drop=True)
            )

        # Forward-fill missing dates
        result_df.loc[:, "date"] = pd.to_datetime(result_df["date"]).dt.tz_localize("UTC")
        result_df = result_df.infer_objects()
        result_df = result_df.sort_values(by="date")
        all_dates = pd.date_range(
            start=result_df["date"].min(),
            end=current_time - timedelta(days=1),
            freq="D",
        )
        result_df = result_df.set_index("date").reindex(all_dates, method="ffill").reset_index()
        result_df["id"] = question_id
        result_df.reset_index(inplace=True)
        result_df.rename(columns={"index": "date"}, inplace=True)

        return result_df[["id", "date", "value"]]

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    def _build_resolution_file(
        self,
        question: dict,
        resolved: bool,
        existing_df: DataFrame[ResolutionFrame] | None = None,
    ) -> DataFrame[ResolutionFrame]:
        """Build or update a resolution file for a single question.

        Args:
            question (dict): Must have 'id', 'nullify_question'. If resolved, must also
                have 'market_info_resolution_datetime' and 'probability'.
            resolved (bool): Whether the question has resolved.
            existing_df (DataFrame[ResolutionFrame] | None): Existing resolution data.
        """
        yesterday = dates.get_datetime_today_midnight() - timedelta(days=1)

        # --- Nullification ---
        if question["nullify_question"]:
            logger.warning(
                f"Nullifying question {question['id']}. "
                "Pushing np.nan values to resolution file."
            )
            if existing_df is None or existing_df.empty:
                return pd.DataFrame(
                    {
                        "id": [question["id"]],
                        "date": [str(yesterday.date())],
                        "value": [np.nan],
                    }
                )
            else:
                df = existing_df.copy()
                df["value"] = np.nan
                return self._finalize_resolution_df(df)

        # --- Already up-to-date check ---
        if (
            existing_df is not None
            and not existing_df.empty
            and pd.to_datetime(existing_df["date"].iloc[-1]).tz_localize("UTC") >= yesterday
        ):
            logger.info(f"{question['id']} is skipped because it's already up-to-date!")
            return existing_df

        # --- Fetch historical forecasts ---
        df = self._get_historical_forecasts(existing_df, question["id"])
        df["date"] = df["date"].dt.date if hasattr(df["date"].dtype, "tz") else df["date"]

        # --- Handle resolved questions ---
        if resolved:
            resolution_date_str = question["market_info_resolution_datetime"][:10]
            resolution_date = pd.to_datetime(resolution_date_str)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] < resolution_date]
            resolution_row = pd.DataFrame(
                {
                    "id": [question["id"]],
                    "date": [resolution_date_str],
                    "value": [question["probability"]],
                }
            )
            df = pd.concat([df, resolution_row], ignore_index=True)

        return self._finalize_resolution_df(df)

    @staticmethod
    def _finalize_resolution_df(df: pd.DataFrame) -> DataFrame[ResolutionFrame]:
        """Apply date filtering and return as validated ResolutionFrame.

        Args:
            df (pd.DataFrame): Raw resolution data with id, date, value columns.
        """
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.date >= constants.BENCHMARK_START_DATE_DATETIME_DATE]
        return ResolutionFrame.validate(df[["id", "date", "value"]])

    # ------------------------------------------------------------------
    # Private: question transformation
    # ------------------------------------------------------------------

    @staticmethod
    def _transform_question(q: dict, current_time: str) -> dict:
        """Transform a single INFER API response to InferFetchFrame row.

        Args:
            q (dict): Raw question dict from the INFER API.
            current_time (str): ISO timestamp for fetch_datetime.
        """
        nullify_question = q["type"] != "Forecast::YesNoQuestion"

        # --- Close datetime: min(scoring_end_time, ends_at) ---
        scoring_end_time_str = (
            dates.convert_datetime_str_to_iso_utc(q["scoring_end_time"])
            if q["scoring_end_time"]
            else "N/A"
        )
        ended_at_str = dates.convert_zulu_to_iso(q["ends_at"]) if q["ends_at"] else "N/A"
        final_closed_at_str = (
            "N/A"
            if scoring_end_time_str == "N/A" and ended_at_str == "N/A"
            else (
                ended_at_str
                if scoring_end_time_str == "N/A"
                else (
                    scoring_end_time_str
                    if ended_at_str == "N/A"
                    else min(scoring_end_time_str, ended_at_str)
                )
            )
        )

        # --- Open datetime ---
        scoring_start_time_str = (
            dates.convert_datetime_str_to_iso_utc(q["scoring_start_time"])
            if q["scoring_start_time"]
            else "N/A"
        )

        # --- Resolution datetime: min(resolved_at, close_datetime) ---
        resolved_at_str = dates.convert_zulu_to_iso(q["resolved_at"]) if q["resolved_at"] else "N/A"
        final_resolved_str = (
            "N/A"
            if resolved_at_str == "N/A" and final_closed_at_str == "N/A"
            else (
                final_closed_at_str
                if resolved_at_str == "N/A"
                else (
                    resolved_at_str
                    if final_closed_at_str == "N/A"
                    else min(resolved_at_str, final_closed_at_str)
                )
            )
        )

        # --- Probability ---
        forecast_yes: Any = "N/A"
        if len(q["answers"]) == 2 and not nullify_question:
            yes_index = 0 if q["answers"][0]["name"].lower() == "yes" else 1
            forecast_yes = q["answers"][yes_index]["probability"]

        return {
            "id": str(q["id"]),
            "question": q["name"],
            "background": q["description"],
            "market_info_resolution_criteria": (
                " ".join([content["content"] for content in q["clarifications"]])
                if q["clarifications"]
                else "N/A"
            ),
            "market_info_open_datetime": scoring_start_time_str,
            "market_info_close_datetime": final_closed_at_str,
            "url": f"{_INFER_URL}/questions/{q['id']}",
            "resolved": q.get("resolved?", False),
            "market_info_resolution_datetime": (
                "N/A" if not q.get("resolved?", False) else final_resolved_str
            ),
            "fetch_datetime": current_time,
            "probability": forecast_yes,
            "forecast_horizons": "N/A",
            "freeze_datetime_value": forecast_yes,
            "freeze_datetime_value_explanation": "The crowd forecast.",
            "nullify_question": nullify_question,
        }
