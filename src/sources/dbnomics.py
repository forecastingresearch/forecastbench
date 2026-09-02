"""DBnomics question source."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, ClassVar

import backoff
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import DbnomicsFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates

from ._dataset import DatasetSource
from .dbnomics_questions import ECB_QUESTIONS, METEOFRANCE_STATIONS

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.db.nomics.world/v22/series/"
_PROVIDER_NAMES = {
    "ECB": "European Central Bank",
    "meteofrance": "Météo-France",
}

# Some dataseries with regular updates have large numbers of NA values during periods in which
# data is not being reported. _OBSERVATIONS_WITHOUT_DATA detects these quiet periods and excludes
# the series from being formed into a question during them (since it's unclear if we'll be able to
# resolve them and the freeze values become increasingly irrelevant).
_OBSERVATIONS_WITHOUT_DATA = 10

_QUESTION_TEMPLATES = {
    "meteofrance": (
        "What is the probability that the daily average temperature at the French weather station "
        "at {station} will be higher on {resolution_date} than on {forecast_due_date}?"
    ),
    "ecb": (
        "What is the probability that the value of the latest observation dated on or before "
        "{resolution_date} for the European Central Bank time series “{question_subject}” "
        "will exceed the value of its latest observation dated on or before "
        "{forecast_due_date}?"
    ),
}

_VALUE_EXPLANATIONS = {
    "meteofrance": "The daily average temperature at the French weather station at {station}.",
    "ecb": (
        "The latest value published on or before the question-set freeze date for "
        "“{question_subject}” in the European Central Bank’s “{dataset_name}” dataset."
    ),
}


def _create_meteofrance_questions(stations: list[dict]) -> list[dict]:
    """Convert station data into the question config consumed by fetch and update.

    Args:
        stations (list[dict]): MeteoFrance station entries with ``id`` and ``station``.
    """
    questions = []
    for item in stations:
        station_id = item["id"]
        station = item["station"]
        question_text = _QUESTION_TEMPLATES["meteofrance"].replace("{station}", station)
        explanation = _VALUE_EXPLANATIONS["meteofrance"].format(station=station)
        questions.append(
            {
                "id": f"meteofrance/TEMPERATURE/celsius.{station_id}.D",
                "question_text": question_text,
                "freeze_datetime_value_explanation": explanation,
                "fill_missing_dates": False,
            }
        )
    return questions


def _create_ecb_questions(series: list[dict[str, str]]) -> list[dict]:
    """Convert curated ECB series into the question config consumed by fetch and update.

    Args:
        series (list[dict[str, str]]): ECB series IDs and question metadata.
    """
    questions = []
    for item in series:
        question_text = _QUESTION_TEMPLATES["ecb"].replace(
            "{question_subject}", item["question_subject"]
        )
        explanation = _VALUE_EXPLANATIONS["ecb"].format(
            question_subject=item["question_subject"],
            dataset_name=item["dataset_name"],
        )
        questions.append(
            {
                "id": item["id"],
                "question_text": question_text,
                "freeze_datetime_value_explanation": explanation,
                "fill_missing_dates": True,
            }
        )
    return questions


DBNOMICS_QUESTIONS = _create_meteofrance_questions(METEOFRANCE_STATIONS) + _create_ecb_questions(
    ECB_QUESTIONS
)


class DbnomicsSource(DatasetSource):
    """DBnomics economic data source."""

    name: ClassVar[str] = "dbnomics"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(self, **kwargs: Any) -> DataFrame[DbnomicsFetchFrame]:
        """Fetch DBnomics series data from the API."""
        self._require_api_key()
        # Compute 'today' once and thread it to every series call so a run straddling midnight
        # uses one consistent upper bound across all of its requests.
        today = dates.get_date_today()
        logger.info("Downloading DBnomics data.")

        frames = []
        for row in DBNOMICS_QUESTIONS:
            new_rows = self._call_endpoint(id=row["id"], today=today)
            if new_rows is not None:
                frames.append(new_rows)

        if not frames:
            raise RuntimeError("No DBnomics series returned usable observations.")

        df = pd.concat(frames, ignore_index=True)
        df["period"] = df["period"].astype(str)
        return df

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[DbnomicsFetchFrame],
        **kwargs: Any,
    ) -> UpdateResult:
        """Process fetched data into updated questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[DbnomicsFetchFrame]): Freshly fetched data.
        """
        # Reproduce the legacy FETCH_COLUMN_DTYPE read: id/period/value are strings. Without this
        # the value column would be inferred as floats and resolution files would store JSON
        # numbers instead of the strings ("NA"/"12.3") the legacy job wrote.
        dff = dff.copy()
        dff[["id", "period", "value"]] = dff[["id", "period", "value"]].astype(str)

        yesterday = dates.get_date_yesterday()
        resolution_files: dict[str, pd.DataFrame] = {}

        new_series = None
        for row in DBNOMICS_QUESTIONS:
            id = row["id"].replace("/", "_")
            df_series = dff[dff["id"] == id]
            if df_series.empty:
                logger.warning(f"Skipping DBnomics series {row['id']}: no fetched observations.")
                continue

            fill_through = yesterday if row["fill_missing_dates"] else None
            resolution_files[id] = self._build_resolution_df(df_series, fill_through=fill_through)

            provider_name = df_series["provider_name"].iloc[0]
            dataset_name = df_series["dataset_name"].iloc[0]
            series_name = df_series["series_name"].iloc[0]
            question = row["question_text"]
            url = f"https://db.nomics.world/{row['id']}"
            background = (
                f"The history of {dataset_name} - {series_name} from {provider_name} is available "
                f"at {url}."
            )
            freeze_datetime_value_explanation = row["freeze_datetime_value_explanation"]
            series_values = df_series["value"]

            if (series_values.tail(_OBSERVATIONS_WITHOUT_DATA) != "NA").any():
                freeze_datetime_value = float(series_values[series_values != "NA"].iloc[-1])
                new_row = {
                    "id": id,
                    "question": question,
                    "background": background,
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "url": url,
                    "market_info_resolution_datetime": "N/A",
                    "resolved": False,
                    "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                    "freeze_datetime_value": freeze_datetime_value,
                    "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
                }
                new_row = pd.DataFrame([new_row])
                if id not in dfq["id"].tolist():
                    new_series = (
                        new_row
                        if new_series is None
                        else pd.concat([new_series, new_row], ignore_index=True)
                    )
                else:
                    dfq.loc[dfq["id"] == id, "freeze_datetime_value"] = float(
                        series_values[series_values != "NA"].iloc[-1]
                    )
                    dfq.loc[dfq["id"] == id, "url"] = url
                    dfq.loc[dfq["id"] == id, "background"] = background

        if new_series is not None:
            dfq = pd.concat([dfq, new_series])

        logger.info(f"Found {len(dfq):,} questions of {len(DBNOMICS_QUESTIONS):,} possible.")

        return UpdateResult(dfq=dfq, resolution_files=resolution_files)

    # ------------------------------------------------------------------
    # Private: API calls
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=300,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _call_endpoint(self, id: str, today) -> pd.DataFrame | None:
        """Fetch a single DBnomics series and return its observation rows (or None if empty).

        Args:
            id (str): DBnomics series ID (with ``/`` separators).
            today (date): Exclusive upper bound for observation periods.
        """
        logger.info(f"Calling DBnomics for series {id}")
        endpoint = _BASE_URL + id
        api_key = self._require_api_key()
        # metadata=0 omits large provider and dataset metadata objects that would otherwise bloat
        # every response; the series document still contains the compact labels used below.
        params = {"observations": 1, "metadata": 0}
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(url=endpoint, params=params, headers=headers, timeout=(10, 60))
        if not response.ok:
            logger.error("Request to DBnomics API endpoint failed.")
            response.raise_for_status()
        data = response.json()
        docs = data.get("series", {}).get("docs", [{}])[0]
        id_safe = id.replace("/", "_")
        df = pd.DataFrame(
            {
                "id": id_safe,
                "period": docs.get("period"),
                "value": docs.get("value"),
                "provider_name": _PROVIDER_NAMES[docs.get("provider_code")],
                "dataset_name": docs.get("dataset_name"),
                "series_name": docs.get("series_name"),
            }
        )
        df["period"] = pd.to_datetime(df["period"]).dt.date
        # Filter to record start date and beyond.
        df = df[
            (df["period"] >= constants.QUESTION_BANK_DATA_STORAGE_START_DATE)
            & (df["period"] < today)
        ].reset_index(drop=True)
        return df if not df.empty else None

    # ------------------------------------------------------------------
    # Private: resolution dataframe building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_resolution_df(
        df: pd.DataFrame, fill_through: date | None = None
    ) -> DataFrame[ResolutionFrame]:
        """Build a resolution DataFrame ([id, date, value]) for a single series.

        Args:
            df (pd.DataFrame): Fetched rows for this series.
            fill_through (date | None): If set, carry observations forward through this date.
        """
        df = df[["id", "period", "value"]].rename(columns={"period": "date"})
        if fill_through is not None:
            df["date"] = pd.to_datetime(df["date"])
            # Match other dataset sources by treating the provider's missing-value sentinel as
            # missing before expanding to calendar dates and carrying the latest value forward.
            df["value"] = df["value"].replace("NA", pd.NA)
            dates_to_fill = pd.date_range(df["date"].min(), fill_through, freq="D")
            df = df.set_index("date").reindex(dates_to_fill).ffill()
            df["value"] = df["value"].fillna("NA")
            df.index.name = "date"
            df = df.reset_index()
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df = df.astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)
        df["value"] = df["value"].replace("NA", "N/A")
        return df
