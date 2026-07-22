"""DBnomics question source."""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.db.nomics.world/v22/series/"

# Some dataseries with regular updates have large numbers of NA values during periods in which
# data is not being reported. _OBSERVATIONS_WITHOUT_DATA detects these quiet periods and excludes
# the series from being formed into a question during them (since it's unclear if we'll be able to
# resolve them and the freeze values become increasingly irrelevant).
_OBSERVATIONS_WITHOUT_DATA = 10

_METEOFRANCE_STATIONS = [
    {"id": "07005", "station": "Abbeville"},
    {"id": "07015", "station": "Lille Airport"},
    {"id": "07020", "station": "Pointe De La Hague"},
    {"id": "07027", "station": "Caen – Carpiquet Airport"},
    {"id": "07037", "station": "Rouen Airport"},
    {"id": "07072", "station": "Reims – Prunay Aerodrome"},
    {"id": "07110", "station": "Brest Bretagne Airport"},
    {"id": "07117", "station": "Ploumanac'h"},
    {"id": "07130", "station": "Rennes–Saint-Jacques Airport"},
    {"id": "07139", "station": "Alençon"},
    {"id": "07149", "station": "Orly"},
    {"id": "07168", "station": "Troyes-Barberey Airport"},
    {"id": "07181", "station": "Nancy – Ochey Air Base"},
    {"id": "07190", "station": "Strasbourg Airport"},
    {"id": "07222", "station": "Nantes Atlantique Airport"},
    {"id": "07240", "station": "Tours"},
    {"id": "07255", "station": "Bourges"},
    {"id": "07280", "station": "Dijon-Bourgogne Airport"},
    {"id": "07299", "station": "EuroAirport Basel Mulhouse Freiburg"},
    {"id": "07335", "station": "Poitiers–Biard Airport"},
    {"id": "07434", "station": "Limoges – Bellegarde Airport"},
    {"id": "07460", "station": "Clermont-Ferrand Auvergne Airport"},
    {"id": "07471", "station": "Le Puy – Loudes Airport"},
    {"id": "07481", "station": "Lyon–Saint Exupéry Airport"},
    {"id": "07510", "station": "Bordeaux–Mérignac Airport"},
    {"id": "07535", "station": "Gourdon"},
    {"id": "07558", "station": "Millau"},
    {"id": "07577", "station": "Montélimar"},
    {"id": "07591", "station": "Embrun"},
    {"id": "07607", "station": "Mont-de-Marsan"},
    {"id": "07621", "station": "Tarbes–Lourdes–Pyrénées Airport"},
    {"id": "07627", "station": "Saint-Girons"},
    {"id": "07630", "station": "Toulouse–Blagnac Airport"},
    {"id": "07650", "station": "Marignane"},
    {"id": "07690", "station": "Nice"},
    {"id": "07747", "station": "Perpignan"},
    {"id": "07761", "station": "Ajaccio"},
    {"id": "61968", "station": "Glorioso Islands"},
    {"id": "61970", "station": "Juan de Nova Island"},
    {"id": "61972", "station": "Europa Island"},
    {"id": "61976", "station": "Tromelin Island"},
    {"id": "61980", "station": "Roland Garros Airport"},
    {"id": "61996", "station": "Amsterdam Island"},
    {"id": "61997", "station": "Île de la Possession"},
    {"id": "61998", "station": "Grande Terre"},
    {"id": "67005", "station": "Pamandzi"},
    {"id": "71805", "station": "Saint-Pierre"},
    {"id": "78890", "station": "La Désirade"},
    {"id": "78894", "station": "Saint Barthélemy"},
    {"id": "78897", "station": "Pointe-à-Pitre International Airport"},
    {"id": "78925", "station": "Martinique Aimé Césaire International Airport"},
    {"id": "81401", "station": "Saint-Laurent"},
    {"id": "81405", "station": "Cayenne – Félix Éboué Airport"},
]

_QUESTION_TEMPLATES = {
    "meteofrance": (
        "What is the probability that the daily average temperature at the French weather station "
        "at {station} will be higher on {resolution_date} than on {forecast_due_date}?"
    )
}

_VALUE_EXPLANATIONS = {
    "meteofrance": "The daily average temperature at the French weather station at {station}."
}


def _create_meteofrance_constants(stations: list[dict]) -> list[dict]:
    """Convert station data into the series config consumed by fetch and update.

    Args:
        stations (list[dict]): MeteoFrance station entries with ``id`` and ``station``.
    """
    constants_list = []
    for item in stations:
        station_id = item["id"]
        station = item["station"]
        question_text = _QUESTION_TEMPLATES["meteofrance"].replace("{station}", station)
        explanation = _VALUE_EXPLANATIONS["meteofrance"].format(station=station)
        constants_list.append(
            {
                "id": f"meteofrance/TEMPERATURE/celsius.{station_id}.D",
                "question_text": question_text,
                "freeze_datetime_value_explanation": explanation,
            }
        )
    return constants_list


_CONSTANTS = _create_meteofrance_constants(_METEOFRANCE_STATIONS)


class DbnomicsSource(DatasetSource):
    """DBnomics economic data source."""

    name: ClassVar[str] = "dbnomics"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(self, **kwargs: Any) -> DataFrame[DbnomicsFetchFrame]:
        """Fetch DBnomics series data from the public API."""
        # Compute 'today' once and thread it to every series call so a run straddling midnight
        # uses one consistent upper bound across all of its requests.
        today = dates.get_date_today()
        logger.info("Downloading DBnomics data.")

        df = None
        for row in _CONSTANTS:
            new_rows = self._call_endpoint(id=row["id"], today=today)
            df = new_rows if df is None else pd.concat([df, new_rows])

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
        for row in _CONSTANTS:
            id = row["id"].replace("/", "_")
            df_series = dff[dff["id"] == id]

            resolution_files[id] = self._build_resolution_df(df_series)

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
            series_dates = pd.to_datetime(df_series["period"])

            last_fetch_date = series_dates.iloc[-1]
            last_fetch_value = series_values.iloc[-1]
            freeze_datetime_value = (
                float(last_fetch_value)
                if last_fetch_date.date() > yesterday and last_fetch_value != "NA"
                else "N/A"
            )

            if (series_values.tail(_OBSERVATIONS_WITHOUT_DATA) != "NA").any():
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

        logger.info(f"Found {len(dfq):,} questions of {len(_CONSTANTS):,} possible.")

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
        params = {"observations": "true"}
        response = requests.get(url=endpoint, params=params)
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
                "provider_name": data.get("provider", {}).get("name"),
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
    def _build_resolution_df(df: pd.DataFrame) -> DataFrame[ResolutionFrame]:
        """Build a resolution DataFrame ([id, date, value]) for a single series.

        Args:
            df (pd.DataFrame): Fetched rows for this series.
        """
        df = df[["id", "period", "value"]].rename(columns={"period": "date"})
        df = df.astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)
        df["value"] = df["value"].replace("NA", "N/A")
        return df
