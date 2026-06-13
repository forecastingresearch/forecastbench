"""ACLED question source."""

from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from typing import Any, ClassVar

import backoff
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import AcledFetchFrame, AcledResolutionFrame, QuestionFrame
from helpers import constants, data_utils, dates

from ._dataset import DatasetSource

logger = logging.getLogger(__name__)

# Need 2 years of data to get monthly average over the year
# As ACLED only uses > filter so >2022 gets 2023 or more recent, providing yearly average for
# questions in 2024
_ACLED_START_YEAR = constants.BENCHMARK_START_YEAR - 2

# Read/write dtypes for the fetch file. Also used by the update job to read the fetch file
# with explicit dtypes (event_date must stay a string so the year-prefix fix can apply).
FETCH_COLUMN_DTYPE = {
    "event_id_cnty": str,
    "event_date": str,
    "iso": int,
    "region": str,
    "country": str,
    "admin1": str,
    "event_type": str,
    "fatalities": int,
    "timestamp": str,
}
FETCH_COLUMNS = list(FETCH_COLUMN_DTYPE.keys())

_BACKGROUND = """
ACLED classifies events into six distinct categories:

1. Battles: violent interactions between two organized armed groups at a particular time and
   location;
2. Protests: in-person public demonstrations of three or more participants in which the participants
   do not engage in violence, though violence may be used against them;
3. Riots: violent events where demonstrators or mobs of three or more engage in violent or
   destructive acts, including but not limited to physical fights, rock throwing, property
   destruction, etc.;
4. Explosions/Remote violence: incidents in which one side uses weapon types that, by their nature,
   are at range and widely destructive;
5. Violence against civilians: violent events where an organized armed group inflicts violence upon
   unarmed non-combatants; and
6. Strategic developments: contextually important information regarding incidents and activities of
   groups that are not recorded as any of the other event types, yet may trigger future events or
   contribute to political dynamics within and across states.

Detailed information about the categories can be found at:
https://acleddata.com/knowledge-base/codebook/#acled-events
"""


class QuestionType(Enum):
    """Types of questions.

    These will determine how a given question is resolved.
    """

    N_30_DAYS_GT_30_DAY_AVG_OVER_PAST_360_DAYS = 0
    N_30_DAYS_X_10_GT_30_DAY_AVG_OVER_PAST_360_DAYS_PLUS_1 = 1


_QUESTIONS = {
    "last30Days.gt.30DayAvgOverPast360Days": {
        "question_type": QuestionType.N_30_DAYS_GT_30_DAY_AVG_OVER_PAST_360_DAYS,
        "question": (
            (
                "Will there be more {event_type} in {country} for the 30 days before "
                "{resolution_date} compared to the 30-day average of {event_type} over the 360 "
                "days preceding {forecast_due_date}?"
                "\n\n"
                "e.g. If the forecast due date is 2024-01-01 and we have the following data:\n"
                "Date,{event_type}\n"
                "2023-11-11,1\n"
                "2023-10-10,2\n"
                "to calculate the 30-day average of {event_type} over the preceding 360 "
                "days, we’d have: (1+2)/12=0.25.\n\n"
                "In this example, for the question to resolve positively, 1 or more "
                "{event_type} would need to occur in the 30 days leading up to the resolution."
            ),
            ("event_type", "country"),
        ),
        "freeze_datetime_value_explanation": (
            (
                "The 30-day average of {event_type} over the past 360 days in {country}. "
                "This reference value will potentially change as ACLED updates its dataset."
            ),
            ("event_type", "country"),
        ),
    },
    "last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1": {
        "question_type": QuestionType.N_30_DAYS_X_10_GT_30_DAY_AVG_OVER_PAST_360_DAYS_PLUS_1,
        "question": (
            (
                "Will there be more than ten times as many {event_type} in {country} for the 30 "
                "days before {resolution_date} compared to one plus the 30-day average of "
                "{event_type} over the 360 days preceding {forecast_due_date}?"
                "\n\n"
                "e.g. If the forecast due date is 2024-01-01 and we have the following data:\n"
                "Date,{event_type}\n"
                "2023-11-11,1\n"
                "2023-10-10,2\n"
                "to calculate one plus the 30-day average of {event_type} over the preceding 360 "
                "days, we’d have: 1+(1+2)/12=1.25.\n\n"
                "In this example, for the question to resolve positively, 13 (10 x 1.25) or more "
                "{event_type} would need to occur in the 30 days leading up to the resolution."
            ),
            ("event_type", "country"),
        ),
        "freeze_datetime_value_explanation": (
            (
                "One plus the 30-day average of {event_type} over the past 360 days in {country}. "
                "This reference value will potentially change as ACLED updates its dataset."
            ),
            ("event_type", "country"),
        ),
    },
}


class AcledSource(DatasetSource):
    """Armed Conflict Location & Event Data source with custom resolution logic."""

    name: ClassVar[str] = "acled"
    resolution_schema: ClassVar[type] = AcledResolutionFrame

    def __init__(self) -> None:
        """Initialize with ACLED credential slots."""
        super().__init__()
        self.api_email: str | None = None
        self.api_password: str | None = None

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(self, **kwargs: Any) -> DataFrame[AcledFetchFrame]:
        """Fetch all ACLED events since _ACLED_START_YEAR.

        Authenticates via OAuth2, then paginates through the events endpoint,
        deduplicating events by event_id_cnty.
        """
        self._require_credentials()
        logger.info("Downloading ACLED data.")
        access_token = self._get_access_token()
        return self._get_events(access_token=access_token)

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[AcledFetchFrame],
        **kwargs: Any,
    ) -> UpdateResult:
        """Generate and update ACLED questions from fetched event data.

        ACLED produces no per-question resolution files: questions are resolved
        directly from the aggregated fetch data. populate_hash_mapping() must be
        called before update() so newly hashed question IDs accumulate into the
        existing mapping.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[AcledFetchFrame]): Raw ACLED event rows from fetch().
        """
        today = dates.get_date_today()
        dfr, countries, event_types = self._prepare_resolution_data(dff)
        dfq = self._generate_questions(dfq, dfr, countries, event_types, today=today)
        dfq = dfq[constants.QUESTION_FILE_COLUMNS]
        logger.info(f"Found {len(dfq):,} questions.")
        return UpdateResult(dfq=dfq, hash_mapping=self.hash_mapping)

    # ------------------------------------------------------------------
    # Private: API calls
    # ------------------------------------------------------------------

    def _require_credentials(self) -> None:
        """Raise if the ACLED API credentials are not set."""
        if not self.api_email or not self.api_password:
            raise RuntimeError(
                "AcledSource.api_email and AcledSource.api_password must be set before "
                "calling fetch(). Set them in the orchestration layer."
            )

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_time=60,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_access_token(self) -> str:
        """
        Authenticate with the ACLED API and retrieve an access token.

        Returns:
            str: The access token if the request is successful.
        """
        logger.info("Get ACLED access token.")
        endpoint = "https://acleddata.com/oauth/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "username": self.api_email,
            "password": self.api_password,
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
        }

        # No try/except: let Timeout/ConnectionError propagate to the @backoff decorator so they
        # are retried. Wrapping them in a plain RequestException (as the legacy code did) defeated
        # the retry, since backoff only retries Timeout/ConnectionError, not their base class.
        response = requests.post(endpoint, headers=headers, data=params)
        logger.debug(f"Response status code: {response.status_code}")
        logger.debug(f"Response headers: {response.headers}")
        logger.debug(f"Response content: {response.text}")
        response.raise_for_status()

        data = response.json()
        if "access_token" not in data:
            raise ValueError("Access token not found in response")
        return data["access_token"]

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=3600,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_page(
        self, endpoint: str, headers: dict[str, str], params: dict[str, Any]
    ) -> dict[str, Any]:
        """Fetch a single ACLED page and retry transient request failures."""
        response = requests.get(endpoint, headers=headers, params=params, timeout=100)

        if not response.ok:
            logger.error(f"Request to ACLED API endpoint {endpoint} failed with params {params}")
        response.raise_for_status()
        return response.json()

    def _get_events(self, access_token: str) -> DataFrame[AcledFetchFrame]:
        """
        Fetch data from the ACLED API and return it as a pandas DataFrame.

        The per-page astype(FETCH_COLUMN_DTYPE) makes the returned frame conform to
        AcledFetchFrame (notably timestamp int -> str).

        Args:
            access_token (str): OAuth2 bearer token for authenticating with the ACLED API.

        Returns:
            DataFrame[AcledFetchFrame]: All retrieved ACLED events with standardized columns.
        """
        endpoint = "https://acleddata.com/api/acled/read?_format=json"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        params = {
            "fields": "|".join(FETCH_COLUMNS),
            "year": _ACLED_START_YEAR,
            "year_where": ">",
            "page": 0,
        }

        seen_ids: set[str] = set()
        dfs: list[pd.DataFrame] = []
        while True:
            params["page"] += 1
            logger.info(f"Downloading page {params['page']}")
            data = self._get_page(endpoint=endpoint, headers=headers, params=params)
            rows = data.get("data", [])

            if not rows:
                logger.info(
                    f"No ACLED rows returned on page {params['page']}; stopping pagination."
                )
                break

            df_tmp = pd.DataFrame(rows).astype(FETCH_COLUMN_DTYPE)
            df_new_rows = df_tmp[~df_tmp["event_id_cnty"].isin(seen_ids)]
            seen_ids.update(df_new_rows["event_id_cnty"])
            dfs.append(df_new_rows)

        if not dfs:
            # No data on any page: return an empty frame so the job's `if dff.empty` guard can
            # handle it gracefully (pd.concat([]) would otherwise raise ValueError).
            return pd.DataFrame(columns=FETCH_COLUMNS)

        df = pd.concat(dfs, ignore_index=True).sort_values(by="event_id_cnty", ignore_index=True)
        logger.info(f"Downloaded {len(df)} rows.")
        return df

    # ------------------------------------------------------------------
    # Private: data transformation
    # ------------------------------------------------------------------

    @staticmethod
    def _prepare_resolution_data(dff: pd.DataFrame) -> tuple[pd.DataFrame, list, list]:
        """Aggregate raw event rows into the ACLED resolution frame.

        Args:
            dff (pd.DataFrame): Raw ACLED event rows.

        Returns:
            Tuple of (dfr, countries, event_types): events one-hot encoded by event_type and
            summed by (country, event_date), plus the unique countries and event types
            (with "fatalities" appended) used to generate questions.
        """
        df = dff.copy()

        # The values for the `event_date` field in the following entries are incorrect.
        # They are "0025-" for "2025" and "0024-" for "2024-"
        #
        # Bug reported to ACLED on 26 Sept 2025
        #
        # 2025:
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=ABW24
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=YEM104718
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=YEM99604
        #
        # 2024:
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=NCL346
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=NCL351
        # * https://acleddata.com/api/acled/read?_format=json&event_id_cnty=PYF127
        def fix_year_prefix(date_str):
            if isinstance(date_str, str):
                if date_str.startswith("0025-"):
                    return "2025-" + date_str[5:]
                if date_str.startswith("0024-"):
                    return "2024-" + date_str[5:]
            return date_str

        df["event_date"] = df["event_date"].apply(fix_year_prefix)
        # End fix bug with ACLED data

        df["event_date"] = pd.to_datetime(df["event_date"])

        df = df[["country", "event_date", "event_type", "fatalities"]].copy()

        dfr = (
            pd.get_dummies(df, columns=["event_type"], prefix="", prefix_sep="")
            .groupby(["country", "event_date"])
            .sum()
            .reset_index()
        )

        countries = df["country"].unique()
        event_types = list(df["event_type"].unique()) + ["fatalities"]

        return dfr, countries, event_types

    # ------------------------------------------------------------------
    # Private: question generation
    # ------------------------------------------------------------------

    def _generate_questions(
        self,
        dfq: pd.DataFrame,
        dfr: pd.DataFrame,
        countries: list,
        event_types: list,
        today,
    ) -> pd.DataFrame:
        """Generate forecast questions for all (country, event_type, question key) combinations.

        Args:
            dfq (pd.DataFrame): Existing questions (may be empty).
            dfr (pd.DataFrame): Aggregated resolution frame.
            countries (list): Unique countries from the fetch data.
            event_types (list): Unique event types plus "fatalities".
            today (date): Reference date for freeze value calculation.
        """
        logger.info(f"Found {len(countries)} countries.")
        logger.info(f"Found {len(event_types)} event_types.")

        questions = []
        for country in countries:
            for event_type in event_types:
                for question_key in _QUESTIONS:
                    questions.append(
                        self._create_question(
                            question_key=question_key,
                            country=country,
                            event_type=event_type,
                            dfr=dfr,
                            today=today,
                        )
                    )

        df = pd.DataFrame(questions)

        if dfq.empty:
            return df
        rows_to_append = df[~df["id"].isin(dfq["id"])]
        dfq = pd.concat([dfq, rows_to_append], ignore_index=True).sort_values(
            by="id", ignore_index=True
        )
        rows_to_update = df[df["id"].isin(dfq["id"])]
        fields_to_update = [
            "question",
            "background",
            "freeze_datetime_value",
            "freeze_datetime_value_explanation",
        ]
        for aid in rows_to_update["id"].unique():
            for field in fields_to_update:
                dfq.loc[dfq["id"] == aid, field] = df.loc[df["id"] == aid, field].iloc[0]
        return dfq

    def _create_question(
        self,
        question_key: str,
        country: str,
        event_type: str,
        dfr: pd.DataFrame,
        today,
    ) -> dict:
        """Create a single ACLED question dict.

        Args:
            question_key (str): One of the keys in _QUESTIONS.
            country (str): Country name.
            event_type (str): Event type column name.
            dfr (pd.DataFrame): Aggregated resolution frame.
            today (date): Reference date for freeze value calculation.
        """
        question_template, variables = _QUESTIONS[question_key]["question"]
        event_type_quoted = event_type if event_type == "fatalities" else f"'{event_type}'"
        question = self._fill_template(
            template=question_template,
            fields=variables,
            values={"event_type": event_type_quoted, "country": country},
        )
        aid = self._id_hash(
            {"key": question_key, "event_type": event_type, "country": country},
        )
        explanation_template, variables = _QUESTIONS[question_key][
            "freeze_datetime_value_explanation"
        ]
        freeze_datetime_value_explanation = self._fill_template(
            template=explanation_template,
            fields=variables,
            values={"event_type": event_type_quoted, "country": country},
        )
        freeze_datetime_value = self._get_freeze_value(
            key=question_key, dfr=dfr, country=country, event_type=event_type, today=today
        )
        return {
            "id": aid,
            "question": question,
            "background": _BACKGROUND,
            "freeze_datetime_value": str(freeze_datetime_value),
            "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": "N/A",
            "market_info_close_datetime": "N/A",
            "market_info_resolution_datetime": "N/A",
            "url": "https://acleddata.com/",
            "resolved": False,
            "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
        }

    @staticmethod
    def _fill_template(template: str, fields: tuple, values: dict) -> str:
        """Fill a template, preserving the {resolution_date}/{forecast_due_date} placeholders.

        Args:
            template (str): Question or explanation template.
            fields (tuple): Field names to fill.
            values (dict): Values for the fields.
        """
        fill_values = {field: values[field] for field in fields}
        # Always maintain resolution_date and forecast_due_date when formatting the string
        default_values = {
            "resolution_date": "{resolution_date}",
            "forecast_due_date": "{forecast_due_date}",
        }
        combined_fill_values = {**default_values, **fill_values}
        return template.format(**combined_fill_values)

    @staticmethod
    def _get_freeze_value(key, dfr, country, event_type, today):
        """Return the freeze value given the key."""
        if key == "last30Days.gt.30DayAvgOverPast360Days":
            return AcledSource._thirty_day_avg_over_past_360_days(dfr, country, event_type, today)

        if key == "last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1":
            return AcledSource._thirty_day_avg_over_past_360_days_plus_1(
                dfr, country, event_type, today
            )

        raise Exception("Invalid key.")

    # ------------------------------------------------------------------
    # Private: resolution
    # ------------------------------------------------------------------

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve ACLED questions row by row."""
        logger.info("Resolving ACLED questions.")
        max_date = dfr["event_date"].max()
        mask = df["resolution_date"] <= max_date
        for index, row in df[mask].iterrows():
            forecast_due_date = row["forecast_due_date"].date()
            resolution_date = row["resolution_date"].date()
            if not self._is_combo(row):
                value = self._resolve_single_question(
                    mid=row["id"],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
            else:
                value1 = self._resolve_single_question(
                    mid=row["id"][0],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
                value2 = self._resolve_single_question(
                    mid=row["id"][1],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
                value = self._combo_change_sign(
                    value1, row["direction"][0]
                ) * self._combo_change_sign(value2, row["direction"][1])
            df.at[index, "resolved_to"] = value
        df.loc[mask, "resolved"] = True
        return df, []

    def _resolve_single_question(self, mid, forecast_due_date, resolution_date, dfq, dfr):
        """Resolve an individual ACLED question by unhashing the ID and comparing aggregates."""
        question = self._get_question(dfq, mid)
        if question is None:
            logger.warning(f"ACLED: could NOT find {mid}")
            return np.nan

        d = self._id_unhash(mid)
        if d is None:
            logger.error(f"ACLED: could NOT unhash {mid}")
            return np.nan

        return self._acled_resolve(
            **d,
            dfr=dfr,
            forecast_due_date=forecast_due_date,
            resolution_date=resolution_date,
        )

    @staticmethod
    def _acled_resolve(key, dfr, country, event_type, forecast_due_date, resolution_date):
        """Compare 30-day sum at resolution_date against baseline at forecast_due_date."""
        lhs = AcledSource._sum_over_past_30_days(
            dfr=dfr,
            country=country,
            col=event_type,
            ref_date=resolution_date,
        )
        rhs = AcledSource._get_base_comparison_value(
            key=key,
            dfr=dfr,
            country=country,
            col=event_type,
            ref_date=forecast_due_date,
        )
        return int(lhs > rhs)

    # ------------------------------------------------------------------
    # Private: aggregation helpers
    #
    # The implementations live in helpers/acled.py (a light module the unrefactored
    # base_eval naive forecaster imports without the fetch deps). We delegate here rather
    # than the reverse so importing this heavy source module is not forced on base_eval.
    # ------------------------------------------------------------------

    @staticmethod
    def _sum_over_past_30_days(dfr, country, col, ref_date):
        """Sum of col for country over the 30 days before ref_date."""
        from helpers.acled import sum_over_past_30_days

        return sum_over_past_30_days(dfr, country, col, ref_date)

    @staticmethod
    def _thirty_day_avg_over_past_360_days(dfr, country, col, ref_date):
        """30-day average (total/12) over the 360 days before ref_date."""
        from helpers.acled import thirty_day_avg_over_past_360_days

        return thirty_day_avg_over_past_360_days(dfr, country, col, ref_date)

    @staticmethod
    def _thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
        """1 + 30-day average over the 360 days before ref_date."""
        from helpers.acled import thirty_day_avg_over_past_360_days_plus_1

        return thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date)

    @staticmethod
    def _get_base_comparison_value(key, dfr, country, col, ref_date):
        """Return the baseline value for comparison given the question key string."""
        from helpers.acled import get_base_comparison_value

        return get_base_comparison_value(key, dfr, country, col, ref_date)

    # ------------------------------------------------------------------
    # Hash mapping
    # ------------------------------------------------------------------

    def populate_hash_mapping(self, raw_json: str) -> None:
        """Parse hash mapping from raw JSON string."""
        self.hash_mapping = json.loads(raw_json) if raw_json else {}

    def dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON string."""
        return json.dumps(self.hash_mapping, indent=4)

    def _id_hash(self, d: dict) -> str:
        """Encode ACLED Ids and store in hash_mapping."""
        dictionary_json = json.dumps(d, sort_keys=True)
        hash_key = hashlib.sha256(dictionary_json.encode()).hexdigest()
        self.hash_mapping[hash_key] = d
        return hash_key

    def _id_unhash(self, hash_key: str):
        """Look up the original question dict from a hash key."""
        return self.hash_mapping.get(hash_key)
