"""SerpAPI dataset source."""

import logging
import re
from datetime import timedelta
from typing import ClassVar
from urllib.parse import quote, urlsplit

import backoff
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import (
    QuestionFrame,
    ResolutionFrame,
    ResolveReadyFrame,
    SerpapiFetchFrame,
)
from helpers import constants, dates

from ._dataset import DatasetSource
from .serpapi_helpers import (
    TODAY_ONLY_ENGINES,
    is_finite_number,
    iter_measurements,
    measurement_params,
)
from .serpapi_questions import QUESTION_SPECS

logger = logging.getLogger(__name__)

# Allow daily snapshot updates to cross midnight, but expire older observations.
MAX_SNAPSHOT_AGE_DAYS = 1
# Allow a few missed daily collections, but stop sampling inactive flight series.
MAX_FLIGHT_AGE_DAYS = 7
# Replace a missing snapshot only with one saved within a week after its date.
MAX_SNAPSHOT_FALLBACK_DAYS = 7


def question_id(name: str, variables: dict) -> str:
    """Build a permanent, filename-safe ID."""
    entity = variables["id"]
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError("SerpAPI names must be lowercase identifiers.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", entity):
        raise ValueError(f"Invalid SerpAPI entity ID: {entity!r}")
    return f"{name}__{entity}"


class SerpapiSource(DatasetSource):
    """Daily search measurements."""

    name: ClassVar[str] = "serpapi"

    @pa.check_types
    def fetch(self) -> DataFrame[SerpapiFetchFrame]:
        """Collect today's snapshots and available departed flights through today (UTC)."""
        api_key = self._require_api_key()
        rows = []
        failures = 0
        requests_attempted = 0
        seen = set()
        for name, spec in QUESTION_SPECS.items():
            for variables in spec["variables"]:
                id = question_id(name, variables)
                if id in seen:
                    raise ValueError(f"Duplicate SerpAPI question ID: {id}")
                seen.add(id)
                if id in self.get_nullified_ids():
                    continue
                # Refresh per request so runs crossing UTC midnight use the correct date.
                fetched_at = dates.get_datetime_now()
                today = pd.Timestamp(fetched_at).date()
                requested_date = (today + timedelta(days=spec["date_offset"])).isoformat()
                observation_date = (
                    today + timedelta(days=spec.get("observation_date_offset", spec["date_offset"]))
                ).isoformat()
                measurement = {**spec, "variables": variables}
                params = measurement_params(measurement, requested_date, today)
                observations = []
                requests_attempted += 1
                try:
                    response = self._get_response({**params, "api_key": api_key})
                    data = response.json()
                    if not isinstance(data, dict):
                        raise ValueError("Expected a JSON object from SerpAPI.")
                    if "error" in data:
                        raise ValueError(data["error"])
                    if spec["engine"] == "amazon_product":
                        data["raw_html"] = self._get_amazon_html(data)
                    observations.extend(
                        iter_measurements(
                            name, measurement, data, requested_date, observation_date, today
                        )
                    )
                except (
                    requests.RequestException,
                    ValueError,
                    KeyError,
                    TypeError,
                    AttributeError,
                ) as exc:
                    # Request exceptions can contain the authenticated URL; never log it.
                    logger.warning(f"SerpAPI {id}: collection failed ({type(exc).__name__}).")
                    failures += 1
                    observations.append((observation_date, None))
                for day, value in observations:
                    valid = is_finite_number(value)
                    if not valid:
                        logger.warning(
                            f"SerpAPI {id}: no valid measurement for {day}; recording N/A."
                        )
                    rows.append(
                        {
                            "id": id,
                            "fetch_datetime": fetched_at,
                            "requested_date": requested_date,
                            "date": day,
                            "value": value if valid else "N/A",
                        }
                    )
        if failures and failures == requests_attempted:
            raise RuntimeError("All SerpAPI requests failed; retaining the previous fetch output.")
        # Preserve required columns when no questions are eligible for fetching.
        return pd.DataFrame(rows, columns=list(SerpapiFetchFrame.to_schema().columns))

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[SerpapiFetchFrame],
        *,
        existing_resolution_files: dict[str, DataFrame[ResolutionFrame]] | None = None,
    ) -> UpdateResult:
        """Append observations to history and refresh the reusable question bank.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[SerpapiFetchFrame]): Dated measurements from fetch().
            existing_resolution_files (dict | None): Existing per-question histories.
        """
        existing_resolution_files = existing_resolution_files or {}
        today = dates.get_date_today()
        dfq = dfq.copy()
        # Retired, nullified and unavailable series are excluded from future curation.
        dfq["resolved"] = True
        resolution_files = {}
        configured = {}
        for name, spec in QUESTION_SPECS.items():
            for variables in spec["variables"]:
                id = question_id(name, variables)
                if id in configured:
                    raise ValueError(f"Duplicate SerpAPI question ID: {id}")
                configured[id] = (name, spec, variables)
        for id, measurements in dff.groupby("id", sort=False):
            if id in self.get_nullified_ids() or id not in configured:
                continue
            name, spec, variables = configured[id]
            measurements = measurements.sort_values("date", kind="stable")
            url = spec["url"].format(
                **{key: quote(str(value), safe=":") for key, value in variables.items()}
            )
            question = {
                "id": id,
                "question": spec["question_template"].format(
                    **variables,
                    resolution_date="{resolution_date}",
                    forecast_due_date="{forecast_due_date}",
                ),
                "background": spec["question_background"].format(**variables, url=url),
                "resolution_criteria": spec["resolution_criteria"].format(**variables, url=url),
                "url": url,
                "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                "market_info_resolution_criteria": "N/A",
                "market_info_open_datetime": "N/A",
                "market_info_close_datetime": "N/A",
                "market_info_resolution_datetime": "N/A",
            }
            history = pd.concat(
                [
                    existing_resolution_files.get(id, pd.DataFrame()),
                    measurements[["id", "date", "value", "fetch_datetime"]],
                ],
                ignore_index=True,
            )
            history["value"] = pd.to_numeric(history["value"], errors="coerce")
            history.loc[~np.isfinite(history["value"]), "value"] = np.nan
            if name == "flight_departure_delay":
                # Also normalize signed values in saved histories and retained fetches.
                history["value"] = history["value"].clip(lower=0)
            history["date"] = pd.to_datetime(history["date"])
            snapshot = spec["engine"] in TODAY_ONLY_ENGINES
            if not snapshot:
                # Legacy files have no collection timestamps. New observations establish
                # provenance; thereafter even same-day replays cannot undo corrections.
                history["fetch_datetime"] = pd.to_datetime(
                    history["fetch_datetime"], utc=True, format="mixed"
                )
                history = history.sort_values("fetch_datetime", kind="stable", na_position="first")
                collected_at = (
                    history[history["value"].notna()].groupby("date")["fetch_datetime"].last()
                )
            # A failed retry must not erase an observation already collected for that date.
            grouped = history.groupby("date")["value"]
            # Snapshot retries must not replace the first successful daily measurement.
            history = (grouped.first() if snapshot else grouped.last()).sort_index()
            # Replaying an older fetch must not roll the bank's current value or
            # availability back to an earlier date than the saved history.
            observation_date = history.index.max().date().isoformat()
            value = history.loc[pd.Timestamp(observation_date)]
            if name == "flight_departure_delay":
                # Keep the freshness requirement while summarizing the prior 14 days.
                # Keep missing dates in history for exact-date resolution.
                cutoff = pd.Timestamp(today - timedelta(days=MAX_FLIGHT_AGE_DAYS))
                window_start = pd.Timestamp(today - timedelta(days=14))
                completed = history[
                    (history.index >= window_start) & (history.index < pd.Timestamp(today))
                ].dropna()
                value = np.nan
                question["freeze_datetime_value_explanation"] = (
                    "N/A: no sufficiently recent valid measurement is available."
                )
                if not completed.empty and completed.index[-1] >= cutoff:
                    value = completed.median()
                    question["freeze_datetime_value_explanation"] = (
                        "Due to data-collection constraints, this reference median delay (minutes) "
                        f"uses {len(completed)} observed days within 14 days before the bank update."
                    )
            if snapshot:
                question["freeze_datetime_value_explanation"] = (
                    f"The listed item price in USD saved for {observation_date} (UTC)."
                )
            elif name != "flight_departure_delay":
                question["freeze_datetime_value_explanation"] = (
                    f"The value stored for {observation_date}, or N/A if none was collected."
                )
            if snapshot and pd.notna(value):
                # Replayed fetches must not keep inactive series available for sampling.
                if (today - history.last_valid_index().date()).days > MAX_SNAPSHOT_AGE_DAYS:
                    value = np.nan
                    question["freeze_datetime_value_explanation"] = (
                        "N/A: no sufficiently recent valid measurement is available."
                    )
            question["resolved"] = pd.isna(value)
            # Stored JSON keeps 10 decimals; drop parsing noise such as 2.8200000000000003.
            question["freeze_datetime_value"] = (
                "N/A" if pd.isna(value) else str(round(float(value), 10))
            )
            frame = history.rename_axis("date").reset_index(name="value")
            if not snapshot:
                frame["fetch_datetime"] = (
                    frame["date"]
                    .map(collected_at)
                    .map(lambda timestamp: timestamp.isoformat() if pd.notna(timestamp) else None)
                )
            frame["id"] = id
            frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
            frame["value"] = frame["value"].astype(object).where(frame["value"].notna(), "N/A")
            columns = ["id", "date", "value"] + ([] if snapshot else ["fetch_datetime"])
            resolution_files[id] = frame[columns]
            if id in dfq["id"].values:
                index = dfq.index[dfq["id"] == id][0]
                for key, value in question.items():
                    dfq.at[index, key] = value
            else:
                dfq = pd.concat([dfq, pd.DataFrame([question])], ignore_index=True)
        return UpdateResult(dfq=dfq, resolution_files=resolution_files)

    @backoff.on_exception(
        backoff.expo,
        (requests.Timeout, requests.ConnectionError, requests.HTTPError),
        max_tries=3,
        jitter=None,
        giveup=lambda exc: isinstance(exc, requests.HTTPError)
        and (exc.response is None or exc.response.status_code not in {429, 500, 502, 503, 504}),
        logger=None,  # Request exception URLs can contain the API key.
    )
    def _get_response(self, params: dict, *, html_url: str | None = None) -> requests.Response:
        """Fetch one response, retrying only transient request failures.

        Args:
            params (dict): Query parameters, including the API key.
            html_url (str | None): Validated SerpAPI capture URL, without API credentials.
        """
        if html_url is None:
            response = requests.get(
                "https://serpapi.com/search.json", params=params, timeout=(15, 180)
            )
        else:
            response = requests.get(html_url, timeout=(15, 180), allow_redirects=False)
            if response.is_redirect:
                raise ValueError("Unexpected redirect from SerpAPI HTML capture.")
        response.raise_for_status()
        return response

    def _get_amazon_html(self, data: dict) -> str:
        """Retrieve the same response's HTML to verify offer condition and seller."""
        metadata = data.get("search_metadata") or {}
        url = metadata.get("raw_html_file", "")
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "serpapi.com"
            or not parsed.path.startswith("/searches/")
            or parsed.path.rsplit("/", 1)[-1] != f"{metadata.get('id')}.html"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Missing or invalid SerpAPI HTML capture URL.")
        return self._get_response({}, html_url=url).text

    def _resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: DataFrame[ResolutionFrame],
    ) -> tuple[DataFrame[ResolveReadyFrame], list[str]]:
        """Apply snapshot fallback and compare flight delays with their due-date 14-day median."""
        # Select fallback values only for resolution; keep their actual dates in history.
        # Preserve existing date rules for retired categories; live specs take precedence.
        # Retain new categories here before removing their specs from collection.
        resolution_specs = {
            "amazon_minimum_product_price": {"engine": "amazon_product"},
            "walmart_food_drink_price": {"engine": "walmart_product"},
            **QUESTION_SPECS,
        }
        snapshot_names = {
            name
            for name, spec in resolution_specs.items()
            if spec.get("engine") in TODAY_ONLY_ENGINES
        }
        dfr = dfr[["id", "date", "value"]].copy()
        numeric = pd.to_numeric(dfr["value"], errors="coerce")
        dfr["value"] = numeric.where(np.isfinite(numeric), np.nan)
        # Resolution can read signed histories before their next update.
        flights = dfr["id"].str.startswith("flight_departure_delay__")
        dfr.loc[flights, "value"] = dfr.loc[flights, "value"].clip(lower=0)
        valid = dfr[dfr["value"].notna()].sort_values("date")
        valid_by_id = {id: history for id, history in valid.groupby("id")}
        result, warnings = super()._resolve(df, dfq, dfr)
        combo = result["id"].apply(self._is_combo)
        for index, row in result[~combo].iterrows():
            name = row["id"].split("__", 1)[0]
            if name == "flight_departure_delay":
                history = valid_by_id.get(row["id"], valid.iloc[:0])
                due, resolution = row["forecast_due_date"], row["resolution_date"]
                # The baseline is fixed at the due date so every horizon resolves against the
                # median the forecaster was asked about: the 14 days ending on the due date.
                prior = history[history["date"].between(due - pd.Timedelta(days=13), due)]["value"]
                target = history.loc[history["date"] == resolution, "value"]
                # Report the observed due-date delay, not the baseline median.
                result.at[index, "resolved_to"] = (
                    float(target.iloc[0] > prior.median())
                    if not target.empty and not prior.empty
                    else np.nan
                )
            elif name in snapshot_names:
                history = valid_by_id.get(row["id"], valid.iloc[:0])
                due, resolution = row["forecast_due_date"], row["resolution_date"]
                window = pd.Timedelta(days=MAX_SNAPSHOT_FALLBACK_DAYS)
                # Choose values per horizon: a longer horizon's baseline may be too
                # late for a shorter one, whose baseline must predate its resolution date.
                selected = []
                for day, last in (
                    (due, min(due + window, resolution - pd.Timedelta(days=1))),
                    (resolution, resolution + window),
                ):
                    available = history[history["date"].between(day, last)]
                    selected.append(available.iloc[0]["value"] if not available.empty else np.nan)
                baseline, target = selected
                result.at[index, "market_value_on_due_date"] = baseline
                result.at[index, "resolved_to"] = (
                    float(target > baseline) if pd.notna(baseline) and pd.notna(target) else np.nan
                )
        # Recompute conjunctions from the corrected component outcomes, including negations.
        outcomes = {
            (row["id"], row["forecast_due_date"], row["resolution_date"]): row["resolved_to"]
            for row in result[~combo].to_dict("records")
        }
        for index, row in result[combo].iterrows():
            result.at[index, "resolved_to"] = np.prod(
                [
                    self._combo_change_sign(
                        outcomes.get(
                            (id, row["forecast_due_date"], row["resolution_date"]), np.nan
                        ),
                        direction,
                    )
                    for id, direction in zip(row["id"], row["direction"])
                ]
            )
        result["resolved"] = result["resolved_to"].notna()
        return result, warnings
