"""SerpAPI source, orchestration and baseline contracts, without paid API calls."""

import json
from copy import deepcopy
from datetime import date
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest
import requests
from google.api_core.exceptions import Forbidden, NotFound, ServiceUnavailable

from _fb_types import SourceType
from _schemas import QuestionFrame, ResolutionFrame, SerpapiFetchFrame
from curate_questions.create_question_set import main as create_question_set
from helpers import constants, question_curation
from llm_forecaster import prompts
from sources import serpapi
from sources.serpapi import SerpapiSource, question_id
from sources.serpapi_helpers import measurement_params, parse_youtube_max_views
from sources.serpapi_questions import QUESTION_SPECS

from ._module_stubs import imported_with_stubs
from .conftest import make_forecast_df, make_question_df, make_resolution_df


def amazon_html(asin, price=12, seller="Amazon.com", condition="New"):
    """Minimal offer markup retaining the identity, condition and purchase-price evidence."""
    return f"""<div id="aod-offer">
      <div id="aod-offer-heading">{condition}</div>
      <div id="aod-offer-soldBy">Sold by {seller}</div>
      <input name="items[0.base][asin]" value="{asin}">
      <input name="submit.addToCart"
             aria-label="Add to Cart from seller {seller} and price ${price:.2f}">
    </div>"""


def response_for(name, variables, requested_date, params):
    """Representative response shapes for all enabled parsers."""
    responses = {
        "amazon_minimum_product_price": {
            "product_results": {"asin": variables.get("asin"), "extracted_price": 12},
            "raw_html": amazon_html(variables.get("asin")),
            "purchase_options": {
                "buy_new": {
                    "caption": "Buy New",
                    "extracted_price": 12,
                    "features": {"shipper_seller": {"text": "Amazon.com"}},
                }
            },
            "other_sellers": [{"extracted_price": 9, "condition": "NEW"}],
        },
        "youtube_channel_max_video_views": {
            "channel_results": {"external_id": variables.get("channel_id"), "handle": "@example"},
            "filters": [{"title": "Popular", "selected": True}],
            "videos_results": [
                {"views": "12,345 views", "extracted_views": 12345},
                {"views": "1.2K views", "extracted_views": 1200},
            ],
            "shorts_results": [{"extracted_views": 99999}],
            "streams_results": [{"extracted_views": 99999}],
        },
        "walmart_food_drink_price": {
            "search_information": {"location": {"store_id": variables.get("store_id")}},
            "product_result": {
                "us_item_id": variables.get("us_item_id"),
                "in_stock": True,
                "price_map": {"price": 9, "currency": "USD", "unit_price": 1},
            },
        },
        "google_finance_ticker_price": {
            "graph": [
                {"date": "Sep 15 2026, 10:00 AM UTC+02:00", "price": 100},
                {"date": "Sep 15 2026, 05:30 PM UTC+02:00", "price": 101},
                {"date": "Sep 16 2026, 09:00 AM UTC+02:00", "price": 999},
            ]
        },
        "flight_departure_delay": {
            "flight_result": {
                "dates": [
                    {
                        "date": requested_date,
                        "flight_designator": variables.get("flight_id"),
                        "metadata": {
                            "origin": variables.get("origin"),
                            "destination": variables.get("destination"),
                            "status": "ARRIVED",
                            "departure_delay": -5,
                        },
                    }
                ]
            }
        },
    }
    return {"search_parameters": params, **responses[name]}


EXPECTED = {
    "amazon_minimum_product_price": (12, "2026-09-16"),
    "youtube_channel_max_video_views": (12345, "2026-09-16"),
    "walmart_food_drink_price": (9, "2026-09-16"),
    "google_finance_ticker_price": (101, "2026-09-15"),
    "flight_departure_delay": (0, "2026-09-15"),
}


@pytest.fixture()
def source(freeze_today, monkeypatch):
    freeze_today(date(2026, 9, 16))
    source = SerpapiSource()
    source.api_key = "test-secret-not-for-logs"
    # Most source tests isolate transport; dedicated tests below exercise HTML retrieval.
    monkeypatch.setattr(source, "_get_amazon_html", lambda data: data.get("raw_html", ""))
    return source


def configure(monkeypatch, name):
    spec = deepcopy(QUESTION_SPECS[name])
    spec["variables"] = spec["variables"][:1]
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {name: spec})
    return spec


def empty_bank():
    return pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)


@pytest.mark.parametrize("name", EXPECTED)
def test_each_api_fetches_updates_and_serializes(source, monkeypatch, tmp_path, name):
    spec = configure(monkeypatch, name)
    value, day = EXPECTED[name]
    observation_date = day

    def get(url, *, params, timeout):
        assert url == "https://serpapi.com/search.json"
        assert params["api_key"] == source.api_key
        assert timeout[1] > 0
        assert params["engine"] == spec["engine"]
        expected_params = spec["params"](spec["variables"][0], day)
        assert all(params[k] == v for k, v in expected_params.items())
        response = Mock()
        response.json.return_value = response_for(name, spec["variables"][0], day, params)
        return response

    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    assert fetched.iloc[0]["date"] == observation_date
    assert fetched.iloc[0]["value"] == value
    assert set(fetched.columns) == {"id", "date", "value", "fetch_datetime", "requested_date"}
    assert fetched.iloc[0]["requested_date"] == day
    fetched = pd.read_json(
        StringIO(fetched.to_json(orient="records", lines=True)), lines=True, convert_dates=False
    )
    SerpapiFetchFrame.validate(fetched)
    updated = source.update(empty_bank(), fetched)
    QuestionFrame.validate(updated.dfq)
    assert "{resolution_date}" in updated.dfq.iloc[0]["question"]
    assert "{forecast_due_date}" in updated.dfq.iloc[0]["question"]
    assert not updated.dfq.iloc[0]["resolved"]
    saved = pd.read_json(
        StringIO(updated.dfq.to_json(orient="records", lines=True)),
        lines=True,
        convert_dates=False,
    ).iloc[0]
    assert "https://" not in saved["question"]
    assert saved["url"] in saved["background"]
    assert "{" not in saved["background"]
    criteria = saved["resolution_criteria"]
    assert criteria.strip() and "{" not in criteria
    if name == "youtube_channel_max_video_views":
        assert f"at least {spec['variables'][0]['percentage_threshold']}% higher" in criteria
    else:
        assert "strictly" in criteria and "equal or lower resolves No" in criteria

    # The persisted question's custom criteria must reach the published set and prompt.
    published = saved.to_frame().T.assign(
        source="serpapi", source_intro=source.source_intro, freeze_datetime="2026-09-16"
    )
    published["resolution_criteria"] = create_question_set.get_resolution_criteria(
        published, question_curation.FREEZE_QUESTION_DATA_SOURCES["serpapi"]["resolution_criteria"]
    )
    output_path = tmp_path / "questions.json"
    monkeypatch.setattr(
        create_question_set, "open", lambda *a, **k: output_path.open(*a[1:], **k), raising=False
    )
    monkeypatch.setattr(create_question_set.env, "RUNNING_LOCALLY", True)
    create_question_set.write_questions(
        {"serpapi": {"dfq": published}}, create_question_set.QuestionSetTarget.LLM
    )
    question = json.loads(output_path.read_text(encoding="utf-8"))["questions"][0]
    assert question["resolution_criteria"] == criteria
    prompt = prompts.render_template(
        prompts.ZERO_SHOT_DATASET_PROMPT,
        {
            "question": question["question"],
            "background": question["background"],
            "resolution_criteria": question["resolution_criteria"],
            "freeze_datetime": question["freeze_datetime"],
            "freeze_datetime_value": question["freeze_datetime_value"],
            "freeze_datetime_value_explanation": question["freeze_datetime_value_explanation"],
            "today_date": "2026-09-16",
            "list_of_resolution_dates": question["resolution_dates"],
        },
    )
    assert f"Resolution Criteria:\n{criteria}" in prompt
    assert not any(character.isspace() for character in saved["url"])
    if name == "flight_departure_delay":
        assert "&hl=en&gl=us" in saved["url"]
        for field in ("question", "background", "resolution_criteria"):
            assert "early and on-time departures" in saved[field]
            assert "zero delay" in saved[field]
    elif name == "amazon_minimum_product_price":
        assert "Set the delivery ZIP code to 10001" in saved["background"]
    history = updated.resolution_files[fetched.iloc[0]["id"]]
    ResolutionFrame.validate(history)
    assert history.iloc[0]["date"] == observation_date
    assert history.iloc[0]["value"] == value


@pytest.mark.parametrize(
    "product",
    [
        "The First Years Stack & Count Stacking Cups 8 Count",
        "Product + Extra #1 50% / Special?",
    ],
)
def test_amazon_question_preserves_product_name_and_links_exact_asin(source, monkeypatch, product):
    spec = configure(monkeypatch, "amazon_minimum_product_price")
    variables = spec["variables"][0]
    variables["product"] = product
    fetched = pd.DataFrame([{"id": question_id("amazon_minimum_product_price", variables)}]).assign(
        date="2026-09-16",
        value=20,
        requested_date="2026-09-16",
        fetch_datetime="2026-09-16T01:00:00Z",
    )

    question = source.update(empty_bank(), fetched).dfq.iloc[0]
    url = urlsplit(question["url"])
    assert url.path == f"/dp/{variables['asin']}"
    assert parse_qs(url.query) == {"language": ["en_US"]}
    assert product in question["question"]
    assert product in question["background"]
    assert not url.fragment
    assert question["url"] in question["background"]


def test_update_refreshes_metadata_without_refetching(source, monkeypatch, freeze_today):
    name = "flight_departure_delay"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    response = Mock()
    response.json.return_value = response_for(
        name, variables, "2026-09-15", spec["params"](variables, "2026-09-15")
    )
    get = Mock(return_value=response)
    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    first = source.update(empty_bank(), fetched)
    # Banks created before custom criteria must gain the field on their next update.
    first.dfq.drop(columns="resolution_criteria", inplace=True)
    monkeypatch.setattr(
        serpapi,
        "QUESTION_SPECS",
        {
            name: {
                **spec,
                "question_template": "Revised: {resolution_date} vs {forecast_due_date}",
                "question_background": "Revised background for {flight_id}. See: {url}",
                "resolution_criteria": "Revised criteria for {flight_id}.",
            }
        },
    )
    freeze_today(date(2026, 9, 20))
    second = source.update(first.dfq, fetched, existing_resolution_files=first.resolution_files)
    assert second.dfq.iloc[0]["question"] == "Revised: {resolution_date} vs {forecast_due_date}"
    saved = second.dfq.iloc[0]
    assert saved["resolution_criteria"] == f"Revised criteria for {variables['flight_id']}."
    assert saved["background"] == (
        f"Revised background for {variables['flight_id']}. " f"See: {saved['url']}"
    )
    explanation = second.dfq.iloc[0]["freeze_datetime_value_explanation"]
    assert "scheduled local departure date 2026-09-15" in explanation
    id = fetched.iloc[0]["id"]
    pd.testing.assert_frame_equal(first.resolution_files[id], second.resolution_files[id])
    get.assert_called_once()
    # A saved fetch must not reactivate an entity removed from the configuration.
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {})
    retired = source.update(second.dfq, fetched, existing_resolution_files=second.resolution_files)
    assert retired.dfq.iloc[0]["resolved"]
    assert not retired.resolution_files


@pytest.mark.parametrize(
    "baseline,target,expected_values,expected",
    [
        (-5, -10, [0, 0], 0),
        (-5, -5, [0, 0], 0),
        (-5, -2, [0, 0], 0),
        (-5, 0, [0, 0], 0),
        (-5, 3, [0, 3], 1),
        (0, -5, [0, 0], 0),
        (0, 0, [0, 0], 0),
        (0, 3, [0, 3], 1),
        (5, -3, [5, 0], 0),
        (5, 5, [5, 5], 0),
        (5, 8, [5, 8], 1),
        (5, 3, [5, 3], 0),
        ("N/A", 3, ["N/A", 3], None),
        (-5, "N/A", [0, "N/A"], None),
    ],
)
def test_flight_requests_resolve_on_departure_dates(
    source, monkeypatch, freeze_today, baseline, target, expected_values, expected
):
    name = "flight_departure_delay"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    histories = {}
    bank = empty_bank()
    # Cross both year and month boundaries; history uses departure dates.
    for collected, previous_day, delay in [
        ("2027-01-01", "2026-12-31", baseline),
        ("2027-01-02", "2027-01-01", target),
    ]:
        freeze_today(date.fromisoformat(collected))
        requested = previous_day
        data = response_for(name, variables, requested, spec["params"](variables, requested))
        data["flight_result"]["dates"][0]["metadata"]["departure_delay"] = delay
        response = Mock()
        response.json.return_value = data
        get = Mock(return_value=response)
        monkeypatch.setattr(serpapi.requests, "get", get)
        fetched = source.fetch()
        params = get.call_args.kwargs["params"]
        assert params["q"] == f"{variables['flight_id']} flight status"
        id = fetched.iloc[0]["id"]
        assert id == question_id(name, variables)
        assert fetched.iloc[0]["date"] == previous_day
        updated = source.update(bank, fetched, existing_resolution_files=histories)
        bank, histories = updated.dfq, updated.resolution_files

    history = histories[id]
    assert history["date"].tolist() == ["2026-12-31", "2027-01-01"]
    assert history["value"].tolist() == expected_values
    question = bank.iloc[0]
    assert "{resolution_date}" in question["question"]
    assert "{forecast_due_date}" in question["question"]
    assert "scheduled local departure date" in question["background"]
    assert "departure delay in minutes" in question["question"]
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-12-31",
                "resolution_date": "2027-01-01",
            }
        ]
    )
    result, _ = source.resolve(
        forecast,
        bank,
        make_resolution_df(history.to_dict("records")),
        forecast_due_date=date(2026, 12, 31),
    )
    if expected_values[0] == "N/A":
        assert pd.isna(result.iloc[0]["market_value_on_due_date"])
    else:
        assert result.iloc[0]["market_value_on_due_date"] == expected_values[0]
    assert bool(result.iloc[0]["resolved"]) == (expected is not None)
    if expected is None:
        assert pd.isna(result.iloc[0]["resolved_to"])
    else:
        assert result.iloc[0]["resolved_to"] == expected


@pytest.mark.parametrize(
    "status,delay,expected",
    [
        ("SCHEDULED_STATUS", 0, "N/A"),
        ("SCHEDULE_ONLY", 0, "N/A"),
        ("DEPARTING_DELAYED", 76, "N/A"),
        ("CANCELLED", -5, "N/A"),
        (None, -5, "N/A"),
        # The departure delay is final once the flight leaves, whether or not it has arrived.
        ("DEPARTED_DELAYED", 76, 76),
        ("ON_THE_RUNWAY_AT_DESTINATION_DELAYED", 76, 76),
        ("ARRIVED", 76, 76),
        ("DEPARTED", -5, 0),
        ("LANDED", -5, 0),
        ("ARRIVED", None, "N/A"),
        ("ARRIVED", False, "N/A"),
        ("ARRIVED", "-5", "N/A"),
        ("ARRIVED", float("-inf"), "N/A"),
    ],
)
def test_flight_delay_is_recorded_only_after_departure(
    source, monkeypatch, status, delay, expected
):
    name = "flight_departure_delay"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    requested = "2026-09-15"
    data = response_for(name, variables, requested, spec["params"](variables, requested))
    data["flight_result"]["dates"][0]["metadata"].update(status=status, departure_delay=delay)
    response = Mock()
    response.json.return_value = data
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    assert fetched.iloc[0]["date"] == requested
    assert fetched.iloc[0]["value"] == expected


@pytest.mark.parametrize("latest_status", ["DEPARTED_DELAYED", "DEPARTING_DELAYED"])
def test_later_fetch_recovers_departed_flights_on_original_dates(
    source, monkeypatch, freeze_today, latest_status
):
    name = "flight_departure_delay"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    data = response_for(name, variables, "2026-09-15", {})
    flight = data["flight_result"]["dates"][0]
    flight["metadata"].update(status="DEPARTING_DELAYED", departure_delay=12)
    response = Mock()
    response.json.return_value = data
    get = Mock(return_value=response)
    monkeypatch.setattr(serpapi.requests, "get", get)
    first = source.update(empty_bank(), source.fetch())
    id = first.dfq.iloc[0]["id"]
    assert first.resolution_files[id]["value"].tolist() == ["N/A"]
    assert first.dfq.iloc[0]["resolved"]
    assert first.dfq.iloc[0]["freeze_datetime_value"] == "N/A"

    freeze_today(date(2026, 9, 17))
    flight["metadata"]["status"] = "ARRIVED_DELAYED"
    records = data["flight_result"]["dates"]
    for day, changes in [
        ("2026-09-16", {"departure_delay": -3, "status": latest_status}),
        ("2026-09-14", {"status": "CANCELLED"}),
        ("2026-09-13", {"origin": "XXX"}),
        ("2026-09-17", {}),  # Today and future dates must not enter history.
        ("2026-09-18", {}),
        ("2026-09-31", {}),
    ]:
        record = deepcopy(flight)
        record["date"] = day
        record["metadata"].update(changes)
        records.append(record)
    fetched = source.fetch()
    assert get.call_count == 2  # One request per daily run, including recovery.
    assert fetched["date"].tolist() == ["2026-09-15", "2026-09-16"]
    assert fetched["requested_date"].tolist() == ["2026-09-16"] * 2
    second = source.update(first.dfq, fetched, existing_resolution_files=first.resolution_files)
    history = second.resolution_files[id]
    assert history["date"].tolist() == ["2026-09-15", "2026-09-16"]
    departed = latest_status == "DEPARTED_DELAYED"
    assert history["value"].tolist() == [12, 0 if departed else "N/A"]
    question = second.dfq.iloc[0]
    assert not question["resolved"]
    assert float(question["freeze_datetime_value"]) == (0 if departed else 12)
    flight_date = "2026-09-16" if departed else "2026-09-15"
    assert flight_date in question["freeze_datetime_value_explanation"]
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-15",
                "resolution_date": "2026-09-16",
            }
        ]
    )
    result, _ = source.resolve(
        forecast,
        second.dfq,
        make_resolution_df(history.to_dict("records")),
        forecast_due_date=date(2026, 9, 15),
    )
    assert bool(result.iloc[0]["resolved"]) == departed
    if not departed:
        assert pd.isna(result.iloc[0]["resolved_to"])
    replay = source.update(second.dfq, fetched, existing_resolution_files=second.resolution_files)
    pd.testing.assert_frame_equal(replay.resolution_files[id], history)


def test_malformed_flight_date_preserves_recovered_measurements(source, monkeypatch):
    """A bad flight date must not discard earlier recovered dates or other entities."""
    name = "flight_departure_delay"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    spec["variables"].append({**variables, "id": "second"})
    data = response_for(name, variables, "2026-09-15", {})
    older = deepcopy(data["flight_result"]["dates"][0])
    older["date"] = "2026-09-14"
    data["flight_result"]["dates"].insert(0, older)
    data["flight_result"]["dates"][1]["metadata"] = "malformed"
    responses = [Mock(), Mock()]
    responses[0].json.return_value = data
    responses[1].json.return_value = response_for(name, spec["variables"][1], "2026-09-15", {})
    monkeypatch.setattr(serpapi.requests, "get", Mock(side_effect=responses))

    fetched = source.fetch()

    assert fetched["id"].tolist() == [
        question_id(name, variables),
        question_id(name, variables),
        question_id(name, spec["variables"][1]),
    ]
    assert fetched["date"].tolist() == ["2026-09-14", "2026-09-15", "2026-09-15"]
    assert fetched["value"].tolist() == [0, "N/A", 0]


@pytest.mark.parametrize(
    "symbol",
    ["DAX:INDEXDB", "NIFTY_50:INDEXNSE", "NZ50G:NZE"],
)
def test_finance_question_uses_index_points(source, monkeypatch, symbol):
    name = "google_finance_ticker_price"
    spec = deepcopy(QUESTION_SPECS[name])
    variables = next(item for item in spec["variables"] if item["symbol"] == symbol)
    spec["variables"] = [variables]
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {name: spec})
    response = Mock()
    response.json.return_value = response_for(
        name, variables, "2026-09-15", spec["params"](variables, "2026-09-15")
    )
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))

    updated = source.update(empty_bank(), source.fetch())
    question = updated.dfq.iloc[0]
    for field in ("question", "background"):
        assert symbol in question[field]
        assert "per share" not in question[field]
    assert "in index points" in question["background"]


def test_finance_refresh_recovers_history_and_recalculates_missing_days(source, monkeypatch):
    name = "google_finance_ticker_price"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    id = question_id(name, variables)
    response = Mock()
    response.json.return_value = {
        "search_parameters": {"q": variables["symbol"]},
        "graph": [
            {"date": "Aug 01 2026, 05:00 PM UTC+02:00", "price": 1},
            {"date": "Sep 11 2026, 05:00 PM UTC+02:00", "price": 110},
            {"date": "Sep 11 2026, 09:00 AM UTC+02:00", "price": 100},
            {"date": "Sep 15 2026, 11:30 PM UTC-04:00", "price": 120},
            {"date": "Sep 16 2026, 12:30 AM UTC+12:00", "price": 999},
        ],
    }
    get = Mock(return_value=response)
    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    assert fetched["value"].tolist() == [110, 120]
    assert fetched["date"].tolist() == ["2026-09-11", "2026-09-15"]
    assert get.call_count == 1
    assert get.call_args.kwargs["params"]["window"] == "1M"
    existing = pd.DataFrame(
        [
            {"id": id, "date": day, "value": value}
            for day, value in [
                ("2026-08-01", 80),
                ("2026-09-11", 90),
                ("2026-09-12", "N/A"),
                ("2026-09-13", "N/A"),
            ]
        ]
    )
    result = source.update(empty_bank(), fetched, existing_resolution_files={id: existing})
    history = result.resolution_files[id].set_index("date")["value"]
    assert history.loc["2026-08-01"] == 80  # Older saved history is retained.
    assert history.loc["2026-09-11"] == 110
    assert history.loc["2026-09-15"] == 120
    assert float(result.dfq.iloc[0]["freeze_datetime_value"]) == 120
    replay = source.update(result.dfq, fetched, existing_resolution_files=result.resolution_files)
    pd.testing.assert_frame_equal(result.resolution_files[id], replay.resolution_files[id])
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-13",
                "resolution_date": "2026-09-15",
            }
        ]
    )
    resolved, _ = source.resolve(
        forecast,
        result.dfq,
        make_resolution_df(result.resolution_files[id].to_dict("records")),
        forecast_due_date=date(2026, 9, 13),
    )
    assert resolved.iloc[0]["market_value_on_due_date"] == 110
    assert resolved.iloc[0]["resolved_to"] == 1


def test_finance_history_extends_through_yesterday_without_backfilling(source, monkeypatch):
    spec = configure(monkeypatch, "google_finance_ticker_price")
    response = Mock()
    response.json.return_value = {
        "search_parameters": {"q": spec["variables"][0]["symbol"]},
        "graph": [{"date": "Sep 11 2026, 05:00 PM UTC", "price": 100}],
    }
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    assert fetched["date"].tolist() == ["2026-09-11", "2026-09-15"]
    assert fetched["value"].tolist() == [100, "N/A"]
    updated = source.update(empty_bank(), fetched)
    assert next(iter(updated.resolution_files.values()))["date"].min() == "2026-09-11"
    assert float(updated.dfq.iloc[0]["freeze_datetime_value"]) == 100


def test_finance_partial_refresh_preserves_observations_and_corrects_filled_dates(
    source, monkeypatch
):
    spec = configure(monkeypatch, "google_finance_ticker_price")
    variables = spec["variables"][0]
    id = question_id("google_finance_ticker_price", variables)
    response = Mock()
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))

    def refresh(prices, previous=None):
        response.json.return_value = {
            "search_parameters": {"q": variables["symbol"]},
            "graph": [
                {"date": f"Sep {day} 2026, 05:00 PM UTC", "price": price} for day, price in prices
            ],
        }
        existing = {}
        if previous is not None:
            # Exercise the persisted contract, not just an in-memory update.
            existing[id] = pd.read_json(
                StringIO(previous.resolution_files[id].to_json(orient="records", lines=True)),
                lines=True,
                convert_dates=False,
            )
        return source.update(empty_bank(), source.fetch(), existing_resolution_files=existing)

    first = refresh([(11, 100), (14, 110), (15, 120)])
    partial = refresh([(11, 100), (15, 120)], first)
    corrected = refresh([(11, 105), (15, 120)], partial)
    forecasts = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-13",
                "resolution_date": "2026-09-14",
            }
        ]
    )
    for result, baseline in [(partial, 100), (corrected, 105)]:
        history = result.resolution_files[id]
        assert history.set_index("date").loc["2026-09-14", "value"] == 110
        resolved, _ = source.resolve(
            forecasts.copy(),
            result.dfq,
            make_resolution_df(history.to_dict("records")),
            forecast_due_date=date(2026, 9, 13),
        )
        assert resolved.iloc[0]["market_value_on_due_date"] == baseline
        assert resolved.iloc[0]["resolved_to"] == 1


def test_finance_invalid_points_do_not_hide_valid_daily_values(source, monkeypatch):
    spec = configure(monkeypatch, "google_finance_ticker_price")
    response = Mock()
    response.json.return_value = {
        "search_parameters": {"q": spec["variables"][0]["symbol"]},
        "graph": [
            {"date": "Sep 14 2026, 05:00 PM UTC", "price": 90},
            {"date": "Sep 15 2026, 04:00 PM UTC", "price": 100},
            {"date": "Sep 15 2026, 05:00 PM UTC", "price": None},
            {"date": "Sep 15 2026, 06:00 PM UTC", "price": float("inf")},
            {"date": "unreadable", "price": 999},
            {"price": 999},
            None,
        ],
    }
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    result = source.update(empty_bank(), fetched)
    history = next(iter(result.resolution_files.values())).set_index("date")["value"]
    assert history.to_dict() == {"2026-09-14": 90, "2026-09-15": 100}


def test_finance_does_not_carry_a_dead_series_forward(source, monkeypatch):
    spec = configure(monkeypatch, "google_finance_ticker_price")
    id = question_id("google_finance_ticker_price", spec["variables"][0])
    # The last real value is on 2026-08-31; every later collection returned nothing.
    days = pd.date_range("2026-08-31", "2026-09-15").strftime("%Y-%m-%d")
    existing = pd.DataFrame({"id": id, "date": days, "value": [100] + ["N/A"] * (len(days) - 1)})
    fetched = pd.DataFrame([{"id": id}]).assign(
        date="2026-09-15",
        value="N/A",
        requested_date="2026-09-15",
        fetch_datetime="2026-09-16T00:01:00+00:00",
    )
    result = source.update(empty_bank(), fetched, existing_resolution_files={id: existing})
    assert result.dfq.iloc[0]["resolved"]  # Excluded from new question sets.

    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-14",
                "resolution_date": "2026-09-15",
            }
        ]
    )
    resolved, _ = source.resolve(
        forecast,
        result.dfq,
        make_resolution_df(result.resolution_files[id].to_dict("records")),
        forecast_due_date=date(2026, 9, 14),
    )
    # 14 days after the last value it is still carried forward; 15 days after, it is not.
    assert resolved.iloc[0]["market_value_on_due_date"] == 100
    assert not resolved.iloc[0]["resolved"]


@pytest.mark.parametrize("retired", [False, True])
@pytest.mark.parametrize("target,expected", [("2026-09-14", 1), ("2026-09-15", None)])
def test_finance_fills_past_last_stored_row_within_limit(
    source, monkeypatch, retired, target, expected
):
    if retired:
        monkeypatch.setattr(serpapi, "QUESTION_SPECS", {})
    id = "google_finance_ticker_price__dax"
    history = make_resolution_df(
        [
            {"id": id, "date": "2026-08-30", "value": 90},
            {"id": id, "date": "2026-08-31", "value": 100},
        ]
    )
    original = history.copy(deep=True)
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-08-30",
                "resolution_date": target,
            }
        ]
    )
    resolved, _ = source.resolve(
        forecast, empty_bank(), history, forecast_due_date=date(2026, 8, 30)
    )
    if expected is None:
        assert not resolved.iloc[0]["resolved"]
    else:
        assert resolved.iloc[0]["resolved_to"] == expected
    pd.testing.assert_frame_equal(history, original)


@pytest.mark.parametrize("name", ["google_finance_ticker_price", "additional_finance_series"])
@pytest.mark.parametrize("fill_missing", [False, True])
def test_update_and_resolution_follow_configured_date_filling(
    source, monkeypatch, name, fill_missing
):
    """The bank and scoring use the same date policy, including for a retired entity."""
    spec = deepcopy(QUESTION_SPECS["google_finance_ticker_price"])
    spec["variables"] = spec["variables"][:1]
    spec["fill_missing_dates"] = fill_missing
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {**QUESTION_SPECS, name: spec})
    id = question_id(name, spec["variables"][0])
    fetched = pd.DataFrame(
        [
            {"id": id, "date": "2026-09-13", "value": 100},
            {"id": id, "date": "2026-09-14", "value": "N/A"},
        ]
    ).assign(requested_date="2026-09-14", fetch_datetime="2026-09-15T01:00:00Z")
    updated = source.update(empty_bank(), fetched)
    bank_value = updated.dfq.iloc[0]["freeze_datetime_value"]
    assert bank_value == ("100.0" if fill_missing else "N/A")
    assert "2026-09-14" in updated.dfq.iloc[0]["freeze_datetime_value_explanation"]
    history = updated.resolution_files[id].to_dict("records")
    assert [row["value"] for row in history] == [100, "N/A"]
    history.append({"id": id, "date": "2026-09-15", "value": 110})
    spec["variables"] = []  # Existing forecasts must resolve after the entity is retired.
    forecast = make_forecast_df(
        [
            dict(
                id=id,
                source="serpapi",
                forecast_due_date="2026-09-14",
                resolution_date="2026-09-15",
            )
        ]
    )
    resolved, _ = source.resolve(
        forecast, updated.dfq, make_resolution_df(history), forecast_due_date=date(2026, 9, 14)
    )
    row = resolved.iloc[0]
    assert bool(row["resolved"]) == fill_missing
    if fill_missing:
        assert row["market_value_on_due_date"] == float(bank_value)
        assert row["resolved_to"] == 1
    else:
        assert pd.isna(row["market_value_on_due_date"])


def test_finance_does_not_extend_stale_history_to_future_dates(source):
    id = "google_finance_ticker_price__dax"
    history = make_resolution_df([{"id": id, "date": "2026-09-14", "value": 100}])
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-14",
                "resolution_date": "2026-09-17",
            }
        ]
    )
    resolved, _ = source.resolve(
        forecast, empty_bank(), history, forecast_due_date=date(2026, 9, 14)
    )
    assert not resolved.iloc[0]["resolved"]


def test_malformed_entity_does_not_discard_other_measurements(source, monkeypatch):
    name = "amazon_minimum_product_price"
    spec = configure(monkeypatch, name)
    spec["variables"] = [spec["variables"][0], {**spec["variables"][0], "id": "second"}]
    variables = spec["variables"][1]
    response = Mock()
    response.json.side_effect = [
        {"product_results": "malformed"},
        response_for(name, variables, "2026-09-16", spec["params"](variables, "2026-09-16")),
    ]
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    assert fetched["value"].tolist() == ["N/A", 12]


@pytest.mark.parametrize("invalid", [float("inf"), float("-inf")])
def test_nonfinite_history_cannot_resolve_a_flight(source, invalid):
    name = "flight_departure_delay"
    id = question_id(name, QUESTION_SPECS[name]["variables"][0])
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-01",
                "resolution_date": "2026-09-15",
            }
        ]
    )
    history = make_resolution_df(
        [
            {"id": id, "date": "2026-09-01", "value": 0},
            {"id": id, "date": "2026-09-15", "value": invalid},
        ]
    )
    resolved, _ = source.resolve(
        forecast, empty_bank(), history, forecast_due_date=date(2026, 9, 1)
    )
    assert not resolved.iloc[0]["resolved"]


@pytest.mark.parametrize(
    "baseline,target,expected",
    [(-5, -2, 0), (-5, 0, 0), (0, -5, 0), (-5, 3, 1), (-5, "N/A", None), ("N/A", 3, None)],
)
def test_saved_signed_flight_delays_resolve_as_lateness(source, baseline, target, expected):
    flight_id = question_id(
        "flight_departure_delay", QUESTION_SPECS["flight_departure_delay"]["variables"][0]
    )
    finance_id = "google_finance_ticker_price__dax"
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-14",
                "resolution_date": "2026-09-15",
            }
            for id in (flight_id, finance_id)
        ]
    )
    history = make_resolution_df(
        [
            {"id": flight_id, "date": "2026-09-14", "value": baseline},
            {"id": flight_id, "date": "2026-09-15", "value": target},
            # The zero floor belongs only to flight delays, not other categories.
            {"id": finance_id, "date": "2026-09-14", "value": -5},
            {"id": finance_id, "date": "2026-09-15", "value": -2},
        ]
    )
    original = history.copy(deep=True)
    result, _ = source.resolve(forecast, empty_bank(), history, forecast_due_date=date(2026, 9, 14))
    result = result.set_index("id")
    flight = result.loc[flight_id]
    assert bool(flight["resolved"]) == (expected is not None)
    if expected is None:
        assert pd.isna(flight["resolved_to"])
    else:
        assert flight["resolved_to"] == expected
        assert flight["market_value_on_due_date"] == 0
    assert result.loc[finance_id, "market_value_on_due_date"] == -5
    assert result.loc[finance_id, "resolved_to"] == 1
    pd.testing.assert_frame_equal(history, original)


@pytest.mark.parametrize("name", ["flight_departure_delay", "google_finance_ticker_price"])
def test_update_normalizes_only_saved_flight_delays(source, monkeypatch, name):
    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    history = pd.DataFrame(
        [
            {"id": id, "date": "2026-09-12", "value": -5},
            {"id": id, "date": "2026-09-13", "value": "N/A"},
            {"id": id, "date": "2026-09-14", "value": float("-inf")},
        ]
    )
    # Retained fetch files can also contain signed delays from earlier test runs.
    fetched = pd.DataFrame([{"id": id, "date": "2026-09-15", "value": -3}]).assign(
        requested_date="2026-09-15", fetch_datetime="2026-09-16T00:00:00Z"
    )
    original = history.copy(deep=True)
    updated = source.update(empty_bank(), fetched, existing_resolution_files={id: history})
    is_flight = name == "flight_departure_delay"
    assert updated.resolution_files[id]["value"].tolist() == [
        0 if is_flight else -5,
        "N/A",
        "N/A",
        0 if is_flight else -3,
    ]
    assert float(updated.dfq.iloc[0]["freeze_datetime_value"]) == (0 if is_flight else -3)
    assert not updated.dfq.iloc[0]["resolved"]
    pd.testing.assert_frame_equal(history, original)
    replay = source.update(updated.dfq, fetched, existing_resolution_files=updated.resolution_files)
    pd.testing.assert_frame_equal(replay.resolution_files[id], updated.resolution_files[id])


def test_all_templates_have_stable_unique_ids_and_only_forecast_placeholders():
    assert set(QUESTION_SPECS) == set(EXPECTED)
    ids = []
    for name, spec in QUESTION_SPECS.items():
        for variables in reversed(spec["variables"]):
            id = question_id(name, variables)
            ids.append(id)
            # Display-name changes and key order must never change identity.
            assert id == question_id(
                name,
                {**dict(reversed(list(variables.items()))), "app_name": "New label"},
            )
            text = QUESTION_SPECS[name]["question_template"].format(
                **variables, resolution_date="2030-01-01", forecast_due_date="2029-12-01"
            )
            assert "{" not in text and "}" not in text
            criteria = spec["resolution_criteria"].format(**variables, url="https://example.com")
            assert criteria.strip() and "{" not in criteria and "}" not in criteria
    assert len(ids) == len(set(ids))
    variables = QUESTION_SPECS["youtube_channel_max_video_views"]["variables"][0]
    assert question_id("youtube_channel_max_video_views", variables) != question_id(
        "youtube_channel_max_video_views", {**variables, "percentage_threshold": 10}
    )
    assert question_id("test", {"id": "example"}) == "test__example"


@pytest.mark.parametrize("name", EXPECTED)
def test_missing_results_are_not_zero(source, monkeypatch, name):
    configure(monkeypatch, name)
    response = Mock()
    response.json.return_value = {}
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    # Date-sensitive parsers reject mismatched responses; both paths retain missing data.
    try:
        fetched = source.fetch()
    except RuntimeError:
        assert name == "google_finance_ticker_price"
        return
    assert fetched.iloc[0]["value"] == "N/A"
    updated = source.update(empty_bank(), fetched)
    assert updated.dfq.iloc[0]["resolved"]
    assert next(iter(updated.resolution_files.values())).iloc[0]["value"] == "N/A"


@pytest.mark.parametrize("failure", [requests.Timeout, requests.HTTPError, ValueError])
def test_total_failure_retains_fetch_artifact_and_hides_key(source, monkeypatch, caplog, failure):
    configure(monkeypatch, "amazon_minimum_product_price")
    monkeypatch.setattr(
        serpapi.requests, "get", Mock(side_effect=failure(f"URL?api_key={source.api_key}"))
    )
    with pytest.raises(RuntimeError, match="All SerpAPI requests failed") as exc:
        source.fetch()
    assert source.api_key not in caplog.text + str(exc.value)


def test_partial_failure_is_missing_and_other_requests_continue(source, monkeypatch):
    name = "amazon_minimum_product_price"
    spec = deepcopy(QUESTION_SPECS[name])
    spec["variables"] = spec["variables"][:2]
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {"amazon_minimum_product_price": spec})
    failure = Mock()
    failure.json.return_value = {"error": "engine unavailable"}
    success = Mock()
    variables = spec["variables"][1]
    success.json.return_value = response_for(
        name, variables, "2026-09-16", spec["params"](variables, "2026-09-16")
    )
    monkeypatch.setattr(serpapi.requests, "get", Mock(side_effect=[failure, success]))
    fetched = source.fetch()
    assert fetched["value"].tolist() == ["N/A", 12]
    assert source.update(empty_bank(), fetched).dfq["resolved"].tolist() == [True, False]


@pytest.mark.parametrize(
    "failure", [429, 500, 502, 503, 504, requests.Timeout, requests.ConnectionError]
)
def test_transient_request_failure_recovers_measurement(source, monkeypatch, caplog, failure):
    name = "amazon_minimum_product_price"
    spec = configure(monkeypatch, name)
    if isinstance(failure, int):
        response = requests.Response()
        response.status_code = failure
        error = requests.HTTPError(f"URL?api_key={source.api_key}", response=response)
    else:
        error = failure(f"URL?api_key={source.api_key}")
    success = Mock()
    variables = spec["variables"][0]
    success.json.return_value = response_for(
        name, variables, "2026-09-16", spec["params"](variables, "2026-09-16")
    )
    get = Mock(side_effect=[error, error, success])
    monkeypatch.setattr(serpapi.requests, "get", get)
    monkeypatch.setattr("time.sleep", Mock())
    assert source.fetch()["value"].tolist() == [12]
    assert source.api_key not in caplog.text


@pytest.mark.parametrize("status, attempts", [(503, 3), (400, 1), (401, 1), (403, 1), (404, 1)])
def test_http_failure_has_bounded_attempts_and_continues(source, monkeypatch, status, attempts):
    name = "amazon_minimum_product_price"
    spec = deepcopy(QUESTION_SPECS[name])
    spec["variables"] = spec["variables"][:2]
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", {"amazon_minimum_product_price": spec})
    response = requests.Response()
    response.status_code = status
    error = requests.HTTPError(response=response)
    success = Mock()
    variables = spec["variables"][1]
    success.json.return_value = response_for(
        name, variables, "2026-09-16", spec["params"](variables, "2026-09-16")
    )

    def get(url, *, params, timeout):
        if params["asin"] == spec["variables"][0]["asin"]:
            raise error
        return success

    request = Mock(side_effect=get)
    monkeypatch.setattr(serpapi.requests, "get", request)
    monkeypatch.setattr("time.sleep", Mock())
    assert source.fetch()["value"].tolist() == ["N/A", 12]
    assert request.call_count == attempts + 1


@pytest.mark.parametrize("name", EXPECTED)
def test_parsers_reject_wrong_entities_or_invalid_measurements(name):
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = EXPECTED[name][1]
    params = spec["params"](variables, day)
    response = response_for(name, variables, day, params)
    if name == "amazon_minimum_product_price":
        response["product_results"].pop("extracted_price")
        response["purchase_options"] = {}
        response["other_sellers"] = [
            {"extracted_price": value, "condition": "NEW"}
            for value in [0, -1, True, float("nan"), float("inf"), "9"]
        ]
    elif name == "youtube_channel_max_video_views":
        response["channel_results"]["external_id"] = "UCwrong"
    elif name == "walmart_food_drink_price":
        response["product_result"]["in_stock"] = False
    elif name == "google_finance_ticker_price":
        response["graph"] = response["graph"][-1:]
    elif name == "flight_departure_delay":
        response["flight_result"]["dates"][0]["metadata"]["status"] = "CANCELLED"
    assert spec["parse_response"](response, variables, day) is None


@pytest.fixture()
def amazon_offer():
    name = "amazon_minimum_product_price"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    return spec, variables, response


@pytest.mark.parametrize("location", ["buy_new", "single_offer", "other_sellers"])
def test_amazon_accepts_matching_new_seller_offer_in_each_json_location(amazon_offer, location):
    spec, variables, response = amazon_offer
    offer = response["purchase_options"].pop("buy_new")
    if location == "other_sellers":
        response["other_sellers"].append({"sold_by": "Amazon.com", "extracted_price": 12})
    else:
        response["purchase_options"][location] = offer
    # Cheaper third-party and used offers must not change the Amazon.com New price.
    response["raw_html"] += amazon_html(variables["asin"], 1, seller="Another seller")
    response["raw_html"] += amazon_html(variables["asin"], 2, condition="Used - Like New")
    assert spec["parse_response"](response, variables, "2026-09-16") == 12


@pytest.mark.parametrize(
    "change",
    ["missing", "used", "resale", "wrong_asin", "wrong_price", "no_button", "conflict", "currency"],
)
def test_amazon_requires_unambiguous_html_evidence(amazon_offer, change):
    spec, variables, response = amazon_offer
    html = response["raw_html"]
    if change == "missing":
        html = "<html>Offer unavailable</html>"
    elif change == "used":
        html = html.replace(">New<", ">Used - Like New<")
    elif change == "resale":
        html = html.replace("Amazon.com", "Amazon Resale")
    elif change == "wrong_asin":
        html = html.replace(variables["asin"], "B000WRONG1")
    elif change == "wrong_price":
        html = html.replace("$12.00", "$13.00")
    elif change == "no_button":
        html = html.replace("submit.addToCart", "subscribe")
    elif change == "conflict":
        html += amazon_html(variables["asin"], 13)
    elif change == "currency":
        html = html.replace("$12.00", "CAD12.00")
    response["raw_html"] = html
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize("failure", [None, "timeout", "redirect", "wrong_host", "wrong_capture"])
def test_amazon_fetch_requires_its_matching_html_capture(
    source, monkeypatch, amazon_offer, failure
):
    spec, variables, data = amazon_offer
    configure(monkeypatch, "amazon_minimum_product_price")
    # Exercise real HTML retrieval, including its transient-error retry policy.
    monkeypatch.delattr(source, "_get_amazon_html")
    html = data.pop("raw_html")
    url = "https://serpapi.com/searches/example/capture.html"
    data["search_metadata"] = {"id": "capture", "raw_html_file": url}
    if failure == "wrong_host":
        data["search_metadata"]["raw_html_file"] = url.replace("serpapi.com", "example.com")
    elif failure == "wrong_capture":
        data["search_metadata"]["id"] = "different"
    calls = []

    def get(request_url, **kwargs):
        calls.append(request_url)
        if request_url.endswith("search.json"):
            return Mock(json=Mock(return_value=data))
        assert request_url == url
        assert "params" not in kwargs  # No API key is sent to the capture URL.
        assert kwargs["allow_redirects"] is False
        if failure == "timeout":
            raise requests.Timeout()
        return Mock(text=html, is_redirect=failure == "redirect")

    monkeypatch.setattr(serpapi.requests, "get", get)
    monkeypatch.setattr("time.sleep", Mock())
    if failure is None:
        assert source.fetch().iloc[0]["value"] == 12
        assert calls == ["https://serpapi.com/search.json", url]
    else:
        with pytest.raises(RuntimeError, match="All SerpAPI requests failed"):
            source.fetch()
        if failure in {"wrong_host", "wrong_capture"}:
            assert calls == ["https://serpapi.com/search.json"]
        if failure == "timeout":
            assert calls.count(url) == 3


@pytest.mark.parametrize("returned_asin", [None, "wrong", "matching"])
def test_amazon_requires_exact_asin_and_tolerates_title_changes(amazon_offer, returned_asin):
    spec, variables, response = amazon_offer
    response["product_results"].update(
        asin=variables["asin"] if returned_asin == "matching" else returned_asin,
        title="Revised title",
    )
    assert spec["parse_response"](response, variables, "2026-09-16") == (
        12 if returned_asin == "matching" else None
    )


@pytest.mark.parametrize("caption", [None, "", "One-time purchase", "Regular Price", "Buy New"])
def test_amazon_condition_comes_from_html_not_purchase_caption(amazon_offer, caption):
    spec, variables, response = amazon_offer
    response["purchase_options"]["buy_new"]["caption"] = caption
    assert spec["parse_response"](response, variables, "2026-09-16") == 12
    response.pop("raw_html")
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize(
    "price", [None, 0, -1, True, float("nan"), float("inf"), float("-inf"), "12"]
)
def test_amazon_requires_a_positive_numeric_buy_new_price(amazon_offer, price):
    spec, variables, response = amazon_offer
    response["purchase_options"]["buy_new"]["extracted_price"] = price
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize(
    "parameter,value",
    [
        ("amazon_domain", None),
        ("amazon_domain", "amazon.ca"),
        ("delivery_zip", None),
        ("delivery_zip", "90210"),
        ("language", None),
        ("language", "es_US"),
    ],
)
def test_amazon_requires_the_configured_locale(amazon_offer, parameter, value):
    spec, variables, response = amazon_offer
    response["search_parameters"][parameter] = value
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize(
    "changes",
    [
        {"asin": "wrong"},
        {"currency": "CAD"},
        {"sponsored": True},
        {"features": {"sold_by": {"text": "Seller"}, "subscription": "Subscribe"}},
        {"features": {}},
        {"features": {"sold_by": {"text": " "}}},
    ],
)
def test_amazon_rejects_ineligible_or_unidentified_buy_new_offers(amazon_offer, changes):
    spec, variables, response = amazon_offer
    response["purchase_options"]["buy_new"].update(changes)
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize("seller_field", ["sold_by", "shipper_seller"])
def test_amazon_rejects_other_sellers_even_when_shipped_by_amazon(amazon_offer, seller_field):
    spec, variables, response = amazon_offer
    response["purchase_options"]["buy_new"].update(
        currency="USD", features={seller_field: {"text": "A different featured seller"}}
    )
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize(
    "option",
    [
        "buy_used",
        "single_offer",
        "free_and_fast",
        "refurbished_premium",
        "refurbished_excellent",
        "refurbished_good",
        "refurbished_acceptable",
        "subscribe_and_save",
    ],
)
def test_amazon_never_substitutes_other_prices_for_buy_new(amazon_offer, option):
    spec, variables, response = amazon_offer
    response["product_results"]["extracted_price"] = 1
    response["purchase_options"][option] = {"extracted_price": 2}
    response["other_sellers"] = [{"extracted_price": 3, "condition": "NEW"}]
    response["prices"] = [{"extracted_price": 4}]
    response["related_products"] = [{"extracted_price": 5}]
    offer = response["purchase_options"]["buy_new"]
    offer.update(extracted_old_price=6, extracted_price_unit=0.1, coupon="$1 off")
    assert spec["parse_response"](response, variables, "2026-09-16") == 12
    del response["purchase_options"]["buy_new"]
    assert spec["parse_response"](response, variables, "2026-09-16") is None


@pytest.mark.parametrize(
    "variables", QUESTION_SPECS["google_finance_ticker_price"]["variables"], ids=lambda v: v["id"]
)
def test_finance_question_and_background_name_the_series_and_units(variables):
    spec = QUESTION_SPECS["google_finance_ticker_price"]
    units = (
        "USD per share"
        if variables["symbol"].split(":")[1] in {"NASDAQ", "NYSEARCA", "BATS"}
        else "index points"
    )
    question = spec["question_template"].format(
        **variables, forecast_due_date="2026-09-16", resolution_date="2026-09-23"
    )
    background = spec["question_background"].format(**variables, url="https://example.com")
    # Forecasters must not need to decode exchange symbols such as 137:TLV.
    assert f"{variables['name']} ({variables['symbol']})" in question
    assert f"{variables['name']} ({variables['symbol']})" in background
    assert units in question
    assert units in background
    assert ("index points" if units == "USD per share" else "USD per share") not in background


@pytest.mark.parametrize("options", [{}, {"single_offer": {"stock": "In Stock"}}])
def test_amazon_does_not_fall_back_to_the_main_product_price(amazon_offer, options):
    spec, variables, response = amazon_offer
    response["purchase_options"] = options
    assert spec["parse_response"](response, variables, "2026-09-16") is None


def test_amazon_requests_exact_products_for_the_browser_locale(source, monkeypatch):
    name = "amazon_minimum_product_price"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    params = measurement_params({**spec, "variables": variables}, "2026-09-16")
    assert params["engine"] == "amazon_product"
    assert params["asin"] == variables["asin"]
    assert params["delivery_zip"] == "10001"
    assert params["amazon_domain"] == "amazon.com"
    assert params["language"] == "en_US"
    assert params["no_cache"] == "true"
    id = question_id(name, variables)
    fetched = pd.DataFrame([{"id": id, "date": "2026-09-16", "value": 9}]).assign(
        requested_date="2026-09-16", fetch_datetime="2026-09-16T01:00:00Z"
    )
    result = source.update(empty_bank(), fetched)
    assert result.resolution_files[id]["value"].tolist() == [9]
    assert "sold by Amazon.com" in result.dfq.iloc[0]["question"]
    assert params["other_sellers"] == "true"


@pytest.mark.parametrize(
    "price,seller,fallback,expected",
    [
        (13, "Amazon.com", False, 1),
        (12, "Amazon.com", False, 0),
        (11, "Amazon.com", False, 0),
        (13, "Third party", False, None),
        (13, "Third party", True, 1),
    ],
)
def test_amazon_seller_lifecycle_keeps_legacy_prices_separate(
    source, monkeypatch, freeze_today, price, seller, fallback, expected
):
    name = "amazon_minimum_product_price"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    id = question_id(name, variables)
    # This ASIN was previously collected from whichever seller won the featured offer.
    legacy_id = f"{name}__{variables['asin'].lower()}-buy-new"
    assert id != legacy_id
    legacy = make_resolution_df(
        [
            {"id": legacy_id, "date": "2026-09-01", "value": 1},
            {"id": legacy_id, "date": "2026-09-08", "value": 100},
        ]
    )
    histories = {legacy_id: legacy}
    bank = make_question_df([{"id": legacy_id, "question": "Old minimum-price question"}])
    response = Mock()
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    observations = [
        (date(2026, 9, 1), 12, "Amazon.com"),
        (date(2026, 9, 8), price, seller),
    ]
    if fallback:
        observations.append((date(2026, 9, 10), price, "Amazon.com"))
    for day, value, label in observations:
        freeze_today(day)
        data = response_for(name, variables, day.isoformat(), spec["params"](variables, str(day)))
        data["purchase_options"]["buy_new"].update(
            caption="One-time purchase",
            extracted_price=value,
            features={"sold_by": {"text": label}},
        )
        data["raw_html"] = amazon_html(variables["asin"], value, seller=label)
        response.json.return_value = data
        updated = source.update(bank, source.fetch(), existing_resolution_files=histories)
        histories.update(updated.resolution_files)
        bank = updated.dfq

    old_question = bank.set_index("id").loc[legacy_id]
    assert old_question["resolved"]  # Retired from sampling, with its old wording preserved.
    assert old_question["question"] == "Old minimum-price question"
    assert histories[id].iloc[0]["value"] == 12
    assert (histories[id]["id"] == id).all()
    freeze_today(date(2026, 9, 16))  # The target's seven-day fallback window has closed.
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-09-01",
                "resolution_date": "2026-09-08",
            }
        ]
    )
    history = make_resolution_df(
        pd.concat(histories.values(), ignore_index=True).to_dict("records")
    )
    resolved, _ = source.resolve(forecast, bank, history, forecast_due_date=date(2026, 9, 1))
    row = resolved.iloc[0]
    assert row["market_value_on_due_date"] == 12
    if expected is None:
        assert not row["resolved"]
    else:
        assert row["resolved"] and row["resolved_to"] == expected


@pytest.mark.parametrize(
    "item_changes,price_map,expected",
    [
        ({}, {"price": 9}, 9),
        ({}, {"price": 9, "currency": "USD"}, 9),
        ({}, {"price": 9, "currency": "CAD"}, "N/A"),
        ({"us_item_id": "wrong"}, {"price": 9}, "N/A"),
        ({"in_stock": False}, {"price": 9}, "N/A"),
        ({"in_stock": None}, {"price": 9}, "N/A"),
        ({}, {"unit_price": 1, "list_price": 9}, "N/A"),
        *[
            ({}, {"price": value}, "N/A")
            for value in [0, -1, True, "9", float("nan"), float("inf")]
        ],
    ],
)
def test_walmart_direct_item_price(source, monkeypatch, item_changes, price_map, expected):
    name = "walmart_food_drink_price"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]

    def get(url, *, params, timeout):
        assert params["engine"] == "walmart_product"
        assert params["product_id"] == variables["us_item_id"]
        assert params["store_id"] == variables["store_id"]
        data = response_for(name, variables, "2026-09-16", params)
        data["product_result"].update(item_changes, price_map=price_map)
        response = Mock()
        response.json.return_value = data
        return response

    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    assert fetched.iloc[0]["value"] == expected
    assert fetched.iloc[0]["id"] == question_id(name, variables)
    question = source.update(empty_bank(), fetched).dfq.iloc[0]
    assert f"store {variables['store_id']}" in question["background"]


@pytest.mark.parametrize("returned_store", ["3081", 3081, "2152", None, ""])
def test_walmart_requires_matching_returned_store(source, monkeypatch, returned_store):
    name = "walmart_food_drink_price"
    spec = configure(monkeypatch, name)
    variables = spec["variables"][0]
    assert variables["store_id"] == "3081"
    data = response_for(name, variables, "2026-09-16", {})
    if returned_store is None:
        data.pop("search_information")
    else:
        data["search_information"]["location"]["store_id"] = returned_store
    response = Mock()
    response.json.return_value = data
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    matches = str(returned_store) == variables["store_id"]
    assert fetched.iloc[0]["value"] == (9 if matches else "N/A")
    question = source.update(empty_bank(), fetched).dfq.iloc[0]
    assert bool(question["resolved"]) is not matches


@pytest.mark.parametrize(
    "variables",
    QUESTION_SPECS["walmart_food_drink_price"]["variables"],
    ids=lambda variables: variables["id"],
)
def test_walmart_store_matches_forecaster_instructions(source, monkeypatch, variables):
    name = "walmart_food_drink_price"
    spec = configure(monkeypatch, name)
    spec["variables"] = [variables]

    def get(url, *, params, timeout):
        assert params["store_id"] == variables["store_id"]
        assert params["product_id"] == variables["us_item_id"]
        response = Mock()
        response.json.return_value = response_for(name, variables, "2026-09-16", params)
        return response

    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    question = source.update(empty_bank(), fetched).dfq.iloc[0]
    background = question["background"]
    assert variables["store_name"] in background
    assert f"store {variables['store_id']}" in background
    assert variables["store_address"] in background
    assert "Open this product URL in your browser" in background
    assert "does not automatically select this store" in background
    assert question["url"] == f"https://www.walmart.com/ip/{variables['us_item_id']}"
    # A question is permanent, so its ID must change whenever the item or store does.
    assert variables["id"] == f"{variables['us_item_id']}-store-{variables['store_id']}"


@pytest.mark.parametrize(
    "variables",
    QUESTION_SPECS["youtube_channel_max_video_views"]["variables"],
    ids=lambda variables: variables["id"],
)
def test_youtube_fetch_targets_channel_popular_videos(source, monkeypatch, variables):
    name = "youtube_channel_max_video_views"
    spec = configure(monkeypatch, name)
    spec["variables"] = [variables]

    def get(url, *, params, timeout):
        assert params["engine"] == "youtube_channel"
        assert params["channel_id"] == variables["channel_handle"]
        assert params["tab"] == "videos"
        assert params["sort"] == "popular"
        assert "search_query" not in params
        response = Mock()
        response.json.return_value = response_for(name, variables, "2026-09-16", params)
        return response

    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    assert fetched.iloc[0]["value"] == 12345  # Higher Shorts and stream counts are excluded.
    question = source.update(empty_bank(), fetched).dfq.iloc[0]
    expected_url = f"https://www.youtube.com/channel/{variables['channel_id']}/videos"
    assert question["url"] == expected_url
    assert expected_url in question["background"]
    assert "Select Popular" in question["background"]
    assert "excluding shorts, live streams and archived streams" in question["background"]


@pytest.mark.parametrize(
    "field,value",
    [("tab", "shorts"), ("tab", "streams"), ("sort", "latest"), ("sort", "oldest"), ("sort", None)],
)
def test_youtube_rejects_wrong_tab_or_sort(field, value):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    params = {**spec["params"](variables, day), field: value}
    response = response_for(name, variables, day, params)
    assert parse_youtube_max_views(response, variables, day) is None


@pytest.mark.parametrize("external_id", [None, "", "UCwrong", "ucy1kmzp36iqsynx_9h4mpcg"])
def test_youtube_rejects_missing_or_wrong_channel_id(external_id):
    spec = QUESTION_SPECS["youtube_channel_max_video_views"]
    variables = spec["variables"][0]
    response = response_for(
        "youtube_channel_max_video_views",
        variables,
        "2026-09-16",
        {
            "tab": "videos",
            "sort": "popular",
        },
    )
    response["channel_results"] = {"external_id": external_id, "handle": "@MarkRober"}
    assert parse_youtube_max_views(response, variables, "2026-09-16") is None


def test_youtube_accepts_handle_and_title_changes_for_same_channel():
    spec = QUESTION_SPECS["youtube_channel_max_video_views"]
    variables = spec["variables"][0]
    response = response_for(
        "youtube_channel_max_video_views",
        variables,
        "2026-09-16",
        {
            "tab": "videos",
            "sort": "popular",
        },
    )
    response["channel_results"].update(handle="@newhandle", title="New channel name")
    assert parse_youtube_max_views(response, variables, "2026-09-16") == 12345


@pytest.mark.parametrize(
    "counts,expected",
    [
        ([], None),
        ([None, True, -1, "1200", float("nan"), float("inf")], None),
        ([0], 0),
        ([12345, None, 1200, -1], 12345),
        ([None, 1200], None),
        ([12345, 1200], 12345),
    ],
)
def test_youtube_only_uses_valid_video_view_counts(counts, expected):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    response["videos_results"] = [
        {"extracted_views": count, "views": f"{count} views"} for count in counts
    ]
    assert parse_youtube_max_views(response, variables, day) == expected


def test_youtube_rejects_listing_not_ranked_by_views():
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    response["videos_results"].reverse()
    # A page labelled Popular whose top video is not the most viewed was sorted otherwise.
    assert parse_youtube_max_views(response, variables, day) is None


@pytest.mark.parametrize(
    "filters",
    [None, [], [{"title": "Popular"}], [{"title": "Members only", "selected": True}]],
)
def test_youtube_requires_returned_popular_listing(filters):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    response["filters"] = filters
    # An echoed sort=popular does not confirm which listing YouTube actually served.
    assert parse_youtube_max_views(response, variables, day) is None


@pytest.mark.parametrize("label", ["11 days ago", "2 weeks ago", "1 month ago", None, ""])
@pytest.mark.parametrize("position,expected", [(0, None), (1, 12345)])
def test_youtube_rejects_upload_ages_even_with_numeric_extracted_views(label, position, expected):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    response["videos_results"][position] = {"views": label, "extracted_views": 99999999}
    # Returning the second video's count would also understate an unknown maximum.
    assert parse_youtube_max_views(response, variables, day) == expected


@pytest.mark.parametrize(
    "label,count",
    [("1 view", 1), ("No views", 0), ("223M views", 223000000), ("2.4B views", 2400000000)],
)
def test_youtube_accepts_displayed_view_counts(label, count):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    day = "2026-09-16"
    response = response_for(name, variables, day, spec["params"](variables, day))
    response["videos_results"] = [{"views": label, "extracted_views": count}]
    assert parse_youtube_max_views(response, variables, day) == count


@pytest.mark.parametrize("extracted", [8199999, 99999999, None])
def test_youtube_expands_displayed_count_without_extraction_errors(extracted):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    response = response_for(name, variables, "2026-09-16", spec["params"](variables, "2026-09-16"))
    response["videos_results"] = [{"views": "8.2M views", "extracted_views": extracted}]
    assert parse_youtube_max_views(response, variables, "2026-09-16") == 8200000


@pytest.mark.parametrize("label", ["1,2 views", "1.5 views", "NoK views", "1..2M views"])
def test_youtube_rejects_malformed_displayed_counts(label):
    name = "youtube_channel_max_video_views"
    spec = QUESTION_SPECS[name]
    variables = spec["variables"][0]
    response = response_for(name, variables, "2026-09-16", spec["params"](variables, "2026-09-16"))
    response["videos_results"] = [{"views": label, "extracted_views": 1200}]
    assert parse_youtube_max_views(response, variables, "2026-09-16") is None


@pytest.mark.parametrize("day", ["2026-09-15", "2026-09-17"])
def test_youtube_cannot_collect_historical_or_future_views(day):
    spec = QUESTION_SPECS["youtube_channel_max_video_views"]
    measurement = {**spec, "variables": spec["variables"][0]}
    with pytest.raises(ValueError, match="only be collected for today"):
        measurement_params(measurement, day, today=date(2026, 9, 16))


def test_missing_api_key_fails_before_requests(monkeypatch):
    get = Mock()
    monkeypatch.setattr(serpapi.requests, "get", get)
    with pytest.raises(RuntimeError, match="api_key"):
        SerpapiSource().fetch()
    get.assert_not_called()


@pytest.mark.parametrize(
    "name,fill",
    [
        ("google_finance_ticker_price", True),
        ("flight_departure_delay", False),
        ("amazon_minimum_product_price", False),
    ],
)
def test_history_missing_dates_and_failed_retries(source, monkeypatch, name, fill):
    configure(monkeypatch, name)
    response = Mock()
    spec = serpapi.QUESTION_SPECS[name]
    params = spec["params"](spec["variables"][0], EXPECTED[name][1])
    response.json.return_value = response_for(name, spec["variables"][0], EXPECTED[name][1], params)
    monkeypatch.setattr(serpapi.requests, "get", Mock(return_value=response))
    fetched = source.fetch()
    id = fetched.iloc[0]["id"]
    first = source.update(empty_bank(), fetched)
    replay = fetched.copy()
    replay["value"] = "N/A"
    second = source.update(first.dfq, replay, existing_resolution_files=first.resolution_files)
    pd.testing.assert_frame_equal(first.resolution_files[id], second.resolution_files[id])
    assert not second.dfq.iloc[0]["resolved"]
    # Simulate a missed run and then a run that returns no measurement.
    replay["date"] = (pd.Timestamp(replay.iloc[0]["date"]) + pd.Timedelta(days=3)).strftime(
        "%Y-%m-%d"
    )
    third = source.update(second.dfq, replay, existing_resolution_files=second.resolution_files)
    history = third.resolution_files[id]
    assert len(history) == 2
    available = fill or name == "flight_departure_delay"
    assert bool(third.dfq.iloc[0]["resolved"]) is not available
    assert history.iloc[-1]["value"] == "N/A"


@pytest.mark.parametrize(
    "baseline,target,threshold,expected",
    [
        (100, 105, 5, 1),
        (100, 104, 5, 0),
        (100, 100, 5, 0),
        (100, 106, 5, 1),
        (2.4, 2.46, 2.5, 1),
        (2.4, 2.4599, 2.5, 0),
        (2.4, 2.4601, 2.5, 1),
        (0, 10, 5, None),
        (-100, 105, 5, None),
        ("N/A", 105, 5, None),  # A missing baseline cannot borrow the resolution snapshot.
        (100, "N/A", 5, None),
        (100, None, 5, None),
    ],
)
def test_percentage_resolution_inclusive_boundary_and_missing_baseline(
    source, baseline, target, threshold, expected
):
    id = f"youtube_channel_max_video_views__markrober__gte{threshold}pct"
    df = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-01-01",
                "resolution_date": "2026-02-01",
            }
        ]
    )
    rows = [{"id": id, "date": "2026-01-01", "value": baseline}]
    if target is not None:
        rows.append({"id": id, "date": "2026-02-01", "value": target})
    # No dependency on today's configuration or bank: archived questions keep their threshold.
    result, _ = source.resolve(
        df, empty_bank(), make_resolution_df(rows), forecast_due_date=date(2026, 1, 1)
    )
    row = result.iloc[0]
    assert bool(row["resolved"]) == (expected is not None)
    assert pd.isna(row["resolved_to"]) if expected is None else row["resolved_to"] == expected


def test_standard_and_percentage_combo_resolution(source):
    social = "youtube_channel_max_video_views__markrober__gte2pct"
    standard = "amazon_minimum_product_price__crayola-52-0064"
    df = make_forecast_df(
        [
            {"id": social, "direction": ()},
            {"id": standard, "direction": ()},
            {"id": (social, standard), "direction": (-1, 1)},
        ]
    )
    df["source"] = "serpapi"
    df["forecast_due_date"] = pd.Timestamp("2026-01-01")
    df["resolution_date"] = pd.Timestamp("2026-02-01")
    rows = [
        {"id": id, "date": day, "value": value}
        for id in (social, standard)
        for day, value in [("2026-01-01", 100), ("2026-02-01", 101)]
    ]
    result, _ = source.resolve(
        df, empty_bank(), make_resolution_df(rows), forecast_due_date=date(2026, 1, 1)
    )
    assert dict(zip(result["id"], result["resolved_to"])) == {
        social: 0,
        standard: 1,
        (social, standard): 1,
    }


@pytest.fixture()
def baseline():
    with imported_with_stubs(
        "base_eval.naive_and_dummy_forecasters.main",
        {
            "helpers.question_sets": SimpleNamespace(),
            "prophet": SimpleNamespace(Prophet=Mock()),
        },
    ) as module:
        yield module


def inputs(id, history):
    df = pd.DataFrame(
        [{"id": id, "source": "serpapi", "resolution_date": date(2026, 2, 1), "forecast": None}]
    )
    dfr = pd.DataFrame(
        [{"id": id, "date": pd.Timestamp(day), "value": value} for day, value in history],
        columns=["id", "date", "value"],
    )
    dfr["date"] = pd.to_datetime(dfr["date"])
    return df, dfr


@pytest.mark.parametrize(
    "history",
    [
        [],
        [("2026-01-01", 100)],
        [("2026-01-01", "N/A"), ("2026-01-02", "N/A")],
        [("2026-01-01", "N/A"), ("2026-01-02", 100)],
    ],
)
def test_new_series_with_too_little_history_gets_neutral_forecast(baseline, history):
    df, dfr = inputs("amazon_minimum_product_price__example", history)
    result = baseline.get_dataset_forecasts("serpapi", df, dfr, pd.Timestamp("2026-01-03"))
    assert result.iloc[0]["forecast"] == 0.5
    baseline.Prophet.assert_not_called()


@pytest.mark.parametrize("suffix,expected", [("", 0.95), ("__gte5pct", 0.5)])
def test_baseline_uses_question_threshold_and_only_past_observations(baseline, suffix, expected):
    id = "youtube_channel_max_video_views__example" + suffix
    df, dfr = inputs(id, [("2026-01-01", 99), ("2026-01-02", 100), ("2026-01-03", 999)])
    model = baseline.Prophet.return_value
    model.predict.return_value = pd.DataFrame(
        {
            "ds": [pd.Timestamp("2026-02-01")],
            "yhat": [105],
            "yhat_lower": [102.44],
            "yhat_upper": [107.56],
        }
    )
    result = baseline.get_dataset_forecasts("serpapi", df, dfr, pd.Timestamp("2026-01-03"))
    fitted = model.fit.call_args.args[0]
    assert fitted["y"].tolist() == [99, 100]
    assert result.iloc[0]["forecast"] == pytest.approx(expected)


@pytest.mark.parametrize(
    "suffix,value,prediction,expected",
    [
        ("", 100, 100, 0.05),
        ("__gte10pct", 100, 110, 0.95),
        ("__gte2.5pct", 2.4, 2.46, 0.95),
        ("__gte2.5pct", 2.4, 2.4599, 0.05),
        ("__gte2.5pct", 2.4, 2.4601, 0.95),
    ],
)
def test_constant_prediction_has_finite_probability(baseline, suffix, value, prediction, expected):
    id = "youtube_channel_max_video_views__example" + suffix
    df, dfr = inputs(id, [("2026-01-01", value), ("2026-01-02", value)])
    model = baseline.Prophet.return_value
    model.predict.return_value = pd.DataFrame(
        {
            "ds": [pd.Timestamp("2026-02-01")],
            "yhat": [prediction],
            "yhat_lower": [prediction],
            "yhat_upper": [prediction],
        }
    )
    result = baseline.get_dataset_forecasts("serpapi", df, dfr, pd.Timestamp("2026-01-03"))
    assert result.iloc[0]["forecast"] == expected


def test_registered_for_curation_and_resolution():
    from sources.registry import DATASET_SOURCES

    assert DATASET_SOURCES["serpapi"].source_type == SourceType.DATASET
    assert "serpapi" in question_curation.FREEZE_QUESTION_DATA_SOURCES


@pytest.mark.parametrize(
    "name,expected",
    [
        ("amazon_minimum_product_price", 1),
        ("walmart_food_drink_price", 1),
        ("youtube_channel_max_video_views", 1),
        ("flight_departure_delay", None),
    ],
)
def test_resolution_uses_next_snapshot_but_never_another_flight(source, name, expected):
    variables = QUESTION_SPECS[name]["variables"][0]
    id = question_id(name, variables)
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-01-01",
                "resolution_date": "2026-02-01",
            }
        ]
    )
    history = make_resolution_df(
        [
            {"id": id, "date": day, "value": value}
            for day, value in [
                ("2025-12-31", 999),
                ("2026-01-01", "N/A"),
                ("2026-01-03", 100),
                ("2026-01-04", 999),
                ("2026-02-01", "N/A"),
                ("2026-02-02", 110),
                ("2026-02-03", 0),
            ]
        ]
    )
    original = history.copy(deep=True)
    result, _ = source.resolve(forecast, empty_bank(), history, forecast_due_date=date(2026, 1, 1))
    pd.testing.assert_frame_equal(history, original)
    row = result.iloc[0]
    if expected is None:
        assert not row["resolved"]
    else:
        assert row["market_value_on_due_date"] == 100
        assert row["resolved_to"] == expected


def test_snapshot_retries_and_multiple_dates_preserve_history(source, monkeypatch):
    name = "amazon_minimum_product_price"
    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    fetched = pd.DataFrame(
        [
            {
                "id": id,
                "date": day,
                "requested_date": day,
                "value": value,
                "fetch_datetime": f"{day}T01:00:00Z",
            }
            for day, value in [
                ("2026-09-15", 10),
                ("2026-09-15", 20),
                ("2026-09-16", "N/A"),
                ("2026-09-16", 30),
            ]
        ]
    )
    result = source.update(empty_bank(), fetched)
    assert result.resolution_files[id]["value"].tolist() == [10, 30]
    assert result.dfq.iloc[0]["freeze_datetime_value"] == "30.0"


@pytest.mark.parametrize(
    "price,expected", [(2.82, "2.82"), (3.97, "3.97"), (4439.1445, "4439.1445")]
)
def test_freeze_value_shows_the_stored_price_without_float_noise(
    source, monkeypatch, price, expected
):
    name = "walmart_food_drink_price"
    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    # Read the fetch output the way the update job does; pandas' fast JSON float parser can
    # turn 2.82 into 2.8200000000000003.
    fetched = pd.read_json(
        StringIO(
            f'{{"id": "{id}", "date": "2026-09-16", "value": {price}, '
            '"requested_date": "2026-09-16", "fetch_datetime": "2026-09-16T01:00:00Z"}\n'
        ),
        lines=True,
        dtype={"id": str},
        convert_dates=False,
    )
    result = source.update(empty_bank(), fetched)
    assert result.dfq.iloc[0]["freeze_datetime_value"] == expected


@pytest.mark.parametrize("name", EXPECTED)
@pytest.mark.parametrize("latest_value", [30, "N/A"])
def test_replayed_fetch_keeps_latest_bank_value_and_availability(
    source, monkeypatch, name, latest_value
):
    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    fetched = pd.DataFrame([{"id": id, "date": "2026-09-14", "value": 10}]).assign(
        requested_date="2026-09-14", fetch_datetime="2026-09-15T01:00:00Z"
    )
    existing = pd.DataFrame(
        [
            {"id": id, "date": "2026-09-14", "value": 10},
            {"id": id, "date": "2026-09-15", "value": latest_value},
        ]
    )
    result = source.update(empty_bank(), fetched, existing_resolution_files={id: existing})
    question = result.dfq.iloc[0]
    available = spec.get("fill_missing_dates", False) or name == "flight_departure_delay"
    if latest_value == "N/A" and not available:
        assert question["resolved"]
        assert question["freeze_datetime_value"] == "N/A"
    else:
        expected = 10 if latest_value == "N/A" else latest_value
        assert not question["resolved"]
        assert float(question["freeze_datetime_value"]) == expected
    value_date = (
        "2026-09-14" if name == "flight_departure_delay" and latest_value == "N/A" else "2026-09-15"
    )
    assert value_date in question["freeze_datetime_value_explanation"]
    assert result.resolution_files[id]["date"].max() == "2026-09-15"


@pytest.mark.parametrize("name", ["google_finance_ticker_price", "flight_departure_delay"])
def test_stale_fetch_cannot_undo_saved_corrections(source, monkeypatch, name, tmp_path):
    from orchestration import _source_io

    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    fetched = pd.DataFrame(
        [
            {"id": id, "date": day, "value": value}
            for day, value in [("2026-09-13", 110), ("2026-09-14", 105)]
        ]
    ).assign(requested_date="2026-09-15", fetch_datetime="2026-09-16T02:00:00Z")
    first = source.update(empty_bank(), fetched)
    # Exercise the actual upload serialization so provenance survives a new process.
    saved_file = tmp_path / "history.jsonl"
    to_json = pd.DataFrame.to_json
    monkeypatch.setattr(
        pd.DataFrame, "to_json", lambda frame, path, **kw: to_json(frame, saved_file, **kw)
    )
    monkeypatch.setattr(_source_io.gcp.storage, "upload", Mock())
    monkeypatch.setenv("QUESTION_BANK_BUCKET", "test-bucket")
    _source_io.upload_resolution_files(
        "serpapi", first.resolution_files, extra_columns=("fetch_datetime",)
    )
    saved = pd.read_json(saved_file, lines=True, convert_dates=False)
    stale = fetched.copy()
    stale.loc[0, "value"] = 100
    stale["fetch_datetime"] = "2026-09-16T01:00:00Z"
    second = source.update(first.dfq, stale, existing_resolution_files={id: saved})
    forecast = make_forecast_df(
        [
            dict(
                id=id,
                source="serpapi",
                forecast_due_date="2026-09-13",
                resolution_date="2026-09-14",
            )
        ]
    )
    result, _ = source.resolve(
        forecast,
        second.dfq,
        make_resolution_df(second.resolution_files[id].to_dict("records")),
        forecast_due_date=date(2026, 9, 13),
    )
    assert result.iloc[0]["resolved_to"] == 0
    # A failed newer request cannot hide a successful intervening correction.
    failed = fetched.assign(value="N/A", fetch_datetime="2026-09-16T04:00:00Z")
    third = source.update(second.dfq, failed, existing_resolution_files=second.resolution_files)
    later = stale.assign(fetch_datetime="2026-09-16T03:00:00Z")
    fourth = source.update(third.dfq, later, existing_resolution_files=third.resolution_files)
    assert fourth.resolution_files[id]["value"].tolist() == [100, 105]


@pytest.mark.parametrize(
    "name,max_age",
    [
        ("amazon_minimum_product_price", 1),
        ("walmart_food_drink_price", 1),
        ("youtube_channel_max_video_views", 1),
        ("google_finance_ticker_price", 14),
        ("flight_departure_delay", 7),
    ],
)
@pytest.mark.parametrize("extra_days", [0, 1, 365])
def test_sampling_expires_without_losing_resolution_history(
    source, monkeypatch, name, max_age, extra_days
):
    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    age = max_age + extra_days
    available = extra_days == 0
    day = (pd.Timestamp("2026-09-16") - pd.Timedelta(days=age)).strftime("%Y-%m-%d")
    previous = (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    history = pd.DataFrame(
        [{"id": id, "date": previous, "value": 10}, {"id": id, "date": day, "value": 15}]
    )
    if name == "google_finance_ticker_price":
        # A later failed collection must not make the carried value appear fresh.
        missing_day = (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        history.loc[len(history)] = {"id": id, "date": missing_day, "value": "N/A"}
    # Even a replay of the last fetch must use today's sampling cutoff.
    fetched_day = history.iloc[-1]["date"]
    fetched = history.iloc[[-1]].assign(
        requested_date=fetched_day, fetch_datetime=f"{fetched_day}T12:00:00Z"
    )
    updated = source.update(empty_bank(), fetched, existing_resolution_files={id: history})
    question = updated.dfq.iloc[0]
    assert bool(question["resolved"]) is not available
    assert (question["freeze_datetime_value"] != "N/A") is available
    assert updated.resolution_files[id]["date"].tolist() == history["date"].tolist()
    assert updated.resolution_files[id]["value"].tolist() == history["value"].tolist()
    forecast = make_forecast_df(
        [dict(id=id, source="serpapi", forecast_due_date=previous, resolution_date=day)]
    )
    result, _ = source.resolve(
        forecast,
        updated.dfq,
        make_resolution_df(updated.resolution_files[id].to_dict("records")),
        forecast_due_date=pd.Timestamp(previous).date(),
    )
    assert result.iloc[0]["resolved_to"] == 1


@pytest.mark.parametrize(
    "name,retired",
    [
        (name, retired)
        for name in (
            "amazon_minimum_product_price",
            "walmart_food_drink_price",
            "youtube_channel_max_video_views",
        )
        for retired in (False, True)
    ]
    + [("additional_snapshot_series", False)],
)
@pytest.mark.parametrize(
    "resolution_date,snapshots,expected",
    [
        # A missing date uses a snapshot saved up to 7 days later.
        ("2026-02-01", [("2026-01-08", 100), ("2026-02-08", 200)], 1),
        # Later snapshots are too stale to stand in for either date.
        ("2026-02-01", [("2026-01-09", 100), ("2026-02-02", 200)], None),
        ("2026-02-01", [("2026-01-02", 100), ("2026-02-09", 200)], None),
        # A collection gap spanning both dates must not resolve from one snapshot.
        ("2026-01-08", [("2026-01-08", 100)], None),
        ("2026-02-01", [("2026-02-03", 100)], None),
    ],
)
def test_snapshot_gaps_resolve_only_from_nearby_distinct_snapshots(
    source, monkeypatch, name, retired, resolution_date, snapshots, expected
):
    spec = QUESTION_SPECS.get(name, QUESTION_SPECS["amazon_minimum_product_price"])
    monkeypatch.setattr(
        serpapi, "QUESTION_SPECS", {} if retired else {**QUESTION_SPECS, name: spec}
    )
    id = question_id(name, spec["variables"][0])
    forecast = make_forecast_df(
        [
            {
                "id": id,
                "source": "serpapi",
                "forecast_due_date": "2026-01-01",
                "resolution_date": resolution_date,
            }
        ]
    )
    history = make_resolution_df(
        [{"id": id, "date": day, "value": value} for day, value in snapshots]
    )
    result, _ = source.resolve(forecast, empty_bank(), history, forecast_due_date=date(2026, 1, 1))
    row = result.iloc[0]
    if expected is None:
        assert not row["resolved"]
    else:
        assert row["market_value_on_due_date"] == 100
        assert row["resolved_to"] == expected


@pytest.mark.parametrize(
    "name",
    ["amazon_minimum_product_price", "walmart_food_drink_price", "youtube_channel_max_video_views"],
)
@pytest.mark.parametrize(
    "resolution_dates", [("2026-01-08", "2026-01-31"), ("2026-01-31", "2026-01-08")]
)
@pytest.mark.parametrize("baseline_date", ["2026-01-07", "2026-01-08"])
def test_snapshot_baselines_are_valid_for_each_horizon(
    source, name, resolution_dates, baseline_date
):
    id = question_id(name, QUESTION_SPECS[name]["variables"][0])
    forecast = make_forecast_df(
        [
            dict(id=id, source="serpapi", forecast_due_date="2026-01-01", resolution_date=day)
            for day in resolution_dates
        ]
    )
    history = make_resolution_df(
        [
            {"id": id, "date": baseline_date, "value": 100},
            {"id": id, "date": "2026-01-09", "value": 120},
            {"id": id, "date": "2026-01-31", "value": 150},
        ]
    )
    result, _ = source.resolve(forecast, empty_bank(), history, forecast_due_date=date(2026, 1, 1))
    result = result.set_index("resolution_date")
    short = result.loc[pd.Timestamp("2026-01-08")]
    if baseline_date == "2026-01-08":
        assert not short["resolved"]
        assert pd.isna(short["resolved_to"])
        assert pd.isna(short["market_value_on_due_date"])
    else:
        assert short["resolved_to"] == 1
    long = result.loc[pd.Timestamp("2026-01-31")]
    assert long["market_value_on_due_date"] == 100
    assert long["resolved_to"] == 1


@pytest.mark.parametrize("name", ["amazon_minimum_product_price", "walmart_food_drink_price"])
def test_fetch_crossing_midnight_does_not_misdate_snapshots(
    source, monkeypatch, freeze_today, name
):
    spec = configure(monkeypatch, name)
    spec["variables"] = [spec["variables"][0], {**spec["variables"][0], "id": "second"}]
    calls = 0

    def get(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            freeze_today(date(2026, 9, 17))
        response = Mock()
        response.json.return_value = response_for(
            name, spec["variables"][0], "2026-09-16", kwargs["params"]
        )
        return response

    monkeypatch.setattr(serpapi.requests, "get", get)
    fetched = source.fetch()
    assert fetched["date"].tolist() == ["2026-09-16", "2026-09-17"]
    assert fetched["value"].tolist() == ["N/A", EXPECTED[name][0]]
    assert fetched["fetch_datetime"].str[:10].tolist() == fetched["date"].tolist()


@pytest.mark.parametrize("rows", [[], [{"id": "example", "value": 9}]])
def test_fetch_reads_named_secret_and_uploads_measurements(monkeypatch, tmp_path, rows):
    keys = SimpleNamespace(API_KEY_SERPAPI="test-key")
    slack = Mock()
    output = tmp_path / "serpapi_fetch.jsonl"
    uploaded = []
    with imported_with_stubs(
        "orchestration.func_serpapi_fetch.main", {"helpers.keys": keys, "helpers.slack": slack}
    ) as job:
        fetched = pd.DataFrame(rows, columns=["id", "value"])
        monkeypatch.setattr(
            job.requests,
            "get",
            Mock(
                return_value=Mock(
                    json=Mock(return_value={"this_month_usage": 0, "searches_per_month": 15000})
                )
            ),
        )
        source = Mock()
        source.fetch.return_value = fetched
        monkeypatch.setattr(job, "SerpapiSource", Mock(return_value=source))
        monkeypatch.setattr(
            job._source_io.data_utils,
            "generate_filenames",
            lambda _: {"local_fetch": str(output), "jsonl_fetch": output.name},
        )
        monkeypatch.setattr(
            job._source_io.gcp.storage,
            "upload",
            lambda **_: uploaded.append(
                [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            ),
        )
        job.driver(None)
        assert source.api_key == "test-key"
        assert uploaded == [rows]
        slack.send_message.assert_not_called()


@pytest.mark.parametrize(
    "empty_categories",
    [
        set(),
        {"amazon_minimum_product_price"},
        {"amazon_minimum_product_price", "walmart_food_drink_price"},
    ],
)
@pytest.mark.parametrize("slack_fails", [False, True])
def test_fetch_warns_for_empty_categories_after_saving_data(
    source, monkeypatch, tmp_path, caplog, empty_categories, slack_fails
):
    """Only fully empty attempted categories alert, without losing successful observations."""
    specs = deepcopy(QUESTION_SPECS)
    for name, spec in specs.items():
        spec["variables"] = spec["variables"][: 3 if name == "amazon_minimum_product_price" else 1]
    monkeypatch.setattr(serpapi, "QUESTION_SPECS", specs)
    amazon = specs["amazon_minimum_product_price"]["variables"]
    skipped = {
        question_id("amazon_minimum_product_price", amazon[2]),
        question_id(
            "youtube_channel_max_video_views",
            specs["youtube_channel_max_video_views"]["variables"][0],
        ),
    }
    monkeypatch.setattr(source, "get_nullified_ids", lambda: skipped)

    def get(url, *, params, timeout):
        if url == "https://serpapi.com/account.json":
            return Mock(
                json=Mock(return_value={"this_month_usage": 0, "searches_per_month": 15000})
            )
        name, spec = next(
            (name, spec) for name, spec in specs.items() if spec["engine"] == params["engine"]
        )
        variables = spec["variables"][0]
        if name == "amazon_minimum_product_price":
            variables = next(v for v in amazon if v["asin"] == params["asin"])
            if variables == amazon[0]:
                raise requests.HTTPError(f"URL?api_key={source.api_key}")
        data = response_for(name, variables, "2026-09-14", params)
        if name in empty_categories:
            data = {}  # HTTP success with no usable price must also trigger the warning.
        if name == "google_finance_ticker_price":
            data["graph"] = [{"date": "Sep 14 2026, 05:30 PM UTC+02:00", "price": 101}]
        return Mock(json=Mock(return_value=data))

    monkeypatch.setattr(serpapi.requests, "get", get)
    output = tmp_path / "serpapi_fetch.jsonl"
    uploaded = []
    messages = []

    def send_message(*, message):
        assert uploaded
        messages.append(message)
        if slack_fails:
            raise RuntimeError("Slack unavailable")

    with imported_with_stubs(
        "orchestration.func_serpapi_fetch.main",
        {
            "helpers.keys": SimpleNamespace(API_KEY_SERPAPI=source.api_key),
            "helpers.slack": SimpleNamespace(send_message=send_message),
        },
    ) as job:
        monkeypatch.setattr(job, "SerpapiSource", lambda: source)
        monkeypatch.setattr(
            job._source_io.data_utils,
            "generate_filenames",
            lambda _: {"local_fetch": str(output), "jsonl_fetch": output.name},
        )
        monkeypatch.setattr(
            job._source_io.gcp.storage,
            "upload",
            lambda **_: uploaded.append(output.read_text(encoding="utf-8")),
        )
        job.driver(None)

    saved = pd.read_json(StringIO(uploaded[0]), lines=True, convert_dates=False)
    assert not skipped.intersection(saved["id"])
    assert saved[saved["id"].str.startswith("walmart_")]["value"].tolist() == [
        "N/A" if "walmart_food_drink_price" in empty_categories else 9
    ]
    for name in ("google_finance_ticker_price", "flight_departure_delay"):
        history = saved[saved["id"].str.startswith(name)]
        assert history["date"].tolist() == ["2026-09-14", "2026-09-15"]
        assert history["value"].tolist() == [
            101 if name == "google_finance_ticker_price" else 0,
            "N/A",
        ]
    assert len(messages) == bool(empty_categories)
    if messages:
        assert "amazon_minimum_product_price (2 attempted IDs)" in messages[0]
        assert {name for name in specs if name in messages[0]} == empty_categories
    assert source.api_key not in caplog.text + "".join(messages)


@pytest.mark.parametrize("consumed", [0, 10000, 10001, 20000])
@pytest.mark.parametrize("renewal", ["2026-10-28", None])
def test_fetch_monthly_usage_warning(monkeypatch, consumed, renewal):
    """Nightly runs warn strictly above the threshold using the actual plan allowance."""
    slack = Mock()
    account = {
        "this_month_usage": consumed,
        "searches_per_month": 20000,
        "plan_renewal_date": renewal,
    }
    with imported_with_stubs(
        "orchestration.func_serpapi_fetch.main",
        {"helpers.keys": SimpleNamespace(API_KEY_SERPAPI="test-key"), "helpers.slack": slack},
    ) as job:
        source = Mock()
        source.fetch.return_value = pd.DataFrame([{"id": "example", "value": 9}])
        monkeypatch.setattr(job, "SerpapiSource", Mock(return_value=source))
        upload = Mock()
        monkeypatch.setattr(job._source_io, "write_fetch_output", upload)
        monkeypatch.setattr(
            job.requests, "get", Mock(return_value=Mock(json=Mock(return_value=account)))
        )
        # Each night's run should alert while over the threshold, with no persistent state.
        job.driver(None)
        job.driver(None)
        assert upload.call_count == 2
    assert slack.send_message.call_count == (2 if consumed > 10000 else 0)
    if consumed > 10000:
        message = slack.send_message.call_args.kwargs["message"]
        assert "Monthly allowance: 20,000 credits" in message
        assert f"Consumed this billing cycle: {consumed:,} credits" in message
        assert f"Subscription renewal: {renewal or 'Unavailable'}" in message


@pytest.mark.parametrize("failure", ["http", "timeout", "json", "missing", "slack", "fetch"])
def test_fetch_usage_notification_failures(monkeypatch, caplog, failure):
    """Usage checks cannot lose data or hide fetch failures, or expose credentials in logs."""
    secret = "private-test-key"
    slack = Mock()
    response = Mock(
        json=Mock(return_value={"this_month_usage": 12000, "searches_per_month": 15000})
    )
    get = Mock(return_value=response)
    if failure == "http":
        response.raise_for_status.side_effect = requests.HTTPError(f"URL?api_key={secret}")
    elif failure == "timeout":
        get.side_effect = requests.Timeout(f"URL?api_key={secret}")
    elif failure == "json":
        response.json.side_effect = ValueError(secret)
    elif failure == "missing":
        response.json.return_value = {}
    elif failure == "slack":
        slack.send_message.side_effect = RuntimeError(secret)
    with imported_with_stubs(
        "orchestration.func_serpapi_fetch.main",
        {"helpers.keys": SimpleNamespace(API_KEY_SERPAPI=secret), "helpers.slack": slack},
    ) as job:
        source = Mock()
        source.fetch.return_value = pd.DataFrame([{"id": "example", "value": 9}])
        monkeypatch.setattr(job, "SerpapiSource", Mock(return_value=source))
        monkeypatch.setattr(job.requests, "get", get)
        upload = Mock()
        monkeypatch.setattr(job._source_io, "write_fetch_output", upload)
        if failure == "fetch":
            source.fetch.side_effect = RuntimeError("Fetch failed")
            with pytest.raises(RuntimeError, match="Fetch failed"):
                job.driver(None)
            upload.assert_not_called()
            slack.send_message.assert_called_once()
        else:
            job.driver(None)
            upload.assert_called_once()
            assert "monthly usage notification failed" in caplog.text
    assert secret not in caplog.text


@pytest.mark.parametrize(
    "name",
    ["amazon_minimum_product_price", "google_finance_ticker_price", "flight_departure_delay"],
)
@pytest.mark.parametrize(
    "download_error", [None, NotFound, Forbidden, ServiceUnavailable, TimeoutError]
)
def test_update_preserves_history_and_stops_on_download_errors(
    monkeypatch, tmp_path, name, download_error
):
    from orchestration.func_serpapi_update import main as job

    spec = configure(monkeypatch, name)
    id = question_id(name, spec["variables"][0])
    fetched = pd.DataFrame([{"id": id}]).assign(
        date="2026-09-16",
        value=20,
        requested_date="2026-09-16",
        fetch_datetime="2026-09-16T01:00:00Z",
    )
    questions = empty_bank() if download_error is NotFound else make_question_df([{"id": id}])
    filenames = job.data_utils.generate_filenames("serpapi")
    filenames["local_question"] = str(tmp_path / "serpapi_questions.jsonl")
    filenames["local_fetch"] = str(tmp_path / "serpapi_fetch.jsonl")
    # Exercise the shared reader with both an existing bank and the manually seeded empty file.
    (tmp_path / "serpapi_questions.jsonl").write_text(
        "" if questions.empty else questions.to_json(orient="records", lines=True), encoding="utf-8"
    )
    fetched.to_json(filenames["local_fetch"], orient="records", lines=True)
    monkeypatch.setattr(job.data_utils, "generate_filenames", lambda _: filenames)
    monkeypatch.setattr(job.data_utils.gcp.storage, "download_no_error_message_on_404", Mock())
    history_file = tmp_path / "history.jsonl"
    pd.DataFrame(
        [{"id": id, "date": "2026-09-15", "value": 19, "fetch_datetime": "2026-09-15T01:00:00Z"}]
    ).to_json(history_file, orient="records", lines=True)
    download = Mock(return_value=str(history_file))
    if download_error:
        download.side_effect = download_error("History download failed")
    monkeypatch.setattr(job._source_io.gcp.storage, "download", download)
    upload_questions = Mock()
    uploaded = []
    saved_file = tmp_path / "uploaded.jsonl"
    to_json = pd.DataFrame.to_json
    monkeypatch.setattr(
        pd.DataFrame, "to_json", lambda frame, path, **kw: to_json(frame, saved_file, **kw)
    )
    monkeypatch.setattr(job.data_utils, "upload_questions", upload_questions)
    monkeypatch.setattr(
        job._source_io.gcp.storage,
        "upload",
        lambda **_: uploaded.append(pd.read_json(saved_file, lines=True, convert_dates=False)),
    )

    if download_error not in (None, NotFound):
        with pytest.raises(download_error):
            job.driver(None)
        upload_questions.assert_not_called()
        assert not uploaded
        return

    job.driver(None)
    history = uploaded[0]
    expected_dates = ["2026-09-16"] if download_error else ["2026-09-15", "2026-09-16"]
    expected_values = [20] if download_error else [19, 20]
    assert history["date"].tolist() == expected_dates
    assert history["value"].tolist() == expected_values
    if name == "amazon_minimum_product_price":
        assert "fetch_datetime" not in history
    else:
        assert pd.Timestamp(history.iloc[-1]["fetch_datetime"]) == pd.Timestamp(
            fetched.iloc[0]["fetch_datetime"]
        )
        if download_error is None:
            assert pd.Timestamp(history.iloc[0]["fetch_datetime"]) == pd.Timestamp(
                "2026-09-15T01:00:00Z"
            )
    assert upload_questions.call_args.args[0]["id"].tolist() == [id]
