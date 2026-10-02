"""Build date-specific SerpAPI requests and parse validated measurements."""

import re
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from math import isfinite

import pandas as pd
from bs4 import BeautifulSoup

from helpers import dates

SAVED_MEASUREMENT_RESOLUTION_CRITERIA = (
    "Resolution uses ForecastBench’s saved SerpApi measurements under the rules below; "
    "values viewed independently on the linked website do not override those measurements.\n\n"
)

# These engines return current snapshots only, so requests must target today;
# historical comparisons use snapshots saved on previous collection dates.
TODAY_ONLY_ENGINES = {
    "amazon_product": "Amazon prices",
    "youtube_channel": "YouTube channel views",
    "walmart_product": "Walmart prices",
}


def is_finite_number(value: object) -> bool:
    """Accept finite built-in integers and floats, excluding booleans."""
    return type(value) in (int, float) and isfinite(value)


def measurement_params(measurement: dict, requested_date: str, today: date | None = None) -> dict:
    """Build a request for one ISO date, rejecting unsupported date choices."""
    requested = date.fromisoformat(requested_date)
    if requested.isoformat() != requested_date:
        raise ValueError("requested_date must use YYYY-MM-DD.")
    today = today if today is not None else dates.get_date_today()
    engine = measurement["engine"]
    if engine in {"google", "google_finance"} and requested >= today:
        raise ValueError("Flight delay and Finance require a past requested_date.")
    if engine in TODAY_ONLY_ENGINES and requested != today:
        raise ValueError(f"{TODAY_ONLY_ENGINES[engine]} can only be collected for today.")
    return {"engine": engine, **measurement["params"](measurement["variables"], requested_date)}


def iter_measurements(
    name: str,
    measurement: dict,
    response: dict,
    requested_date: str,
    observation_date: str,
    today: date,
) -> Iterator[tuple[str, int | float | None]]:
    """Yield dated measurements, including recoverable Finance and flight history.

    Yield as parsing proceeds so a later error does not discard earlier flight dates.

    Args:
        name (str): Configured question name.
        measurement (dict): Question spec with variables for one entity.
        response (dict): Validated API response object.
        requested_date (str): Date targeted by the request.
        observation_date (str): Date to store the requested measurement under.
        today (date): UTC date when the request started.
    """
    variables = measurement["variables"]
    if measurement["engine"] == "google_finance":
        start = (pd.Timestamp(today) - pd.DateOffset(months=1)).date()
        prices = {
            day: price
            for day, price in parse_google_finance_history(response, variables).items()
            if start.isoformat() <= day <= requested_date
        }
        if prices:
            # Persist observations only; filling before merging can erase
            # a saved observation omitted from a partial response.
            prices.setdefault(requested_date, None)
            yield from sorted(prices.items())
            return
    elif name == "flight_departure_delay":
        # Recover older flights that depart after their first fetch.
        flight_dates = {
            flight.get("date")
            for flight in (response.get("flight_result") or {}).get("dates", [])
            if isinstance(flight.get("date"), str)
        }
        for day in sorted(flight_dates):
            try:
                valid_date = date.fromisoformat(day).isoformat() == day
            except ValueError:
                continue
            if not valid_date or day >= requested_date:
                continue
            delay = measurement["parse_response"](response, variables, day)
            if is_finite_number(delay):
                yield day, delay
    value = measurement["parse_response"](response, variables, requested_date)
    if measurement["engine"] in TODAY_ONLY_ENGINES and dates.get_date_today() != today:
        # A current snapshot returned after midnight cannot represent yesterday.
        value = None
    yield observation_date, value


def google_finance_window(requested_date: str) -> str:
    """Choose the smallest graph window covering the requested date."""
    requested = date.fromisoformat(requested_date)
    today = dates.get_date_today()
    for months, window in [(1, "1M"), (6, "6M"), (12, "1Y"), (60, "5Y")]:
        if requested > (pd.Timestamp(today) - pd.DateOffset(months=months)).date():
            return window
    return "MAX"


def parse_walmart_price(response: dict, variables: dict, requested_date: str) -> float | None:
    """Return the in-stock product's USD price only for the confirmed selected store."""
    if "error" in response:
        raise RuntimeError(response["error"])
    item = response.get("product_result") or {}
    offer = item.get("price_map") or {}
    price = offer.get("price")
    location = (response.get("search_information") or {}).get("location") or {}
    # The Walmart Product endpoint serves walmart.com; omitted currency means USD.
    if (
        str(item.get("us_item_id")) == str(variables["us_item_id"])
        and str(location.get("store_id")) == str(variables["store_id"])
        and item.get("in_stock") is True
        and offer.get("currency", "USD") == "USD"
        and is_finite_number(price)
        and price > 0
    ):
        return float(price)
    return None


def parse_youtube_max_views(response: dict, variables: dict, requested_date: str) -> int | None:
    """Return the channel's highest displayed view count from its Popular Videos tab."""
    if "error" in response:
        raise RuntimeError(response["error"])
    parameters = response.get("search_parameters") or {}
    channel_id = (response.get("channel_results") or {}).get("external_id")
    if (
        parameters.get("tab", "videos") != "videos"
        or parameters.get("sort") != "popular"
        or channel_id != variables["channel_id"]
        or not any(
            item.get("title") == "Popular" and item.get("selected") is True
            for item in response.get("filters") or []
        )
    ):
        return None
    counts = []
    for index, video in enumerate(response.get("videos_results") or []):
        # Parse the display exactly: extracted_views can misread ages or round down by one.
        label = video.get("views")
        match = re.fullmatch(
            r"(?:(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?([KMB]?)|No) views?",
            label if isinstance(label, str) else "",
            re.IGNORECASE,
        )
        views = None
        if match:
            number, fraction, suffix = match.groups()
            views = Decimal((number or "0").replace(",", "") + (fraction or ""))
            views *= {"": 1, "K": 1000, "M": 1000000, "B": 1000000000}[(suffix or "").upper()]
        if views is None or views != views.to_integral_value():
            # The top-ranked Popular video must have a readable count.
            if index == 0:
                return None
            continue
        counts.append(int(views))
    # Popular ranks the most-viewed video first; otherwise the sort was not applied.
    return counts[0] if counts and counts[0] == max(counts) else None


def parse_amazon_seller_price(response: dict, variables: dict, requested_date: str) -> float | None:
    """Require an Amazon.com New offer in the same capture's HTML and JSON.

    Other-seller JSON omits condition. The HTML offer must identify the exact ASIN,
    New condition and Amazon.com seller together with an ordinary Add to Cart price.
    Conflicting qualifying prices are missing, never a minimum across sellers.
    """
    if "error" in response:
        raise RuntimeError(response["error"])
    product = response.get("product_results") or {}
    if product.get("asin") != variables["asin"]:
        return None
    params = response.get("search_parameters") or {}
    if (
        params.get("amazon_domain") != "amazon.com"
        or params.get("delivery_zip") != "10001"
        or params.get("language") != "en_US"
    ):
        return None
    html = response.get("raw_html")
    if not isinstance(html, str) or not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    prices = set()
    for block in soup.select('[id="aod-pinned-offer"], [id="aod-offer"]'):
        heading = block.select_one('[id="aod-offer-heading"]')
        seller = block.select_one('[id="aod-offer-soldBy"]')
        if (
            heading is None
            or heading.get_text(" ", strip=True) != "New"
            or seller is None
            or seller.get_text(" ", strip=True) != "Sold by Amazon.com"
        ):
            continue
        asins = {node.get("value") for node in block.select('input[name="items[0.base][asin]"]')}
        buttons = block.select('input[name="submit.addToCart"]')
        if asins != {variables["asin"]} or len(buttons) != 1:
            return None
        # This accessible price excludes shipping, crossed-out/unit prices and coupons.
        match = re.fullmatch(
            r"Add to Cart from seller Amazon\.com and price \$([\d,]+\.\d{2})",
            buttons[0].get("aria-label", "").strip(),
        )
        if match is None:
            return None
        price = float(match[1].replace(",", ""))
        if not is_finite_number(price) or price <= 0:
            return None
        prices.add(price)
    if len(prices) != 1:
        return None
    price = prices.pop()
    options = response.get("purchase_options") or {}
    offers = [options.get(key) for key in ("buy_new", "single_offer")]
    offers.extend(response.get("other_sellers") or [])
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        features = offer.get("features") or {}
        seller = features.get("sold_by") or features.get("shipper_seller") or {}
        if (
            (offer.get("sold_by") == "Amazon.com" or seller.get("text") == "Amazon.com")
            and not features.get("subscription")
            and not offer.get("sponsored", False)
            and offer.get("asin", variables["asin"]) == variables["asin"]
            and offer.get("currency", "USD") == "USD"
            and is_finite_number(offer.get("extracted_price"))
            and offer["extracted_price"] == price
        ):
            return price
    return None


def parse_google_finance_price(
    response: dict, variables: dict, requested_date: str
) -> float | None:
    """Return the last graph value on the exact date in its native quoted units.

    Use each timestamp's own local date, without shifting it to UTC. Missing
    dates return None; graph prices are not guaranteed official closing prices.
    Stock indices are measured in index points, without currency conversion.
    """
    return parse_google_finance_history(response, variables).get(requested_date)


def parse_google_finance_history(response: dict, variables: dict) -> dict[str, float]:
    """Return the last valid graph price per local date, without filling missing dates."""
    if "error" in response:
        raise RuntimeError(response["error"])
    if response.get("search_parameters", {}).get("q") != variables["symbol"]:
        raise ValueError("Google Finance response does not match the requested symbol.")
    observations = {}
    for point in response.get("graph") or []:
        if not isinstance(point, dict) or not is_finite_number(point.get("price")):
            continue
        timestamp_text = point.get("date")
        if not isinstance(timestamp_text, str):
            continue
        if timestamp_text.endswith(" UTC"):
            timestamp_text += "+00:00"
        try:
            timestamp = datetime.strptime(timestamp_text, "%b %d %Y, %I:%M %p UTC%z")
        except ValueError:
            continue
        day = timestamp.date().isoformat()
        if day not in observations or timestamp >= observations[day][0]:
            observations[day] = (timestamp, point.get("price"))
    return {day: float(price) for day, (_, price) in observations.items()}


def parse_google_flight_departure_delay(
    response: dict, variables: dict, requested_date: str
) -> float | None:
    """Return nonnegative departure delay in minutes for the exact departed flight.

    Match the departure date, flight designator, and airport pair. Early departures
    count as zero delay; missing or not-yet-departed flights return None.
    Google only supplies a limited set of dates, not a historical archive.
    """
    # Status prefixes after departure, when the departure delay is final. Landed flights can
    # stay at ON_THE_RUNWAY_AT_DESTINATION(_DELAYED) and never reach ARRIVED.
    DEPARTED_STATUSES = ("DEPARTED", "ON_THE_RUNWAY_AT_DESTINATION", "LANDED", "ARRIVED")
    if "error" in response:
        raise RuntimeError(response["error"])
    result = response.get("flight_result") or {}
    requested_flight = re.sub(r"\s+", "", variables["flight_id"]).upper()
    matches = []
    for flight in result.get("dates", []):
        metadata = flight.get("metadata") or {}
        # Live responses can omit the top-level designator shown in the docs.
        designator = (
            flight.get("flight_designator")
            or result.get("flight_designator")
            or f"{metadata.get('airline_iata_code', '')}{metadata.get('flight_number', '')}"
        )
        if (
            flight.get("date") == requested_date
            and re.sub(r"\s+", "", designator).upper() == requested_flight
            and metadata.get("origin") == variables["origin"]
            and metadata.get("destination") == variables["destination"]
            and str(metadata.get("status")).startswith(DEPARTED_STATUSES)
        ):
            matches.append(metadata)
    if len(matches) != 1:
        return None
    delay = matches[0].get("departure_delay")
    # SerpApi documents departure_delay as integer minutes; reject other types.
    return float(max(0, delay)) if type(delay) is int else None
