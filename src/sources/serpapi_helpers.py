"""Build date-specific SerpAPI requests and parse validated measurements."""

import re
from collections.abc import Iterator
from datetime import date
from math import isfinite

from bs4 import BeautifulSoup

from helpers import dates

# These engines return current snapshots only, so requests must target today;
# historical comparisons use snapshots saved on previous collection dates.
TODAY_ONLY_ENGINES = {
    "amazon_product": "Amazon prices",
    "walmart_product": "Walmart prices",
}

# Walmart.com seller identity verified in a Walmart Product response on 2026-10-03.
WALMART_SELLER_ID = "F55CDC31AB754BB68FE0B39041159D63"


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
    if engine == "google" and requested >= today:
        raise ValueError("Flight delay requires a past requested_date.")
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
    """Yield dated measurements, including recoverable flight history.

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
    if name == "flight_departure_delay":
        # Collect all available departed flights through the request's UTC day.
        # Keep scheduled local date labels; the requested date is emitted below.
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
            if not valid_date or day > today.isoformat() or day == requested_date:
                continue
            delay = measurement["parse_response"](response, variables, day)
            if is_finite_number(delay):
                yield day, delay
    value = measurement["parse_response"](response, variables, requested_date)
    if measurement["engine"] in TODAY_ONLY_ENGINES and dates.get_date_today() != today:
        # A current snapshot returned after midnight cannot represent yesterday.
        value = None
    yield observation_date, value


def parse_walmart_price(response: dict, variables: dict, requested_date: str) -> float | None:
    """Return Walmart.com's in-stock product price for the confirmed selected store.

    Use only product_result, matching us_item_id and the returned location's store_id.
    Require in_stock true, seller_name Walmart.com and seller_id WALMART_SELLER_ID.
    The price must be finite and positive. Omitted currency means USD; reject other
    currencies and missing or conflicting required fields. Ignore alternative offers.
    """
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
        and item.get("seller_id") == WALMART_SELLER_ID
        and item.get("seller_name") == "Walmart.com"
        and item.get("in_stock") is True
        and offer.get("currency", "USD") == "USD"
        and is_finite_number(price)
        and price > 0
    ):
        return float(price)
    return None


def parse_amazon_seller_price(response: dict, variables: dict, requested_date: str) -> float | None:
    """Require an Amazon.com New offer in the same capture's HTML and JSON.

    Require a matching product ASIN and SerpAPI-reported search parameters of
    amazon.com, en_US and ZIP 10001; these do not independently verify delivery settings.
    The HTML offer must identify the exact ASIN, New condition and Amazon.com seller
    together with an ordinary Add to Cart USD price. JSON purchase options or
    other-seller results must confirm that price, with every seller field in the
    confirming offer agreeing on Amazon.com. Other-seller JSON omits condition;
    omitted JSON currency means USD. The literal label 'Buy New' is not required.
    Missing confirmation or conflicting qualifying HTML prices are missing measurements,
    never a minimum across sellers.
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
        sellers = [
            seller
            for seller in (
                offer.get("sold_by"),
                (features.get("sold_by") or {}).get("text"),
                (features.get("shipper_seller") or {}).get("text"),
            )
            if seller is not None
        ]
        if (
            sellers
            and all(seller == "Amazon.com" for seller in sellers)
            and not features.get("subscription")
            and not offer.get("sponsored", False)
            and offer.get("asin", variables["asin"]) == variables["asin"]
            and offer.get("currency", "USD") == "USD"
            and is_finite_number(offer.get("extracted_price"))
            and offer["extracted_price"] == price
        ):
            return price
    return None


def parse_google_flight_departure_delay(
    response: dict, variables: dict, requested_date: str
) -> float | None:
    """Return nonnegative departure delay in minutes for the exact departed flight.

    Match the departure date, flight designator, and airport pair. Early departures
    count as zero delay; missing or not-yet-departed flights return None.
    Google only supplies a limited set of dates, not a historical archive.
    """
    # Status prefixes after departure, when the departure delay is final. Airborne flights report
    # IN_AIR_* (e.g. IN_AIR_ON_TIME); Google drops their date before the next daily fetch, so
    # rejecting them loses the measurement. Landed flights can stay at
    # ON_THE_RUNWAY_AT_DESTINATION(_DELAYED) and never reach ARRIVED.
    DEPARTED_STATUSES = (
        "DEPARTED",
        "IN_AIR",
        "ON_THE_RUNWAY_AT_DESTINATION",
        "LANDED",
        "ARRIVED",
    )
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
