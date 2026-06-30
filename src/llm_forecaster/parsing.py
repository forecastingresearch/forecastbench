"""Forecast response parsing."""

import ast
import re
from typing import Any

from llm_forecaster import prompts

_PROBABILITY_TOKEN = r"(?:\*)?(\d*\.?\d+)(?:\*)?"
_PROBABILITY_PATTERN = re.compile(_PROBABILITY_TOKEN)
_EXTRACTION_ERROR = "Expected a Python list of numeric probabilities"


def extract_probability(text: str | None) -> float | None:
    """Parse a stripped response containing only one probability token."""
    if text is None or text.strip() == "":
        return None

    probability_match = _PROBABILITY_PATTERN.fullmatch(text.strip())
    if probability_match is None:
        return None

    number = float(probability_match.group(1))
    if 0 <= number <= 1:
        return number
    return None


def extract_probabilities(text: str | None) -> list[float]:
    """Parse a stripped response containing only probability tokens."""
    if text is None or text.strip() == "":
        return []

    probabilities = []
    for token in text.strip().replace(",", " ").split():
        probability = extract_probability(token)
        if probability is None:
            return []
        probabilities.append(probability)
    return probabilities


def _parse_probability_list(raw_response: str) -> list[float]:
    """Parse an extraction response as a Python list of numeric probabilities."""
    try:
        parsed = ast.literal_eval(raw_response.strip())
    except (AttributeError, SyntaxError, ValueError) as exc:
        raise ValueError(f"{_EXTRACTION_ERROR}, got: {raw_response!r}") from exc

    if not isinstance(parsed, list):
        raise ValueError(f"{_EXTRACTION_ERROR}, got: {raw_response!r}")

    probabilities = []
    for value in parsed:
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{_EXTRACTION_ERROR}, got: {raw_response!r}")
        if not 0 <= value <= 1:
            raise ValueError(f"{_EXTRACTION_ERROR}, got: {raw_response!r}")
        probabilities.append(float(value))

    return probabilities


def _extract_forecast_probabilities(
    response: str | None,
    n_horizons: int,
    forecast_extraction_model: Any,
) -> str:
    """Ask the extraction model to return stated probabilities as a Python list."""
    params = {
        "model_response": response,
        "n_horizons": n_horizons,
    }
    extraction_prompt = prompts.render_template(
        template=prompts.FORECAST_EXTRACTION_PROMPT,
        params=params,
    )
    return forecast_extraction_model.get_response(extraction_prompt)


def parse_market_forecast(
    response: str | None,
    forecast_extraction_model: Any,
) -> float:
    """Parse a market question forecast response into one probability."""
    if response is None or response.strip() == "":
        raise ValueError("Expected 1 market forecast, got 0")

    forecast = extract_probability(response)
    if forecast is not None:
        return forecast

    forecasts = _parse_probability_list(
        _extract_forecast_probabilities(
            response=response,
            n_horizons=1,
            forecast_extraction_model=forecast_extraction_model,
        )
    )
    if len(forecasts) != 1:
        raise ValueError(f"Expected 1 extracted market forecast, got {len(forecasts)}")
    return forecasts[0]


def parse_dataset_forecast(
    response: str | None,
    question: dict[str, Any],
    forecast_extraction_model: Any,
) -> list[float]:
    """Parse a dataset question forecast response into extracted horizon probabilities."""
    n_horizons = len(question["resolution_dates"])
    if response is None or response.strip() == "":
        raise ValueError(f"Expected {n_horizons} dataset forecasts, got 0")

    forecasts = extract_probabilities(response)
    if len(forecasts) == n_horizons:
        return forecasts

    forecasts = _parse_probability_list(
        _extract_forecast_probabilities(
            response=response,
            n_horizons=n_horizons,
            forecast_extraction_model=forecast_extraction_model,
        )
    )
    if len(forecasts) != n_horizons:
        raise ValueError(f"Expected {n_horizons} dataset forecasts, got {len(forecasts)}")
    return forecasts
