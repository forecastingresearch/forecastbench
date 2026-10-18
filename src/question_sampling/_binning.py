"""Bin market questions by probability and time horizon; the bin weights steer the sampling."""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


MARKET_VALUE_CONFIG = [
    {"min": 0.00, "max": 0.01, "weight": 0},
    {"min": 0.01, "max": 0.10, "weight": 0},
    {"min": 0.10, "max": 0.20, "weight": 0.125},
    {"min": 0.20, "max": 0.30, "weight": 0.125},
    {"min": 0.30, "max": 0.40, "weight": 0.125},
    {"min": 0.40, "max": 0.50, "weight": 0.125},
    {"min": 0.50, "max": 0.60, "weight": 0.125},
    {"min": 0.60, "max": 0.70, "weight": 0.125},
    {"min": 0.70, "max": 0.80, "weight": 0.125},
    {"min": 0.80, "max": 0.90, "weight": 0.125, "inclusive_max": True},
    {"min": 0.90, "max": 0.99, "weight": 0},
    {"min": 0.99, "max": 1.00, "weight": 0},
]

TIME_HORIZON_CONFIG = [
    {"min": 0, "max": 7, "weight": 0},
    {"min": 8, "max": 25, "weight": 0.20},
    {"min": 26, "max": 50, "weight": 0.35},
    {"min": 51, "max": 70, "weight": 0.20},
    {"min": 71, "max": 90, "weight": 0.1},
    {"min": 91, "max": 120, "weight": 0.08},
    {"min": 121, "max": 180, "weight": 0.05},
    {"min": 181, "max": 365, "weight": 0.02},
    {"min": 366, "max": float("inf"), "weight": 0},
]


UNKNOWN_BIN_WEIGHT = 0.0


def get_bin_label(bin_config: dict, bin_type: str) -> str:
    """Generate a label for a bin configuration.

    Args:
        bin_config (dict): Dict with min and max values
        bin_type (str): Either "market_value" or "time_horizon"

    Returns
        label (str): Bin label like "0.0-0.1%" or "0-7d"

    Raises:
        ValueError: If bin_type is not recognized
    """
    if bin_type == "market_value":
        suffix = "%"
    elif bin_type == "time_horizon":
        suffix = "d"
    else:
        raise ValueError(
            f"Invalid bin_type: '{bin_type}'. Must be 'market_value' or 'time_horizon'"
        )

    return f"{bin_config['min']}-{bin_config['max']}{suffix}"


def get_market_value_bin(value: float | str | None) -> str:
    """Assign market value to a probability bin.

    Args:
        value (float | str | None): Market probability value (0-1) or "N/A"/None

    Returns
        bin_label (str): Bin label from get_bin_label() or "unknown"
    """
    if pd.isna(value) or value == "N/A":
        return "unknown"

    try:
        val = float(value)
    except ValueError, TypeError:
        return "unknown"

    for bin_config in MARKET_VALUE_CONFIG:
        inclusive_max = bin_config.get("inclusive_max", False)
        if inclusive_max:
            # [min, max]
            if bin_config["min"] <= val <= bin_config["max"]:
                return get_bin_label(bin_config, "market_value")
        else:
            # [min, max)
            if bin_config["min"] <= val < bin_config["max"]:
                return get_bin_label(bin_config, "market_value")

    return "unknown"


def get_time_horizon_bin(close_datetime: str | None, forecast_datetime: datetime) -> str:
    """Assign question to a time horizon bin based on market close date.

    Args:
        close_datetime (str | None): ISO format datetime string of market close
        forecast_datetime (datetime): The forecast due date the horizon is measured from

    Returns
        bin_label (str): Time bin like "31-90d" or "unknown"
    """
    if pd.isna(close_datetime) or close_datetime == "N/A":
        return "unknown"

    try:
        close_date = datetime.fromisoformat(close_datetime)
        days_until_close = (close_date - forecast_datetime).days
        for horizon_config in TIME_HORIZON_CONFIG:
            if horizon_config["min"] <= days_until_close <= horizon_config["max"]:
                return get_bin_label(horizon_config, "time_horizon")
        return "unknown"
    except ValueError, TypeError:
        return "unknown"


def add_bin_columns(dfq: pd.DataFrame, forecast_datetime: datetime) -> pd.DataFrame:
    """Add bin columns to dataframe for market value and time horizon.

    Args:
        dfq (pd.DataFrame): Market questions
        forecast_datetime (datetime): The forecast due date the time horizon is measured from

    Returns
        dfq (pd.DataFrame): Copy with market_value_bin and time_horizon_bin columns
    """
    dfq = dfq.copy()
    dfq["market_value_bin"] = dfq["freeze_datetime_value"].apply(get_market_value_bin)
    dfq["time_horizon_bin"] = dfq["market_info_close_datetime"].apply(
        get_time_horizon_bin, forecast_datetime=forecast_datetime
    )

    n_unknown = (
        (dfq["time_horizon_bin"] == "unknown") | (dfq["market_value_bin"] == "unknown")
    ).sum()
    if n_unknown > 0:
        logger.warning(f"{n_unknown} questions have unknown bins (will not be sampled)")

    return dfq


def create_composite_bins(dfq: pd.DataFrame) -> pd.DataFrame:
    """Create composite bin identifier combining all dimensions.

    Args:
        dfq (pd.DataFrame): DataFrame with market_value_bin and time_horizon_bin columns

    Returns
        dfq (pd.DataFrame): DataFrame with composite_bin column added
    """
    dfq["composite_bin"] = dfq["market_value_bin"] + "_" + dfq["time_horizon_bin"]
    return dfq


def calculate_bin_weights(dfq: pd.DataFrame) -> pd.DataFrame:
    """Calculate target weights for each composite bin and add to DataFrame.

    Args:
        dfq (pd.DataFrame): DataFrame with composite bins

    Returns
        dfq (pd.DataFrame): DataFrame with bin_weight column
    """

    def build_weight_lookup(configs: list[dict], bin_type: str) -> dict[str, float]:
        weights = {get_bin_label(c, bin_type): c["weight"] for c in configs}
        weights["unknown"] = UNKNOWN_BIN_WEIGHT
        return weights

    market_value_weights = build_weight_lookup(MARKET_VALUE_CONFIG, "market_value")
    time_horizon_weights = build_weight_lookup(TIME_HORIZON_CONFIG, "time_horizon")

    composite_weights = {}
    unique_bins = dfq[["composite_bin", "market_value_bin", "time_horizon_bin"]].drop_duplicates()
    for _, row in unique_bins.iterrows():
        bin_key = row["composite_bin"]
        mv_weight = market_value_weights.get(row["market_value_bin"], 0)
        th_weight = time_horizon_weights.get(row["time_horizon_bin"], 0)
        composite_weights[bin_key] = mv_weight * th_weight

    # Normalize composite weights
    total_weight = sum(composite_weights.values())
    if total_weight > 0:
        composite_weights = {k: v / total_weight for k, v in composite_weights.items()}

    dfq = dfq.copy()
    dfq["bin_weight"] = dfq["composite_bin"].map(composite_weights).fillna(0)
    return dfq
