"""Sample questions.

This module samples questions for LLM and human question sets.

Sampling strategies:
- Market questions: Multi-dimensional binning across market value and time horizon
- Data questions: Even distribution across categories
- Human questions: Random sampling from the LLM question set

Market question sampling aims to achieve balanced representation across:
1. Market probability values (0-10%, ..., 90-100%)
2. Time horizons (0-7 days, 8-30 days, ..., >365 days)
"""

import json
import logging
import os
import random
import sys
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from fractions import Fraction

import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    question_curation,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MARKET_VALUE_CONFIG = [
    {"min": 0.00, "max": 0.01, "weight": 0.02},
    {"min": 0.01, "max": 0.10, "weight": 0.096},
    {"min": 0.10, "max": 0.20, "weight": 0.096},
    {"min": 0.20, "max": 0.30, "weight": 0.096},
    {"min": 0.30, "max": 0.40, "weight": 0.096},
    {"min": 0.40, "max": 0.50, "weight": 0.096},
    {"min": 0.50, "max": 0.60, "weight": 0.096},
    {"min": 0.60, "max": 0.70, "weight": 0.096},
    {"min": 0.70, "max": 0.80, "weight": 0.096},
    {"min": 0.80, "max": 0.90, "weight": 0.096},
    {"min": 0.90, "max": 0.99, "weight": 0.096},
    {"min": 0.99, "max": 1.00, "weight": 0.02, "inclusive_max": True},
]

TIME_HORIZON_CONFIG = [
    {"min": 0, "max": 7, "weight": 0.12},
    {"min": 8, "max": 30, "weight": 0.21},
    {"min": 31, "max": 50, "weight": 0.21},
    {"min": 51, "max": 90, "weight": 0.14},
    {"min": 91, "max": 180, "weight": 0.14},
    {"min": 181, "max": 365, "weight": 0.14},
    {"min": 366, "max": float("inf"), "weight": 0.04},
]

UNKNOWN_BIN_WEIGHT = 0.0


class QuestionSetTarget(str, Enum):
    """Question set targets used throughout sampling and writing."""

    LLM = "llm"
    HUMAN = "human"


def validate_bin_weights():
    """Verify that MARKET_VALUE_CONFIG and TIME_HORIZON_CONFIG weights each sum to 1.

    Uses Fraction to avoid floating-point rounding errors in the summation.
    """
    for name, config in [
        ("MARKET_VALUE_CONFIG", MARKET_VALUE_CONFIG),
        ("TIME_HORIZON_CONFIG", TIME_HORIZON_CONFIG),
    ]:
        total = sum(Fraction(str(c["weight"])) for c in config)
        if total != 1:
            raise ValueError(f"{name} weights sum to {float(total)}, expected 1")


def process_questions(
    questions: dict,
    to_questions: dict,
    single_generation_func: Callable,
    show_plots: bool,
    question_set_target: QuestionSetTarget,
) -> dict:
    """Sample from `questions` to get the number of questions needed.

    Args:
        questions (dict): Source questions keyed by source name, each with a "dfq" DataFrame
        to_questions (dict): Allocation info keyed by source with "num_questions_to_sample"
        single_generation_func (Callable): Sampling function taking (values, n) and returning DataFrame
        show_plots (bool): Whether to display distribution plots
        question_set_target (QuestionSetTarget): Target question set ("llm" or "human")

    Returns
        processed_questions (dict): Deep copy of questions with sampled DataFrames
    """
    num_found = 0
    processed_questions = deepcopy(questions)
    source_summaries = []
    market_sampled = []
    market_available = []

    for source, values in processed_questions.items():
        num_single = to_questions[source]["num_questions_to_sample"]
        df_available = values["dfq"].copy()

        # Sample questions for this source
        values["dfq"] = single_generation_func(values, num_single)
        df_sampled = values["dfq"]
        num_found += len(df_sampled)

        # Log per-source compact table (skip for human set which is random sampling)
        if question_set_target != QuestionSetTarget.HUMAN:
            shortfalls = log_source_sampling(source, df_sampled, df_available, num_single)
        else:
            shortfalls = []
        source_summaries.append((source, len(df_sampled), num_single, shortfalls))

        is_market_source = source in question_curation.MARKET_SOURCES
        if is_market_source:
            # Keep aggregate market-source data for combined summary/logging.
            market_available.append(df_available)
            market_sampled.append(df_sampled.copy())

        if show_plots and is_market_source:
            # Plot per-source distribution
            available_with_bins = add_bin_columns(df_available)
            sampled_with_bins = add_bin_columns(df_sampled.copy())
            plot_sampling_distribution(
                sampled_with_bins,
                available_with_bins,
                num_single,
                source_name=source,
            )

    assert len(market_sampled) > 0, "Should not arrive here."

    total_market_requested = sum(
        to_questions[s]["num_questions_to_sample"]
        for s in to_questions
        if s in question_curation.MARKET_SOURCES
    )

    if show_plots:
        # Plot overall market distribution
        df_all_sampled = add_bin_columns(pd.concat(market_sampled, ignore_index=True))
        df_all_available = add_bin_columns(pd.concat(market_available, ignore_index=True))
        plot_sampling_distribution(
            df_all_sampled,
            df_all_available,
            total_market_requested,
            source_name="ALL SOURCES",
        )

    log_sampling_summary(
        source_summaries,
        question_set_target=question_set_target,
    )
    log_all_market_source_sampling(
        df_sampled=pd.concat(market_sampled, ignore_index=True),
        df_available=pd.concat(market_available, ignore_index=True),
        n_target=total_market_requested,
        question_set_target=question_set_target,
    )

    return processed_questions


def human_sample_questions(values: dict, n_single: int) -> pd.DataFrame:
    """Get questions for the human question set by sampling from LLM questions.

    Args:
        values (dict): Source data dict containing "dfq" DataFrame
        n_single (int): Number of questions to sample

    Returns
        dfq (pd.DataFrame): Randomly sampled questions
    """
    dfq = values["dfq"].copy()
    indices_to_sample_from = dfq.index.tolist()
    indices = random.sample(indices_to_sample_from, min(n_single, len(indices_to_sample_from)))
    return dfq.loc[indices]


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
    except (ValueError, TypeError):
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


def get_time_horizon_bin(close_datetime: str | None) -> str:
    """Assign question to a time horizon bin based on market close date.

    Args:
        close_datetime (str | None): ISO format datetime string of market close

    Returns
        bin_label (str): Time bin like "31-90d" or "unknown"
    """
    if pd.isna(close_datetime) or close_datetime == "N/A":
        return "unknown"

    try:
        close_date = datetime.fromisoformat(close_datetime)
        forecast_due_date = question_curation.FORECAST_DATETIME
        days_until_close = (close_date - forecast_due_date).days
        for horizon_config in TIME_HORIZON_CONFIG:
            if horizon_config["min"] <= days_until_close <= horizon_config["max"]:
                return get_bin_label(horizon_config, "time_horizon")
        return "unknown"
    except (ValueError, TypeError):
        return "unknown"


def add_bin_columns(dfq: pd.DataFrame) -> pd.DataFrame:
    """Add bin columns to dataframe for market value and time horizon.

    Args:
        dfq (pd.DataFrame): Market questions

    Returns
        dfq (pd.DataFrame): Copy with market_value_bin and time_horizon_bin columns
    """
    dfq = dfq.copy()
    dfq["market_value_bin"] = dfq["freeze_datetime_value"].apply(get_market_value_bin)
    dfq["time_horizon_bin"] = dfq["market_info_close_datetime"].apply(get_time_horizon_bin)

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


def _log_bin_dimension(
    dim_name: str,
    bin_configs: list[dict],
    bin_type: str,
    sampled_series: pd.Series,
    available_series: pd.Series,
    n_target: int,
    prefix: str,
) -> list[tuple[str, int, int]]:
    """Log sampling results for a single bin dimension and return shortfall descriptions.

    Args:
        dim_name (str): Display name for the dimension (e.g., "Market Value")
        bin_configs (list[dict]): Bin config dicts with min, max, weight keys
        bin_type (str): Bin type for get_bin_label ("market_value" or "time_horizon")
        sampled_series (pd.Series): Bin labels for sampled questions
        available_series (pd.Series): Bin labels for all available questions
        n_target (int): Total number of questions targeted for this source
        prefix (str): Short prefix for shortfall labels (e.g., "MV", "TH")

    Returns
        shortfalls (list[tuple[str, int, int]]): (label, got, want) for bins below target
    """
    shortfalls = []
    sampled_counts = sampled_series.value_counts()
    available_counts = available_series.value_counts()
    max_label_len = max(len(get_bin_label(c, bin_type)) for c in bin_configs)
    max_label_len = max(max_label_len, len(dim_name))

    logger.info("")
    logger.info(f"  {dim_name:<{max_label_len}}  Got/Want [Avail]")
    for config in bin_configs:
        bin_label = get_bin_label(config, bin_type)
        want = int(round(n_target * config["weight"]))
        got = int(sampled_counts.get(bin_label, 0))
        avail = int(available_counts.get(bin_label, 0))
        marker = " *" if got < want and avail <= want else ""
        logger.info(f"  {bin_label:<{max_label_len}}  {got:>3}/{want:<3} [{avail:>5}]{marker}")
        if got < want:
            shortfalls.append((f"{prefix} {bin_label}", got, want))

    got_unknown = int(sampled_counts.get("unknown", 0))
    avail_unknown = int(available_counts.get("unknown", 0))
    if avail_unknown > 0 or got_unknown > 0:
        logger.info(
            f"  {'unknown':<{max_label_len}}  {got_unknown:>3}/{'0':<3} [{avail_unknown:>5}]"
        )

    return shortfalls


def log_source_sampling(
    source: str,
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
) -> list[tuple[str, int, int]]:
    """Log compact sampling results for a single source.

    For market sources, shows market value and time horizon bin distributions.
    For data sources, shows category distribution.

    Args:
        source (str): Source name (e.g., "polymarket", "acled")
        df_sampled (pd.DataFrame): Sampled questions
        df_available (pd.DataFrame): All available questions for this source
        n_target (int): Number of questions targeted for this source

    Returns
        shortfalls (list[tuple[str, int, int]]): (label, got, want) for bins below target
    """
    got = len(df_sampled)
    shortfalls = []

    logger.info("")
    logger.info(f"{source}  {got}/{n_target} sampled from {len(df_available):,} available")

    if source in question_curation.MARKET_SOURCES:
        df_sampled_bins = add_bin_columns(df_sampled.copy())
        df_available_bins = add_bin_columns(df_available.copy())

        shortfalls.extend(
            _log_bin_dimension(
                "Market Value",
                MARKET_VALUE_CONFIG,
                "market_value",
                df_sampled_bins["market_value_bin"],
                df_available_bins["market_value_bin"],
                n_target,
                prefix="MV",
            )
        )
        shortfalls.extend(
            _log_bin_dimension(
                "Time Horizon",
                TIME_HORIZON_CONFIG,
                "time_horizon",
                df_sampled_bins["time_horizon_bin"],
                df_available_bins["time_horizon_bin"],
                n_target,
                prefix="TH",
            )
        )
    else:
        # Data source - show category distribution
        categories = sorted(df_available["category"].unique())
        n_categories = len(categories)
        ideal_per_cat = n_target / n_categories if n_categories > 0 else 0

        sampled_counts = df_sampled["category"].value_counts()
        available_counts = df_available["category"].value_counts()

        max_cat_len = max((len(c) for c in categories), default=8)
        max_cat_len = max(max_cat_len, len("Category"))

        logger.info("")
        logger.info(f"  {'Category':<{max_cat_len}}  Got/Want [Avail]")
        for cat in categories:
            got_cat = int(sampled_counts.get(cat, 0))
            avail_cat = int(available_counts.get(cat, 0))
            want_cat = int(round(ideal_per_cat))
            marker = " *" if got_cat < want_cat and avail_cat <= want_cat else ""
            logger.info(
                f"  {cat:<{max_cat_len}}  {got_cat:>3}/{want_cat:<3} [{avail_cat:>5}]{marker}"
            )
            if got_cat < want_cat:
                shortfalls.append((cat, got_cat, want_cat))

    return shortfalls


def log_all_market_source_sampling(
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
    question_set_target: QuestionSetTarget,
) -> None:
    """Log market-value/time-horizon sampling results across all market sources combined."""
    separator_len = 65
    logger.info("")
    logger.info(f"{'=' * separator_len}")
    logger.info(
        f"ALL MARKET SOURCES ({question_set_target.name}) "
        f"{len(df_sampled)}/{n_target} sampled from {len(df_available):,} available"
    )
    logger.info(f"{'=' * separator_len}")

    df_sampled_bins = add_bin_columns(df_sampled.copy())
    df_available_bins = add_bin_columns(df_available.copy())

    _log_bin_dimension(
        "Market Value",
        MARKET_VALUE_CONFIG,
        "market_value",
        df_sampled_bins["market_value_bin"],
        df_available_bins["market_value_bin"],
        n_target,
        prefix="MV",
    )
    _log_bin_dimension(
        "Time Horizon",
        TIME_HORIZON_CONFIG,
        "time_horizon",
        df_sampled_bins["time_horizon_bin"],
        df_available_bins["time_horizon_bin"],
        n_target,
        prefix="TH",
    )


def log_sampling_summary(
    source_summaries: list[tuple[str, int, int, list]],
    question_set_target: QuestionSetTarget,
) -> None:
    """Log a compact summary table of all sources.

    Args:
        source_summaries (list[tuple]): (source, got, want, shortfalls) per source
        question_set_target (QuestionSetTarget): Question set target
    """
    header = f"SAMPLING SUMMARY ({question_set_target.name})"

    max_name = max((len(s[0]) for s in source_summaries), default=10)
    max_name = max(max_name, len("Source"))
    separator_len = max_name + 55

    logger.info("")
    logger.info(f"{'=' * separator_len}")
    logger.info(header)
    logger.info(f"{'=' * separator_len}")
    logger.info(f"  {'Source':<{max_name}}   Got/Want  Biggest shortfall")
    logger.info(f"  {'-' * (separator_len - 2)}")

    total_got = 0
    total_want = 0
    for source, got, want, shortfalls in source_summaries:
        total_got += got
        total_want += want
        if shortfalls:
            # Find the biggest shortfall by absolute gap (want - got)
            biggest = max(shortfalls, key=lambda s: s[2] - s[1])
            shortfall_str = f"* {biggest[0]}: {biggest[1]}/{biggest[2]}"
        else:
            shortfall_str = ""
        logger.info(f"  {source:<{max_name}}  {got:>4}/{want:<4} {shortfall_str}")

    logger.info(f"  {'-' * (separator_len - 2)}")
    logger.info(f"  {'Total':<{max_name}}  {total_got:>4}/{total_want:<4}")


def plot_sampling_distribution(
    df_sampled: pd.DataFrame,
    df_available: pd.DataFrame,
    n_target: int,
    source_name: str | None = None,
) -> None:
    """Plot the realized vs expected distribution for market question sampling.

    Creates line charts comparing Available, Selected, and Target distributions
    for both market value bins and time horizon bins.

    Args:
        df_sampled (pd.DataFrame): Sampled questions (must include bin columns)
        df_available (pd.DataFrame): All available questions (must include bin columns)
        n_target (int): Number of questions requested
        source_name (str | None): Name for the source (used in chart title)
    """
    if not env.RUNNING_LOCALLY:
        return

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Ensure bin columns exist
    required_cols = ["market_value_bin", "time_horizon_bin"]
    for col in required_cols:
        if col not in df_sampled.columns or col not in df_available.columns:
            logger.warning(f"Column '{col}' missing. Skipping plot.")
            return

    title = "Sampling Distribution"
    if source_name:
        title = f"Sampling Distribution: {source_name}"

    fig = make_subplots(
        rows=3,
        cols=1,
        subplot_titles=("Market Value", "Time Horizon", "Category"),
        vertical_spacing=0.1,
    )

    def get_distribution_data(
        bin_configs: list[dict],
        bin_type: str,
        col_name: str,
    ) -> tuple[list[str], list[int], list[int], list[float]]:
        """Extract distribution data for a dimension."""
        bin_labels = [get_bin_label(config, bin_type) for config in bin_configs]
        weights = {get_bin_label(c, bin_type): c["weight"] for c in bin_configs}

        # Check if unknown is present
        has_unknown = (
            "unknown" in df_sampled[col_name].values or "unknown" in df_available[col_name].values
        )
        if has_unknown:
            bin_labels.append("unknown")
            weights["unknown"] = 0

        # Count occurrences in each bin
        available_counts = df_available[col_name].value_counts()
        selected_counts = df_sampled[col_name].value_counts()

        available_data = [int(available_counts.get(lbl, 0)) for lbl in bin_labels]
        selected_data = [int(selected_counts.get(lbl, 0)) for lbl in bin_labels]
        target_data = [n_target * weights.get(lbl, 0) for lbl in bin_labels]

        return bin_labels, available_data, selected_data, target_data

    def add_line_chart(
        bin_labels: list[str],
        available_data: list[int],
        selected_data: list[int],
        target_data: list[float],
        row: int,
        show_legend: bool,
    ) -> None:
        """Add line chart with spline interpolation."""
        x_numeric = list(range(len(bin_labels)))

        fig.add_trace(
            go.Scatter(
                name="Available",
                x=x_numeric,
                y=available_data,
                mode="lines+markers",
                line=dict(color="gray", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="available",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="Selected",
                x=x_numeric,
                y=selected_data,
                mode="lines+markers",
                line=dict(color="steelblue", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="selected",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                name="Target",
                x=x_numeric,
                y=target_data,
                mode="lines+markers",
                line=dict(color="coral", shape="spline", smoothing=1.0),
                marker=dict(size=6),
                legendgroup="target",
                showlegend=show_legend,
            ),
            row=row,
            col=1,
        )

        # Update x-axis to show bin labels
        fig.update_xaxes(
            tickmode="array",
            tickvals=x_numeric,
            ticktext=bin_labels,
            tickangle=45,
            row=row,
            col=1,
        )

    # Market value distribution
    mv_labels, mv_avail, mv_sel, mv_target = get_distribution_data(
        MARKET_VALUE_CONFIG, "market_value", "market_value_bin"
    )
    add_line_chart(mv_labels, mv_avail, mv_sel, mv_target, row=1, show_legend=True)

    # Time horizon distribution
    th_labels, th_avail, th_sel, th_target = get_distribution_data(
        TIME_HORIZON_CONFIG, "time_horizon", "time_horizon_bin"
    )
    add_line_chart(th_labels, th_avail, th_sel, th_target, row=2, show_legend=False)

    # Category distribution (bar chart)
    categories = sorted(df_available["category"].unique())
    avail_counts = df_available["category"].value_counts()
    sampled_counts = df_sampled["category"].value_counts()
    cat_avail = [int(avail_counts.get(cat, 0)) for cat in categories]
    cat_sampled = [int(sampled_counts.get(cat, 0)) for cat in categories]

    fig.add_trace(
        go.Bar(
            name="Available",
            x=categories,
            y=cat_avail,
            marker=dict(color="gray", opacity=0.5),
            legendgroup="available",
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            name="Selected",
            x=categories,
            y=cat_sampled,
            marker=dict(color="steelblue"),
            legendgroup="selected",
            showlegend=False,
        ),
        row=3,
        col=1,
    )
    fig.update_xaxes(tickangle=45, row=3, col=1)

    fig.update_layout(
        title_text=title,
        height=1000,
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig.show()


def stratified_sample_questions(dfq: pd.DataFrame, n_target: int) -> pd.DataFrame:
    """Sample questions using stratified sampling to achieve target distribution.

    This ensures we get the desired distribution regardless of source data skew.

    Args:
        dfq (pd.DataFrame): DataFrame with bin_weight column and composite bins
        n_target (int): Number of questions to sample

    Returns
        result (pd.DataFrame): Sampled questions
    """
    if len(dfq) == 0 or n_target == 0:
        return pd.DataFrame()

    dfq_weighted = dfq[dfq["bin_weight"] > 0].copy()

    if len(dfq_weighted) == 0:
        raise ValueError("No questions with nonzero bin weight available for sampling.")

    # Calculate how many samples we want from each composite bin
    bin_samples = {}
    for bin_name in dfq_weighted["composite_bin"].unique():
        bin_df = dfq_weighted[dfq_weighted["composite_bin"] == bin_name]
        bin_weight = bin_df["bin_weight"].iloc[0]
        # Target number of samples from this bin
        target_samples = round(n_target * bin_weight)
        # Can't sample more than available
        available = len(bin_df)
        bin_samples[bin_name] = min(target_samples, available)

    # Adjust for rounding errors - add/remove samples to match n_target
    bin_weight_for = {
        name: dfq_weighted[dfq_weighted["composite_bin"] == name]["bin_weight"].iloc[0]
        for name in bin_samples
    }
    total_samples = sum(bin_samples.values())
    if total_samples < n_target:
        # Add samples to highest-weighted bins first
        bins_by_weight_desc = sorted(bin_samples, key=lambda x: bin_weight_for[x], reverse=True)
        shortage = n_target - total_samples
        for bin_name in bins_by_weight_desc:
            bin_df = dfq_weighted[dfq_weighted["composite_bin"] == bin_name]
            available = len(bin_df)
            current = bin_samples[bin_name]
            if current < available:
                add = min(shortage, available - current)
                bin_samples[bin_name] += add
                shortage -= add
                if shortage == 0:
                    break
    elif total_samples > n_target:
        # Remove samples from lowest-weighted bins first
        bins_by_weight_asc = sorted(bin_samples, key=lambda x: bin_weight_for[x])
        excess = total_samples - n_target
        for bin_name in bins_by_weight_asc:
            if bin_samples[bin_name] > 0:
                remove = min(excess, bin_samples[bin_name])
                bin_samples[bin_name] -= remove
                excess -= remove
                if excess == 0:
                    break

    # Sample from each bin
    sampled_dfs = []
    for bin_name, n_samples in bin_samples.items():
        if n_samples > 0:
            bin_df = dfq_weighted[dfq_weighted["composite_bin"] == bin_name]
            sampled = bin_df.sample(n=n_samples, replace=False)
            sampled_dfs.append(sampled)

    if not sampled_dfs:
        raise ValueError("Stratified sampling produced no results.")
    return pd.concat(sampled_dfs, ignore_index=True)


def sample_market_questions(dfq: pd.DataFrame, n_target: int) -> pd.DataFrame:
    """Sample market questions using multi-dimensional binning strategy.

    Ensures balanced sampling across market probability values and time horizons.

    Args:
        dfq (pd.DataFrame): Market questions
        n_target (int): Number of questions to sample

    Returns
        df_result (pd.DataFrame): Sampled questions
    """
    if len(dfq) == 0:
        raise ValueError("No market questions available for sampling.")

    dfq = add_bin_columns(dfq=dfq)
    dfq = create_composite_bins(dfq=dfq)
    dfq = calculate_bin_weights(dfq=dfq)
    df_result = stratified_sample_questions(
        dfq=dfq,
        n_target=n_target,
    )
    df_result = df_result.drop(
        columns=[
            "market_value_bin",
            "time_horizon_bin",
            "composite_bin",
            "bin_weight",
        ]
    )
    return df_result


def llm_sample_questions(values: dict, n_single: int) -> pd.DataFrame:
    """Generate questions for the LLM question set.

    For market questions: Sample using binning strategy.
    For data questions: Sample evenly across categories.

    Args:
        values (dict): Source data dict containing "dfq" DataFrame
        n_single (int): Number of questions to sample

    Returns
        df (pd.DataFrame): Sampled questions
    """
    dfq = values["dfq"].copy()
    source = dfq["source"].iloc[0]

    if source in question_curation.MARKET_SOURCES:
        # Use binning-based sampling for market questions
        return sample_market_questions(dfq, n_single)
    else:
        # Use existing category-based sampling for data sources
        allocation = allocate_across_categories(num_questions=n_single, dfq=dfq)

        dfs = []
        for key, value in allocation.items():
            dfs.append(dfq[dfq["category"] == key].sample(value))
        return pd.concat(dfs, ignore_index=True)


def allocate_evenly(data: dict[str, int], n: int) -> dict:
    """Allocate n items evenly across keys in data, respecting availability.

    `data` maps keys to available counts (e.g., {'source1': 30, 'source2': 50}).
    Returns allocation dict with same keys where each value <= the original.
    If sum(data.values()) <= n, returns data unchanged.

    Args:
        data (dict[str,int]): Keys to allocate across, values are available counts
        n (int): Total number of items to allocate

    Returns
        allocation (dict): Keys mapped to allocated counts
    """

    def validate_allocation(num_allocated: int, n: int) -> None:
        if num_allocated != n:
            raise ValueError(f"Failed to allocate evenly: allocated {num_allocated:,}/{n}")
        logger.info(f"Successfully allocated {num_allocated:,}/{n}.")

    sum_n_items = sum(data.values())
    if sum_n_items <= n:
        validate_allocation(sum_n_items, n)
        return data

    # initial allocation
    allocation = {key: min(n // len(data), value) for key, value in data.items()}
    allocated_num = sum(allocation.values())

    while allocated_num < n:
        remaining = n - allocated_num
        under_allocated = {
            key: value - allocation[key] for key, value in data.items() if allocation[key] < value
        }

        if not under_allocated:
            # Break if nothing more to allocate
            break

        # Amount to add in this iteration
        to_allocate = max(remaining // len(under_allocated), 1)
        for key in under_allocated:
            if under_allocated[key] > 0:
                add_amount = min(to_allocate, under_allocated[key], remaining)
                allocation[key] += add_amount
                remaining -= add_amount
                if remaining <= 0:
                    break
        allocated_num = sum(allocation.values())

    num_allocated = sum(allocation.values())
    validate_allocation(num_allocated, n)
    return allocation


def allocate_across_categories(num_questions: int, dfq: pd.DataFrame) -> dict:
    """Allocate questions evenly among categories.

    Args:
        num_questions (int): Total number of questions to allocate
        dfq (pd.DataFrame): DataFrame with a "category" column

    Returns
        allocation (dict): Category to count mapping
    """
    categories = dfq["category"].unique()
    data = {category: sum(dfq["category"] == category) for category in categories}
    return allocate_evenly(data=data, n=num_questions)


def allocate_across_sources(questions: dict, num_questions: int) -> dict:
    """Allocate questions evenly among sources.

    Args:
        questions (dict): Source data keyed by source name
        num_questions (int): Total number of questions to allocate

    Returns
        sources (dict): Deep copy with num_questions_to_sample added per source
    """
    sources = deepcopy(questions)
    data = {key: source["num_questions_available"] for key, source in sources.items()}

    allocation = allocate_evenly(data=data, n=num_questions)

    for source in sources:
        sources[source]["num_questions_to_sample"] = allocation[source]

    num_allocated = sum(allocation.values())
    if num_allocated != num_questions:
        raise ValueError(
            f"Failed to allocate across sources: allocated {num_allocated:,}/{num_questions:,}"
        )

    logger.info(f"Allocated {num_allocated:,}/{num_questions:,}.")
    return sources


def write_questions(questions: dict, question_set_target: QuestionSetTarget) -> None:
    """Write questions to JSON file and upload to GCS.

    Args:
        questions (dict): Source data keyed by source name, each with "dfq" DataFrame
        question_set_target (QuestionSetTarget): Question set target ("llm" or "human")
    """

    def forecast_horizons_to_resolution_dates(forecast_horizons: list | str) -> list | str:
        return (
            [
                (question_curation.FORECAST_DATETIME + timedelta(days=day)).date().isoformat()
                for day in forecast_horizons
            ]
            if forecast_horizons != "N/A"
            else forecast_horizons
        )

    dfs = []
    for _, values in tqdm(questions.items(), "Writing questions"):
        df_source = values["dfq"]
        # Order columns consistently for writing
        df_source = deepcopy(
            df_source[
                [
                    "id",
                    "source",
                    "question",
                    "resolution_criteria",
                    "background",
                    "market_info_open_datetime",
                    "market_info_close_datetime",
                    "market_info_resolution_criteria",
                    "url",
                    "freeze_datetime",
                    "freeze_datetime_value",
                    "freeze_datetime_value_explanation",
                    "source_intro",
                    "forecast_horizons",
                ]
            ]
        )
        df_source["resolution_dates"] = df_source["forecast_horizons"].apply(
            forecast_horizons_to_resolution_dates
        )
        df_source = df_source.drop(columns="forecast_horizons")
        dfs.append(df_source)

    df = pd.concat(dfs, ignore_index=True)

    forecast_date_str = question_curation.FORECAST_DATE.isoformat()
    filename = f"{forecast_date_str}-{question_set_target.value}.json"
    latest_filename = f"latest-{question_set_target.value}.json"
    local_filename = f"/tmp/{filename}"

    json_data = {
        "forecast_due_date": forecast_date_str,
        "question_set": filename,
        "questions": df.to_dict(orient="records"),
    }

    with open(local_filename, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    if not env.RUNNING_LOCALLY:
        gcp.storage.upload(
            bucket_name=env.QUESTION_SETS_BUCKET,
            local_filename=local_filename,
            filename=filename,
        )

        gcp.storage.upload(
            bucket_name=env.QUESTION_SETS_BUCKET,
            local_filename=local_filename,
            filename=latest_filename,
        )


def drop_invalid_questions(dfq: pd.DataFrame, dfmeta: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid questions from dfq.

    Args:
        dfq (pd.DataFrame): Questions to filter
        dfmeta (pd.DataFrame): Metadata with valid_question column

    Returns
        dfq (pd.DataFrame): Only questions marked valid in metadata
    """
    if dfmeta.empty:
        return dfq
    dfq = pd.merge(
        dfq,
        dfmeta,
        how="inner",
        on=["id", "source"],
    )
    return dfq[dfq["valid_question"]].drop(columns="valid_question")


def drop_missing_freeze_datetime(dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop questions with missing values in the freeze_datetime_value column.

    Args:
        dfq (pd.DataFrame): Questions to filter

    Returns
        dfq (pd.DataFrame): Questions with valid freeze_datetime_value
    """
    col = "freeze_datetime_value"
    dfq = dfq.dropna(subset=col, ignore_index=True)
    dfq = dfq[dfq[col] != "N/A"]
    dfq = dfq[dfq[col] != "nan"]
    return dfq


def market_resolves_before_forecast_due_date(dt: datetime) -> bool:
    """Determine whether the market resolves before the forecast due date.

    Args:
        dt (datetime): Market close time

    Returns
        resolves_too_soon (bool): True if market closes before forecasts are due
    """
    llm_forecast_release_datetime = question_curation.FREEZE_DATETIME + timedelta(
        days=question_curation.FREEZE_WINDOW_IN_DAYS
    )
    all_forecasts_due = llm_forecast_release_datetime.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    ndays = dt - all_forecasts_due
    ndays = ndays.days + (1 if ndays.total_seconds() > 0 else 0)
    return ndays <= 0


def drop_questions_that_resolve_too_soon(source: str, dfq: pd.DataFrame) -> pd.DataFrame:
    """Drop questions that resolve too soon.

    Given the freeze date:
    * for market questions determine whether or not the market will close before at least the first
      forecasting horizon. If it does, then do not use this question.
    * for data questions if forecast_horizons is empty, don't use the question

    Args:
        source (str): Source name
        dfq (pd.DataFrame): Questions to filter

    Returns
        dfq (pd.DataFrame): Questions that don't resolve too soon
    """
    if source in question_curation.DATA_SOURCES:
        empty_horizons = dfq["forecast_horizons"].apply(lambda x: len(x) == 0)
        is_na = dfq["forecast_horizons"] == "N/A"
        return dfq[~(empty_horizons | is_na)]

    resolves_too_soon = dfq["market_info_close_datetime"].apply(
        lambda x: market_resolves_before_forecast_due_date(datetime.fromisoformat(x))
    )
    return dfq[~resolves_too_soon]


@decorator.log_runtime
def driver(_: None) -> None:
    """Create question set."""
    if not env.RUNNING_LOCALLY and not question_curation.is_today_question_curation_date():
        logger.info("Today is NOT the question set creation date.")
        return

    validate_bin_weights()

    dfmeta = data_utils.download_and_read(
        filename=constants.META_DATA_FILENAME,
        local_filename=f"/tmp/{constants.META_DATA_FILENAME}",
        df_tmp=pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
        dtype=constants.META_DATA_FILE_COLUMN_DTYPE,
    )

    QUESTIONS = deepcopy(question_curation.FREEZE_QUESTION_SOURCES)
    sources_to_remove = []
    for source in QUESTIONS:
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        if dfq.empty:
            sources_to_remove.append(source)
            logger.warning(f"Found 0 questions from {source}.")
        else:
            dfq["source"] = source
            dfq = drop_invalid_questions(dfq=dfq, dfmeta=dfmeta)
            dfq = drop_missing_freeze_datetime(dfq)
            dfq = dfq[dfq["category"] != "Other"]
            dfq = dfq[~dfq["resolved"]]
            dfq = drop_questions_that_resolve_too_soon(source=source, dfq=dfq)
            dfq["source_intro"] = QUESTIONS[source]["source_intro"]
            dfq["resolution_criteria"] = dfq["url"].apply(
                lambda url, template=QUESTIONS[source]["resolution_criteria"]: template.format(
                    url=url
                )
            )
            dfq["freeze_datetime"] = question_curation.FREEZE_DATETIME.isoformat()
            dfq = dfq.drop(columns=["market_info_resolution_datetime", "resolved"])

            num_questions = len(dfq)
            QUESTIONS[source]["dfq"] = dfq.reset_index(drop=True)
            QUESTIONS[source]["num_questions_available"] = num_questions
            logger.info(f"Found {num_questions:,} single questions from {source}.")

    QUESTIONS = {key: value for key, value in QUESTIONS.items() if key not in sources_to_remove}

    # Find allocations of questions
    LLM_QUESTIONS, HUMAN_QUESTIONS = {}, {}
    for question_type in [question_curation.MARKET_SOURCES, question_curation.DATA_SOURCES]:
        questions_of_question_type = {k: v for k, v in QUESTIONS.items() if k in question_type}
        llm_questions_of_question_type = allocate_across_sources(
            questions=questions_of_question_type,
            num_questions=question_curation.FREEZE_NUM_LLM_QUESTIONS // 2,
        )
        LLM_QUESTIONS.update(llm_questions_of_question_type)
        human_questions_of_question_type = allocate_across_sources(
            questions=llm_questions_of_question_type,
            num_questions=question_curation.FREEZE_NUM_HUMAN_QUESTIONS // 2,
        )
        HUMAN_QUESTIONS.update(human_questions_of_question_type)

    # Sample questions
    logger.info("LLM SET")
    LLM_QUESTIONS = process_questions(
        questions=QUESTIONS,
        to_questions=LLM_QUESTIONS,
        single_generation_func=llm_sample_questions,
        show_plots=env.RUNNING_LOCALLY,
        question_set_target=QuestionSetTarget.LLM,
    )

    logger.info("HUMAN SET")
    HUMAN_QUESTIONS = process_questions(
        questions=LLM_QUESTIONS,
        to_questions=HUMAN_QUESTIONS,
        single_generation_func=human_sample_questions,
        show_plots=False,
        question_set_target=QuestionSetTarget.HUMAN,
    )

    write_questions(LLM_QUESTIONS, question_set_target=QuestionSetTarget.LLM)
    write_questions(HUMAN_QUESTIONS, question_set_target=QuestionSetTarget.HUMAN)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
