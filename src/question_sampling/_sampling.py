"""Draw the LLM and human question sets from the eligible questions."""

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from fractions import Fraction

import numpy as np
import pandas as pd

from _fb_types import QuestionSetTarget
from helpers import constants

from ._allocation import allocate_across_categories
from ._binning import (
    MARKET_VALUE_CONFIG,
    TIME_HORIZON_CONFIG,
    add_bin_columns,
    calculate_bin_weights,
    create_composite_bins,
)
from ._prepare import is_market_source
from ._report import (
    log_all_market_source_sampling,
    log_sampling_summary,
    log_source_sampling,
    plot_sampling_distribution,
)

# Probability that a market question of each category is kept in the pool before binning. Every
# category in `constants.QUESTION_CATEGORIES` must be listed, with a weight in (0, 1]. Thinning
# happens before the bins are built, so a bin that would hold only Sports questions comes out
# nearly empty and its quota moves to other bins instead of being filled with Sports.
CATEGORY_SAMPLING_WEIGHTS = {
    "Science & Tech": 1.0,
    "Healthcare & Biology": 1.0,
    "Economics & Business": 1.0,
    "Environment & Energy": 1.0,
    "Politics & Governance": 1.0,
    "Arts & Recreation": 1.0,
    "Security & Defense": 1.0,
    "Sports": 0.03,
    "Other": 1.0,
}


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
    if set(CATEGORY_SAMPLING_WEIGHTS) != set(constants.QUESTION_CATEGORIES):
        raise ValueError("CATEGORY_SAMPLING_WEIGHTS must list every category exactly once")
    if any(not 0 < weight <= 1 for weight in CATEGORY_SAMPLING_WEIGHTS.values()):
        raise ValueError("CATEGORY_SAMPLING_WEIGHTS must all be probabilities in (0, 1]")


def process_questions(
    questions: dict,
    to_questions: dict,
    single_generation_func: Callable,
    show_plots: bool,
    question_set_target: QuestionSetTarget,
    forecast_datetime: datetime,
    random_state: int | np.random.RandomState | None = None,
) -> dict:
    """Sample from `questions` to get the number of questions needed.

    Args:
        questions (dict): Source questions keyed by source name, each with a "dfq" DataFrame
        to_questions (dict): Allocation info keyed by source with "num_questions_to_sample"
        single_generation_func (Callable): Sampling function taking (values, n, random_state) and
            returning a DataFrame
        show_plots (bool): Whether to display distribution plots
        question_set_target (QuestionSetTarget): Target question set ("llm" or "human")
        forecast_datetime (datetime): The forecast due date, used to bin market questions by time
            horizon in the sampling logs and plots
        random_state: Seed/``np.random.RandomState`` threaded to ``single_generation_func`` for
            reproducibility. ``None`` (default) samples without a fixed seed.

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
        values["dfq"] = single_generation_func(values, num_single, random_state)
        df_sampled = values["dfq"]
        num_found += len(df_sampled)

        # Log per-source compact table (skip for human set which is random sampling)
        if question_set_target != QuestionSetTarget.HUMAN:
            shortfalls = log_source_sampling(
                source, df_sampled, df_available, num_single, forecast_datetime
            )
        else:
            shortfalls = []
        source_summaries.append((source, len(df_sampled), num_single, shortfalls))

        if is_market_source(source):
            # Keep aggregate market-source data for combined summary/logging.
            market_available.append(df_available)
            market_sampled.append(df_sampled.copy())

        if (
            show_plots
            and is_market_source(source)
            and question_set_target != QuestionSetTarget.HUMAN
        ):
            # Plot per-source distribution
            available_with_bins = add_bin_columns(df_available, forecast_datetime)
            sampled_with_bins = add_bin_columns(df_sampled.copy(), forecast_datetime)
            plot_sampling_distribution(
                sampled_with_bins,
                available_with_bins,
                num_single,
                source_name=source,
            )

    assert len(market_sampled) > 0, "Should not arrive here."

    total_market_requested = sum(
        to_questions[s]["num_questions_to_sample"] for s in to_questions if is_market_source(s)
    )

    if show_plots:
        # Plot overall market distribution
        df_all_sampled = add_bin_columns(
            pd.concat(market_sampled, ignore_index=True), forecast_datetime
        )
        df_all_available = add_bin_columns(
            pd.concat(market_available, ignore_index=True), forecast_datetime
        )
        plot_sampling_distribution(
            df_all_sampled,
            df_all_available,
            total_market_requested,
            source_name=f"ALL SOURCES ({question_set_target.value.upper()} SET)",
            source_counts=[(source, got, want) for source, got, want, _ in source_summaries],
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
        forecast_datetime=forecast_datetime,
    )

    return processed_questions


def human_sample_questions(
    values: dict, n_single: int, random_state: int | np.random.RandomState | None = None
) -> pd.DataFrame:
    """Get questions for the human question set by sampling from LLM questions.

    Args:
        values (dict): Source data dict containing "dfq" DataFrame
        n_single (int): Number of questions to sample
        random_state: Seed/``np.random.RandomState`` for reproducible sampling (anything
            ``DataFrame.sample`` accepts). ``None`` (default) samples without a fixed seed.

    Returns
        dfq (pd.DataFrame): Randomly sampled questions
    """
    dfq = values["dfq"].copy()
    n = min(n_single, len(dfq))
    return dfq.sample(n=n, random_state=random_state)


def _as_random_state(
    random_state: int | np.random.RandomState | None,
) -> np.random.RandomState | None:
    """Normalize a seed/RandomState/None to a RandomState (or None).

    Returns ``None`` unchanged (preserving unseeded behavior) and a ``RandomState`` unchanged, but
    promotes a bare int seed to a ``RandomState`` so successive ``.sample`` calls in a loop
    *decorrelate* (advancing one shared generator) instead of reusing the same seed per iteration.
    """
    if random_state is None or isinstance(random_state, np.random.RandomState):
        return random_state
    return np.random.RandomState(random_state)


def thin_pool_by_category(
    dfq: pd.DataFrame, random_state: int | np.random.RandomState | None = None
) -> pd.DataFrame:
    """Keep each question with the probability `CATEGORY_SAMPLING_WEIGHTS` gives its category.

    Args:
        dfq (pd.DataFrame): Market questions with a category column
        random_state: Seed/``np.random.RandomState`` for a reproducible draw. ``None`` (default)
            draws without a fixed seed.

    Returns
        dfq (pd.DataFrame): The rows that survived the draw, in their original order
    """
    if "category" not in dfq.columns:
        return dfq
    rng = _as_random_state(random_state) or np.random
    keep_probability = dfq["category"].map(CATEGORY_SAMPLING_WEIGHTS).to_numpy()
    return dfq[rng.random_sample(len(dfq)) < keep_probability]


def stratified_sample_questions(
    dfq: pd.DataFrame, n_target: int, random_state: int | np.random.RandomState | None = None
) -> pd.DataFrame:
    """Sample questions using stratified sampling to achieve target distribution.

    This ensures we get the desired distribution regardless of source data skew.

    Args:
        dfq (pd.DataFrame): DataFrame with bin_weight column and composite bins
        n_target (int): Number of questions to sample
        random_state: Seed/``np.random.RandomState`` for reproducible per-bin sampling (anything
            ``DataFrame.sample`` accepts). ``None`` (default) samples without a fixed seed. Thread a
            single ``RandomState`` instance through to decorrelate the successive per-bin draws.

    Returns
        result (pd.DataFrame): Sampled questions
    """
    random_state = _as_random_state(random_state)
    if len(dfq) == 0 or n_target == 0:
        return pd.DataFrame()

    dfq_weighted = dfq[dfq["bin_weight"] > 0].copy()

    if len(dfq_weighted) == 0:
        raise ValueError("No questions with nonzero bin weight available for sampling.")

    # Apportion the quota across composite bins by largest remainder: each bin gets the floor of
    # its fractional target, then the leftover slots go one at a time to the bins with the largest
    # fractional parts. Rounding each bin on its own gives nothing to a bin whose target is below
    # one half, and with about a hundred composite bins and n_target near 80 that is every
    # long-horizon bin.
    bin_weight_for = {}
    available_for = {}
    for bin_name in dfq_weighted["composite_bin"].unique():
        bin_df = dfq_weighted[dfq_weighted["composite_bin"] == bin_name]
        bin_weight_for[bin_name] = bin_df["bin_weight"].iloc[0]
        available_for[bin_name] = len(bin_df)
    fractional_target = {name: n_target * weight for name, weight in bin_weight_for.items()}
    bin_samples = {
        name: min(int(target), available_for[name]) for name, target in fractional_target.items()
    }
    bins_by_remainder_desc = sorted(
        bin_samples,
        key=lambda name: (fractional_target[name] % 1, bin_weight_for[name]),
        reverse=True,
    )
    for bin_name in bins_by_remainder_desc:
        if sum(bin_samples.values()) == n_target:
            break
        if bin_samples[bin_name] < available_for[bin_name]:
            bin_samples[bin_name] += 1

    # Bins that ran out of questions leave a shortage; fill it from the highest-weighted bins first
    shortage = n_target - sum(bin_samples.values())
    if shortage > 0:
        bins_by_weight_desc = sorted(bin_samples, key=lambda x: bin_weight_for[x], reverse=True)
        for bin_name in bins_by_weight_desc:
            add = min(shortage, available_for[bin_name] - bin_samples[bin_name])
            bin_samples[bin_name] += add
            shortage -= add
            if shortage == 0:
                break

    # Sample from each bin
    sampled_dfs = []
    for bin_name, n_samples in bin_samples.items():
        if n_samples > 0:
            bin_df = dfq_weighted[dfq_weighted["composite_bin"] == bin_name]
            sampled = bin_df.sample(n=n_samples, replace=False, random_state=random_state)
            sampled_dfs.append(sampled)

    if not sampled_dfs:
        raise ValueError("Stratified sampling produced no results.")
    return pd.concat(sampled_dfs, ignore_index=True)


def sample_market_questions(
    dfq: pd.DataFrame,
    n_target: int,
    forecast_datetime: datetime,
    random_state: int | np.random.RandomState | None = None,
) -> pd.DataFrame:
    """Sample market questions using multi-dimensional binning strategy.

    Ensures balanced sampling across market probability values and time horizons.

    Args:
        dfq (pd.DataFrame): Market questions
        n_target (int): Number of questions to sample
        forecast_datetime (datetime): The forecast due date the time horizon is measured from
        random_state: Seed/``np.random.RandomState`` threaded to ``stratified_sample_questions``
            for reproducibility. ``None`` (default) samples without a fixed seed.

    Returns
        df_result (pd.DataFrame): Sampled questions
    """
    if len(dfq) == 0:
        raise ValueError("No market questions available for sampling.")

    random_state = _as_random_state(random_state)
    dfq = thin_pool_by_category(dfq=dfq, random_state=random_state)
    dfq = add_bin_columns(dfq=dfq, forecast_datetime=forecast_datetime)
    dfq = create_composite_bins(dfq=dfq)
    dfq = calculate_bin_weights(dfq=dfq)
    df_result = stratified_sample_questions(
        dfq=dfq,
        n_target=n_target,
        random_state=random_state,
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


def llm_sample_questions(
    values: dict,
    n_single: int,
    random_state: int | np.random.RandomState | None = None,
    *,
    forecast_datetime: datetime,
) -> pd.DataFrame:
    """Generate questions for the LLM question set.

    For market questions: Sample using binning strategy.
    For data questions: Sample evenly across categories.

    Args:
        values (dict): Source data dict containing "dfq" DataFrame
        n_single (int): Number of questions to sample
        random_state: Seed/``np.random.RandomState`` threaded to the market/category samplers for
            reproducibility. ``None`` (default) samples without a fixed seed.
        forecast_datetime (datetime): The forecast due date the market time horizon is measured
            from

    Returns
        df (pd.DataFrame): Sampled questions
    """
    dfq = values["dfq"].copy()
    source = dfq["source"].iloc[0]
    random_state = _as_random_state(random_state)

    if is_market_source(source):
        # Use binning-based sampling for market questions
        return sample_market_questions(
            dfq, n_single, forecast_datetime=forecast_datetime, random_state=random_state
        )
    else:
        # Use existing category-based sampling for data sources
        allocation = allocate_across_categories(num_questions=n_single, dfq=dfq)

        dfs = []
        for key, value in allocation.items():
            dfs.append(dfq[dfq["category"] == key].sample(value, random_state=random_state))
        return pd.concat(dfs, ignore_index=True)
