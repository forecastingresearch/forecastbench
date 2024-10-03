"""Create leaderboard."""

import itertools
import json
import logging
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats
from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import (  # noqa: E402
    constants,
    dates,
    decorator,
    env,
    question_curation,
    resolution,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LEADERBOARD_UPDATED_DATE_STR = "Updated " + datetime.now().strftime("%b. %d, %Y")
BASELINE_ORG_MODEL = {"organization": constants.BENCHMARK_NAME, "model": "Naive Forecast"}

CONFIDENCE_LEVEL = 0.95
LEADERBOARD_DECIMAL_PLACES = 3


def download_and_read_forecast_file(filename):
    """Download forecast file."""
    local_filename = "/tmp/tmp.json"
    gcp.storage.download(
        bucket_name=env.PROCESSED_FORECAST_SETS_BUCKET,
        filename=filename,
        local_filename=local_filename,
    )
    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def upload_leaderboard(df, basename):
    """Upload leaderboard."""
    logger.info(f"Uploading leaderboard {basename}")
    local_filename = f"/tmp/{basename}.csv"
    df.to_csv(local_filename, index=False)
    gcp.storage.upload(
        bucket_name=env.PROCESSED_FORECAST_SETS_BUCKET,
        local_filename=local_filename,
    )


def get_leaderboard_entry(df):
    """Create the leaderboard entry for the given dataframe."""
    # Masks
    data_mask = df["source"].isin(question_curation.DATA_SOURCES)

    # Market sources should be reduced to the value at a single date. This is because we always
    # evaluate to the latest market value or the resolution value for a market and orgs only
    # forecast the outcome. Hence they get the same score at every period.
    single_resolution_date = sorted(df["resolution_date"].unique())[0]
    market_mask = df["source"].isin(question_curation.MARKET_SOURCES) & (
        df["resolution_date"] == single_resolution_date
    )

    resolved_mask = df["resolved"].astype(bool)
    unresolved_mask = ~resolved_mask

    def get_scores(df, mask):
        scores = df[mask]["score"]
        return scores.mean(), len(scores)

    # Datasets
    data_resolved_score, n_data_resolved = get_scores(df, data_mask & resolved_mask)
    data_resolved_std_dev = df[data_mask & resolved_mask]["score"].std(ddof=1)
    data_resolved_se = data_resolved_std_dev / np.sqrt(n_data_resolved)

    # Markets
    market_resolved_score, n_market_resolved = get_scores(df, market_mask & resolved_mask)
    market_unresolved_score, n_market_unresolved = get_scores(df, market_mask & unresolved_mask)
    market_overall_score, n_market_overall = get_scores(df, market_mask)
    market_overall_std_dev = df[market_mask]["score"].std(ddof=1)
    market_overall_se = market_overall_std_dev / np.sqrt(n_market_overall)

    # Overall Resolved
    overall_resolved_score = (data_resolved_score + market_resolved_score) / 2
    n_overall_resolved = n_data_resolved + n_market_resolved

    # Overall
    overall_score = (data_resolved_score + market_overall_score) / 2
    n_overall = n_data_resolved + n_market_overall
    overall_se = np.sqrt(data_resolved_se**2 + market_overall_se**2) / 2

    # Overall CI
    conservative_dof = min(n_data_resolved, n_market_overall) - 1
    confidence_interval = np.round(
        stats.t.interval(
            confidence=CONFIDENCE_LEVEL, df=conservative_dof, loc=overall_score, scale=overall_se
        ),
        LEADERBOARD_DECIMAL_PLACES,
    )

    # % imputed
    pct_imputed = int(df["imputed"].sum() / len(df) * 100)

    return {
        "data": data_resolved_score,
        "n_data": n_data_resolved,
        "market_resolved": market_resolved_score,
        "n_market_resolved": n_market_resolved,
        "market_unresolved": market_unresolved_score,
        "n_market_unresolved": n_market_unresolved,
        "market_overall": market_overall_score,
        "n_market_overall": n_market_overall,
        "overall_resolved": overall_resolved_score,
        "n_overall_resolved": n_overall_resolved,
        "overall": overall_score,
        "confidence_interval_overall": confidence_interval,
        "n_overall": n_overall,
        "pct_imputed": pct_imputed,
        "df": df.copy(),
    }


def get_permutation_p_value(
    df_data,
    n_best_data,
    n_comparison_data,
    df_market,
    n_best_market,
    n_comparison_market,
    observed_difference,
    n_replications,
):
    """Get the p-value comparing comparison to best using the Permutation Test."""
    permutation_differences = []
    for _ in range(n_replications):
        permuted_data_scores = np.random.permutation(df_data["score"])
        comparison_data_sample = permuted_data_scores[:n_comparison_data]
        best_data_sample = permuted_data_scores[n_comparison_data:]

        permuted_market_scores = np.random.permutation(df_market["score"])
        comparison_market_sample = permuted_market_scores[:n_comparison_market]
        best_market_sample = permuted_market_scores[n_comparison_market:]

        comparison_overall_mean = (
            np.mean(comparison_data_sample) + np.mean(comparison_market_sample)
        ) / 2
        best_overall_mean = (np.mean(best_data_sample) + np.mean(best_market_sample)) / 2
        permutation_differences.append(comparison_overall_mean - best_overall_mean)

    permutation_differences = np.array(permutation_differences)
    return np.round(
        np.mean(permutation_differences > observed_difference), LEADERBOARD_DECIMAL_PLACES
    )


def get_bootstrap_p_value(
    df_data,
    n_best_data,
    n_comparison_data,
    df_market,
    n_best_market,
    n_comparison_market,
    observed_difference,
    n_replications,
):
    """Get the p-value comparing comparison to best by Bootstrapping."""
    bootstrap_differences = []
    for _ in range(n_replications):
        df_best_data_resample = df_data["score"].sample(
            n=n_best_data, replace=True, ignore_index=True
        )
        df_best_market_resample = df_market["score"].sample(
            n=n_best_market, replace=True, ignore_index=True
        )

        df_comparison_data_resample = df_data["score"].sample(
            n=n_comparison_data, replace=True, ignore_index=True
        )
        df_comparison_market_resample = df_market["score"].sample(
            n=n_comparison_market, replace=True, ignore_index=True
        )

        comparison_overall_mean = (
            df_comparison_data_resample.mean() + df_comparison_market_resample.mean()
        ) / 2
        best_overall_mean = (df_best_data_resample.mean() + df_best_market_resample.mean()) / 2
        bootstrap_differences.append(comparison_overall_mean - best_overall_mean)

    bootstrap_differences = np.array(bootstrap_differences)
    return np.round(
        np.mean(bootstrap_differences > observed_difference),
        LEADERBOARD_DECIMAL_PLACES,
    )


def get_p_values(d):
    """Get p values comparing comparison to best to see if they're significantly different."""
    n_replications = 10000
    df = pd.DataFrame(d)
    df = df.sort_values(by=["overall"], ignore_index=True)

    # Only get pairwise p-values for now, skip treating questions as indpendent.
    # Keeping the code because it may come in handy when we run a new round with a different
    # question set.
    df = get_pairwise_p_values(df, n_replications)
    df.drop(columns="df", inplace=True)
    return df

    p_val_bootstrap_col_name = "p-value_bootstrap"
    p_val_permutation_col_name = "p-value_permutation"
    df[p_val_bootstrap_col_name] = None
    df[p_val_permutation_col_name] = None

    # Get best performer
    best_organization = df.at[0, "organization"]
    best_model = df.at[0, "model"]
    logger.info(f"p-value comparison best performer is: {best_organization} {best_model}.")

    df_best = pd.DataFrame(df.at[0, "df"])
    observed_overall_score_best = df.at[0, "overall"]

    df_best_data = df_best[
        (df_best["source"].isin(resolution.DATA_SOURCES)) & df_best["resolved"].astype(bool)
    ]
    df_best_market = df_best[df_best["source"].isin(resolution.MARKET_SOURCES)]

    n_best_data = len(df_best_data)
    n_best_market = len(df_best_market)

    for index in range(1, len(df)):
        df_comparison = pd.DataFrame(df.at[index, "df"])
        observed_overall_score_comparison = df.at[index, "overall"]

        df_comparison_data = df_comparison[
            (df_comparison["source"].isin(resolution.DATA_SOURCES))
            & df_comparison["resolved"].astype(bool)
        ]
        df_comparison_market = df_comparison[
            df_comparison["source"].isin(resolution.MARKET_SOURCES)
        ]

        n_comparison_data = len(df_comparison_data)
        n_comparison_market = len(df_comparison_market)

        df_data = pd.concat([df_best_data, df_comparison_data], ignore_index=True)
        df_market = pd.concat([df_best_market, df_comparison_market], ignore_index=True)

        observed_difference = observed_overall_score_comparison - observed_overall_score_best

        df.at[index, p_val_bootstrap_col_name] = get_bootstrap_p_value(
            df_data=df_data,
            n_best_data=n_best_data,
            n_comparison_data=n_comparison_data,
            df_market=df_market,
            n_best_market=n_best_market,
            n_comparison_market=n_comparison_market,
            observed_difference=observed_difference,
            n_replications=n_replications,
        )
        df.at[index, p_val_permutation_col_name] = get_permutation_p_value(
            df_data=df_data,
            n_best_data=n_best_data,
            n_comparison_data=n_comparison_data,
            df_market=df_market,
            n_best_market=n_best_market,
            n_comparison_market=n_comparison_market,
            observed_difference=observed_difference,
            n_replications=n_replications,
        )

    df.drop(columns="df", inplace=True)
    return df


def get_pairwise_p_values(df, n_replications):
    """Calculate p-values on Brier differences on individual questions.

    From Ezra: this improves precision because, for any two groups, forecasting accuracy is very
    correlated on the set of questions. Treating them as independent overstates
    imprecision. Instead, we can bootstrap the questions by focusing on the difference in scores on
    a question-by-question basis.

    A nice example of this is that if group A is always epsilon more accurate than group B on every
    question, we can be quite confident A is a better forecaster, even if epsilon is arbitrarily
    small and even if the standard deviation of accuracy for group A's forecasts is high.
    """
    p_val_bootstrap_col_name = "p-value_pairwise_bootstrap"
    df[p_val_bootstrap_col_name] = None
    better_than_super_col_name = "pct_better_than_no1"
    df[better_than_super_col_name] = 0.0

    # Get best performer
    best_organization = df.at[0, "organization"]
    best_model = df.at[0, "model"]
    logger.info(f"p-value comparison best performer is: {best_organization} {best_model}.")

    df_best = pd.DataFrame(df.at[0, "df"])
    observed_overall_score_best = df.at[0, "overall"]

    for index in range(1, len(df)):
        df_comparison = pd.DataFrame(df.at[index, "df"])
        observed_overall_score_comparison = df.at[index, "overall"]

        # first merge on the questions to then get the diff between the scores
        df_merged = pd.merge(
            df_best.copy(),
            df_comparison,
            on=[
                "id",
                "source",
                "direction",
                "forecast_due_date",
                "resolved",
                "resolution_date",
            ],
            how="inner",
            suffixes=[
                "_best",
                "_comparison",
            ],
        )
        df_merged = df_merged[["id", "source", "resolved", "score_comparison", "score_best"]]

        assert len(df_best) == len(df_comparison) and len(df_best) == len(df_merged), (
            "Problem with merge in `get_pairwise_p_values()`. Comparing org: "
            f"{df.at[index, 'organization']}, model: {df.at[index, 'model']} "
            f"n_best: {len(df_best)}, n_comparison: {len(df_comparison)}, "
            f"n_merged: {len(df_merged)}"
        )

        df_merged_data = df_merged[
            (df_merged["source"].isin(resolution.DATA_SOURCES)) & df_merged["resolved"].astype(bool)
        ].reset_index(drop=True)
        df_merged_market = df_merged[
            df_merged["source"].isin(resolution.MARKET_SOURCES)
        ].reset_index(drop=True)

        df_merged_data_diff = df_merged_data["score_comparison"] - df_merged_data["score_best"]
        df_merged_market_diff = (
            df_merged_market["score_comparison"] - df_merged_market["score_best"]
        )

        observed_difference = observed_overall_score_comparison - observed_overall_score_best
        assert (
            abs(
                observed_difference
                - ((df_merged_data_diff.mean() + df_merged_market_diff.mean()) / 2)
            )
            < 1e-15
        ), "Observed difference in scores is incorrect in `get_pairwise_p_values()`."

        # Shift mean of scores to 0 for the null hypothesis that comparison and best scores are
        # identical and hence their difference is 0
        df_merged_data_diff -= df_merged_data_diff.mean()
        df_merged_market_diff -= df_merged_market_diff.mean()

        assert (
            (df_merged_data_diff.mean() + df_merged_market_diff.mean()) / 2
        ) < 1e-15, "Observed difference in scores is incorrect in `get_pairwise_p_values()`."

        n_data = len(df_merged_data_diff)
        n_market = len(df_merged_market_diff)

        # Bootstrap p-value
        overall_diff = []
        for _ in range(n_replications):
            df_data_diff_bootstrapped = df_merged_data_diff.sample(
                n=n_data, replace=True, ignore_index=True
            )
            df_market_diff_bootstrapped = df_merged_market_diff.sample(
                n=n_market, replace=True, ignore_index=True
            )
            overall_diff.append(
                (df_data_diff_bootstrapped.mean() + df_market_diff_bootstrapped.mean()) / 2
            )
        overall_diff = np.array(overall_diff)

        df.at[index, p_val_bootstrap_col_name] = np.round(
            np.mean(overall_diff >= observed_difference), LEADERBOARD_DECIMAL_PLACES
        )

        # Percent better than supers
        df_combo = pd.concat([df_merged_data, df_merged_market], ignore_index=True)
        df.at[index, better_than_super_col_name] = (
            np.round(
                np.mean(df_combo["score_comparison"] < df_combo["score_best"]),
                LEADERBOARD_DECIMAL_PLACES,
            )
            * 100
        )

    return df


def add_to_leaderboard(leaderboard, org_and_model, df, forecast_due_date):
    """Add scores to the leaderboard."""
    resolution_dates = df["resolution_date"].unique()
    forecast_due_date_date = dates.convert_iso_str_to_date(forecast_due_date)
    for resolution_date in resolution_dates:
        resolution_date_key = (resolution_date - forecast_due_date_date).days
        leaderboard_entry = [
            org_and_model | get_leaderboard_entry(df[df["resolution_date"] == resolution_date])
        ]
        leaderboard[resolution_date_key] = (
            leaderboard.get(resolution_date_key, []) + leaderboard_entry
        )

    leaderboard_entry = [org_and_model | get_leaderboard_entry(df)]
    leaderboard["overall"] = leaderboard.get("overall", []) + leaderboard_entry


def add_to_llm_leaderboard(*args, **kwargs):
    """Wrap `add_to_leaderboard` for easy reading of driver."""
    add_to_leaderboard(*args, **kwargs)


def download_question_set_save_in_cache(forecast_due_date, cache):
    """Time-saving function to only download files once per run.

    Save question files in cache.
    """
    if forecast_due_date not in cache:
        cache[forecast_due_date] = {}

    for human_or_llm in ["human", "llm"]:
        if human_or_llm not in cache[forecast_due_date]:
            cache[forecast_due_date][human_or_llm] = resolution.download_and_read_question_set_file(
                filename=f"{forecast_due_date}-{human_or_llm}.json"
            )


def add_to_llm_and_human_leaderboard(leaderboard, org_and_model, df, forecast_due_date, cache):
    """Parse the forecasts to include only those questions that were in the human question set."""
    download_question_set_save_in_cache(forecast_due_date, cache)
    df_human_question_set = cache[forecast_due_date]["human"].copy()
    df_only_human_question_set = pd.merge(
        df,
        df_human_question_set[["id", "source"]],
        on=["id", "source"],
    ).reset_index(drop=True)
    add_to_leaderboard(
        leaderboard=leaderboard,
        org_and_model=org_and_model,
        df=df_only_human_question_set,
        forecast_due_date=forecast_due_date,
    )


def add_to_llm_and_human_combo_leaderboards(
    leaderboard_combo, leaderboard_combo_generated, org_and_model, df, forecast_due_date, cache
):
    """Parse the forecasts to include only those questions that were in the human question set."""
    download_question_set_save_in_cache(forecast_due_date, cache)
    df_human_question_set = cache[forecast_due_date]["human"].copy()
    df_llm_question_set = cache[forecast_due_date]["llm"].copy()
    if "combos" not in cache[forecast_due_date]:
        human_possible_combos = []
        for _, row in df_llm_question_set[
            df_llm_question_set["id"].apply(resolution.is_combo)
        ].iterrows():
            id0, id1 = row["id"]
            source = row["source"]
            df_source = df_human_question_set[df_human_question_set["source"] == source]
            if {id0, id1}.issubset(df_source["id"]):
                human_possible_combos.append({"source": source, "id": row["id"]})
            cache[forecast_due_date]["combos"] = human_possible_combos
    human_combos = cache[forecast_due_date]["combos"].copy()

    df_only_human_question_set = pd.merge(
        df,
        df_human_question_set[["id", "source"]],
        on=["id", "source"],
    ).reset_index(drop=True)

    # Is this a human forecast set or an llm set (human sets don't have any combo forecasts)
    df_from_llm = df["id"].apply(resolution.is_combo).any()

    # Add pertinent combos from llm forecast file to llm df
    if df_from_llm:
        df_llm_combos = df[
            df.apply(
                lambda row: (row["id"], row["source"])
                in [(combo["id"], combo["source"]) for combo in human_combos],
                axis=1,
            )
        ]
        df_only_human_question_set = pd.concat(
            [df_only_human_question_set, df_llm_combos], ignore_index=True
        )

    def generate_combo_forecasts(df, forecast_due_date):
        """Generate combo forecasts."""
        # Remove combos in df, if any
        df = df[~df["id"].apply(resolution.is_combo)]

        # Generate combos from the df
        for combo in human_combos:
            source = combo["source"]
            id0, id1 = combo["id"]
            df_source = df[df["source"] == source]
            df_forecast0 = df_source[df_source["id"] == id0]
            df_forecast1 = df_source[df_source["id"] == id1]
            if df_forecast0.empty or df_forecast1.empty:
                # If either forecast set is empty, it means one of the questions was dropped as N/A
                # and hence is not in the processed forecast file.
                continue
            resolution_dates = set(df_forecast0["resolution_date"]).intersection(
                set(df_forecast1["resolution_date"])
            )
            for resolution_date in resolution_dates:
                df_forecast0_tmp = df_forecast0[df_forecast0["resolution_date"] == resolution_date]
                df_forecast1_tmp = df_forecast1[df_forecast1["resolution_date"] == resolution_date]
                if len(df_forecast0_tmp) != 1 or len(df_forecast1_tmp) != 1:
                    raise ValueError("`generate_combo_forecasts`: should not arrive here.")

                for dir0, dir1 in list(itertools.product([1, -1], repeat=2)):
                    forecast = resolution.combo_change_sign(
                        df_forecast0_tmp["forecast"].iloc[0], dir0
                    ) * resolution.combo_change_sign(df_forecast1["forecast"].iloc[0], dir1)
                    resolved_to = resolution.combo_change_sign(
                        df_forecast0_tmp["resolved_to"].iloc[0], dir0
                    ) * resolution.combo_change_sign(df_forecast1["resolved_to"].iloc[0], dir1)
                    resolved = (
                        df_forecast0_tmp["resolved"].iloc[0]
                        and df_forecast1_tmp["resolved"].iloc[0]
                    )
                    imputed = (
                        df_forecast0_tmp["imputed"].iloc[0] or df_forecast1_tmp["imputed"].iloc[0]
                    )
                    score = (forecast - resolved_to) ** 2
                    new_row = {
                        "id": (id0, id1),
                        "source": source,
                        "direction": (dir0, dir1),
                        "forecast_due_date": df_forecast0_tmp["forecast_due_date"].iloc[0],
                        "market_value_on_due_date": np.nan,
                        "resolution_date": resolution_date,
                        "resolved_to": resolved_to,
                        "resolved": resolved,
                        "forecast": forecast,
                        "imputed": imputed,
                        "score": score,
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        return df

    if not df_from_llm:
        # This is a human forecast set. Hence combos need to be generated for both leaderboard_
        # combo and leaderboard_combo_generated.
        df_only_human_question_set = generate_combo_forecasts(
            df_only_human_question_set, forecast_due_date
        )

    leaderboard_combo = add_to_leaderboard(
        leaderboard=leaderboard_combo,
        org_and_model=org_and_model,
        df=df_only_human_question_set,
        forecast_due_date=forecast_due_date,
    )

    if df_from_llm:
        # This is an LLM set, so only generate combos for leaderboard_combo_generated.
        # This means the LLMs combo forecasts were used for leaderboard_combo.
        df_only_human_question_set = generate_combo_forecasts(
            df_only_human_question_set, forecast_due_date
        )

    leaderboard_combo_generated = add_to_leaderboard(
        leaderboard=leaderboard_combo_generated,
        org_and_model=org_and_model,
        df=df_only_human_question_set,
        forecast_due_date=forecast_due_date,
    )


def make_html_table(df, title, basename):
    """Make and upload HTLM leaderboard."""
    # Replace NaN with empty strings for display
    logger.info(f"Making HTML leaderboard file: {title} {basename}.")
    df = df.fillna("")

    # Add ranking
    df = df.sort_values(by=["overall"], ignore_index=True)
    df.insert(loc=0, column="Ranking", value="")
    df["score_diff"] = df["overall"] - df["overall"].shift(1)
    for index, row in df.iterrows():
        if row["score_diff"] != 0:
            prev_rank = index + 1
        df.loc[index, "Ranking"] = prev_rank
    df.drop(columns="score_diff", inplace=True)

    # Round columns to 3 decimal places
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].round(3)

    # Rename columns
    n_data = df["n_data"].max()
    n_market_resolved = df["n_market_resolved"].max()
    n_market_unresolved = df["n_market_unresolved"].max()
    n_market_overall = df["n_market_overall"].max()
    n_overall = df["n_overall"].max()
    n_overall_resolved = df["n_overall_resolved"].max()

    df["pct_imputed"] = df["pct_imputed"].round(0).astype(int).astype(str) + "%"
    df["pct_better_than_no1"] = df["pct_better_than_no1"].round(0).astype(int).astype(str) + "%"

    # For small p-values, only show <0.001
    df["p-value_pairwise_bootstrap"] = df["p-value_pairwise_bootstrap"].apply(
        lambda x: ("<0.001" if isinstance(x, (float, int)) and x < 0.001 else x)
    )

    df = df[
        [
            "Ranking",
            "organization",
            "model",
            "data",
            "market_resolved",
            "market_unresolved",
            "market_overall",
            "overall_resolved",
            "overall",
            "confidence_interval_overall",
            "p-value_pairwise_bootstrap",
            "pct_better_than_no1",
            "pct_imputed",
        ]
    ]
    df = df.rename(
        columns={
            "organization": "Organization",
            "model": "Model",
            "data": f"Dataset Score (N={n_data:,})",
            "market_resolved": f"Market Score (resolved) (N={n_market_resolved:,})",
            "market_unresolved": f"Market Score (unresolved) (N={n_market_unresolved:,})",
            "market_overall": f"Market Score (overall) (N={n_market_overall:,})",
            "overall_resolved": f"Overall Resolved Score (N={n_overall_resolved:,})",
            "overall": f"Overall Score (N={n_overall:,})",
            "confidence_interval_overall": "Overall Score 95% CI",
            "p-value_pairwise_bootstrap": "Pairwise p-value comparing to No. 1 (bootstrapped)",
            "pct_better_than_no1": "Pct. more accurate than No. 1",
            "pct_imputed": "Pct. Imputed",
            # "std_dev": Std. Dev.", # DELETE
            # "z_score_wrt_naive_mean": "Z-score",
        }
    )

    column_descriptions = """
        <div style="display: flex; align-items: center;">
          <a data-bs-toggle="collapse" data-bs-target="#descriptionCollapse" aria-expanded="false"
             aria-controls="descriptionCollapse" style="text-decoration: none; color: inherit;
             display: flex; align-items: center; cursor: pointer;">
            <i class="bi bi-chevron-right rotate" id="toggleArrow" style="margin-left: 5px;"></i>
            <span>Column descriptions</span>
          </a>
        </div>
        <div class="collapse mt-3" id="descriptionCollapse" style="padding: 0px;">
          <div class="card card-body">
            <ul>
              <li><b>Ranking</b>: The position of the model in the leaderboard as ordered by
                                  Overall Score</li>
              <li><b>Organization</b>: The group responsible for the model or forecasts</li>
              <li><b>Model</b>: The LLM model & prompt info or the human group and forecast
                                aggregation method
                  <ul>
                    <li>zero shot: used a zero-shot prompt</li>
                    <li>scratchpad: used a scratchpad prompt with instructions that outline a
                                    procedure the model should use to reason about the question</li>
                    <li>with freeze values: means that, for questions from market sources, the prompt
                                            was supplemented with the aggregate human forecast from
                                            the relevant platform on the day the question set was
                                            generated</li>
                    <li>with news: means that the prompt was supplemented with relevant news
                                   summaries obtained through an automated process</li>
                  </ul>
              <li><b>Dataset Score</b>: The average Brier score across all questions sourced from
                                        datasets</li>
              <li><b>Market Score (resolved)</b>: The average Brier score across all resolved
                                                  questions sourced from prediction markets and
                                                  forecast aggregation platforms</li>
              <li><b>Market Score (unresolved)</b>: The average Brier score across all unresolved
                                                    questions sourced from prediction markets and
                                                    forecast aggregation platforms</li>
              <li><b>Market Score (overall)</b>: The average Brier score across all questions
                                                 sourced from prediction markets and forecast
                                                 aggregation platforms</li>
              <li><b>Overall Resolved Score</b>: The average of the Dataset Score and the Market
                                                 Score (resolved) columns</li>
              <li><b>Overall Score</b>: The average of the Dataset Score and the Market Score
                                        (overall) columns</li>
              <li><b>Overall Score 95% CI</b>: The 95% confidence interval for the Overall
                                               Score</li>
              <li><b>Pairwise p-value comparing to No. 1 (bootstrapped)</b>: The p-value calculated
                              by bootstrapping the differences in overall score between each model
                              and the best forecaster (the group with rank 1) under the null
                              hypothesis that there's no difference.</li>
              <li><b>Pct. more accurate than No. 1</b>: The percent of questions where this
                              forecaster had a better overall score than the best forecaster (with
                              rank 1)</li>
              <li><b>Pct. imputed</b>: The percent of questions for which this forecaster did not
                              provide a forecast and hence had a forecast value imputed (0.5 for
                              dataset questions and the aggregate human forecast on the forecast
                              due date for questions sourced from prediction markets or forecast
                              aggregation platforms)</li>
            </ul>
          </div>
        </div>
        <script>
        var toggleArrow = document.getElementById('toggleArrow');
        var toggleLink = document.querySelector('[data-bs-toggle="collapse"]');

        toggleLink.addEventListener('click', function () {
          if (toggleArrow.classList.contains('rotate-down')) {
            toggleArrow.classList.remove('rotate-down');
          } else {
            toggleArrow.classList.add('rotate-down');
          }
        });
        </script>
    """

    # Remove lengths from df
    df = df[[c for c in df.columns if not c.startswith("n_")]]

    html_code = df.to_html(
        classes="table table-striped table-bordered", index=False, table_id="myTable"
    )
    html_code = (
        """<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <title>LLM Data Table</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
              integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
              crossorigin="anonymous">
        <link rel="stylesheet" type="text/css"
              href="https://cdn.datatables.net/2.1.6/css/dataTables.jqueryui.min.css">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons/font/bootstrap-icons.css" rel="stylesheet">
        <script src="https://code.jquery.com/jquery-3.7.1.js"></script>
        <script type="text/javascript" charset="utf8"
                src="https://cdn.datatables.net/2.1.6/js/dataTables.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
        <style>
            body {
              font-size: 10px;
            }
            table.dataTable {
              font-size: 10px;
            }
            .dataTables_wrapper .dataTables_length,
            .dataTables_wrapper .dataTables_filter,
            .dataTables_wrapper .dataTables_info,
            .dataTables_wrapper .dataTables_paginate {
                font-size: 10px;
            }
            .dataTables_length {
              display: none;
            }
            .dataTables_paginate {
              display: none;
            }
            .dataTables_info {
              display: none;
            }
            .dataTables_wrapper {
              margin-bottom: 20px; /* Add bottom margin */
            }
            .highlight {
              background-color: #eeebe0 !important;
            }
            h1 {
              text-align: center;
              margin-top: 10px;
              font-family: 'Arial', sans-serif;
              font-size: 16px;
           }
           .rotate {
             transition: transform 0.3s ease;
           }
           .rotate-down {
             transform: rotate(90deg);
           }
           .right-align {
             text-align: right;
           }
          .updated-date {
               font-size: 10px;
               text-align: center;
               color: #6c757d; /* Bootstrap muted text color */
               margin-top: -10px;
           }
        </style>
    </head>
    <body>
        <div class="container mt-4">
    """
        + "<h1>"
        + title
        + "</h1>"
        + '<p class="updated-date">'
        + LEADERBOARD_UPDATED_DATE_STR
        + "</p>"
        + column_descriptions
        + html_code
        + """
        </div>
        <script>
        $(document).ready(function() {
            var table = $('#myTable').DataTable({
                "pageLength": -1,
                "lengthMenu": [[-1], ["All"]],
                "order": [[8, 'asc']],
                "paging": false,
                "info": false,
                "search": {
                    "regex": true,
                    "smart": true
                },
                "columnDefs": [
                    {
                        "targets": 10,
                        "className": "right-align"
                    },
                    {
                        "targets": '_all',
                        "searchable": true
                    }
                ]
            });
        table.column(8).nodes().to$().addClass('highlight');
        });
        </script>
    </body>
</html>"""
    )

    local_filename = f"/tmp/{basename}.html"
    with open(local_filename, "w") as file:
        file.write(html_code)
    gcp.storage.upload(
        bucket_name=env.LEADERBOARD_BUCKET,
        local_filename=local_filename,
    )


@decorator.log_runtime
def driver(_):
    """Create new leaderboard."""
    cache = {}
    llm_leaderboard = {}
    llm_and_human_leaderboard = {}
    llm_and_human_combo_leaderboard = {}
    llm_and_human_combo_all_generated_leaderboard = {}
    files = gcp.storage.list(env.PROCESSED_FORECAST_SETS_BUCKET)
    files = [file for file in files if file.endswith(".json")]
    for f in files:
        logger.info(f"Downloading, reading, and scoring forecasts in `{f}`...")

        data = download_and_read_forecast_file(filename=f)
        if not data or not isinstance(data, dict):
            logger.warning(f"Problem processing {f}. First `continue`.")
            continue

        organization = data.get("organization")
        model = data.get("model")
        question_set_filename = data.get("question_set")
        forecast_due_date = data.get("forecast_due_date")
        forecasts = data.get("forecasts")
        if (
            not organization
            or not model
            or not question_set_filename
            or not forecast_due_date
            or not forecasts
        ):
            logger.warning(f"Problem processing {f}. Second `continue`.")
            continue

        df = pd.DataFrame(forecasts)
        if df.empty:
            logger.warning(f"Problem processing {f}. Third `continue`.")
            continue

        sanity_check = df["score"] - ((df["forecast"] - df["resolved_to"]) ** 2)
        if sanity_check.sum() > 1e-5:
            raise ValueError(
                f"Sanity Check failed. Should be close to 0. Instead value is {sanity_check.sum()}."
            )

        df = resolution.make_columns_hashable(df)
        df["resolution_date"] = pd.to_datetime(df["resolution_date"]).dt.date
        df["forecast_due_date"] = pd.to_datetime(df["forecast_due_date"]).dt.date

        is_human_question_set = "human" in question_set_filename
        org_and_model = {"organization": organization, "model": model}
        if not is_human_question_set:
            add_to_llm_leaderboard(llm_leaderboard, org_and_model, df, forecast_due_date)
        add_to_llm_and_human_leaderboard(
            llm_and_human_leaderboard,
            org_and_model,
            df,
            forecast_due_date,
            cache,
        )

        add_to_llm_and_human_combo_leaderboards(
            llm_and_human_combo_leaderboard,
            llm_and_human_combo_all_generated_leaderboard,
            org_and_model,
            df,
            forecast_due_date,
            cache,
        )

    def get_z_score(df):
        # mask = (df["organization"] == BASELINE_ORG_MODEL["organization"]) & (
        #     df["model"] == BASELINE_ORG_MODEL["model"]
        # )
        # naive_baseline_mean = df[mask]["overall"].values[0]
        # naive_std_dev = df[mask]["std_dev"].values[0]
        # df["z_score_wrt_naive_mean"] = (df["overall"] - naive_baseline_mean) / naive_std_dev
        return df

    def is_numeric(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def make_leaderboard(d, title, basename):
        logger.info(colored(f"Making leaderboard: {title}", "red"))
        df = get_p_values(d)
        upload_leaderboard(df=df, basename=basename)
        make_html_table(
            df=df,
            title=title,
            basename=basename,
        )
        logger.info(colored("Done.", "red"))

    for key in llm_leaderboard:
        title = "Leaderboard: " + (f"{key} day" if is_numeric(key) else "overall")
        make_leaderboard(d=llm_leaderboard[key], title=title, basename=f"leaderboard_{key}")

        if key in llm_and_human_leaderboard:
            make_leaderboard(
                d=llm_and_human_leaderboard[key],
                title=f"Human {title}",
                basename=f"human_leaderboard_{key}",
            )
            make_leaderboard(
                d=llm_and_human_combo_leaderboard[key],
                title=f"Human Combo {title}",
                basename=f"human_combo_leaderboard_{key}",
            )
            make_leaderboard(
                d=llm_and_human_combo_all_generated_leaderboard[key],
                title=f"Human Combo Generated {title}",
                basename=f"human_combo_generated_leaderboard_{key}",
            )

    return "OK", 200


if __name__ == "__main__":
    driver(None)
