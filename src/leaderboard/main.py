"""Create leaderboard."""

import json
import logging
import os
import pickle
import sys
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import pyfixest as pf
from jinja2 import Template
from scipy.stats import norm
from statsmodels.stats.multitest import multipletests
from termcolor import colored
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    git,
    keys,
    question_curation,
    resolution,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LEADERBOARD_UPDATED_DATE_STR = "Updated " + datetime.now().strftime("%b. %-d, %Y")

BASELINE_ORG_NAIVE_MODEL = {"organization": constants.BENCHMARK_NAME, "model": "Naive Forecaster"}

LEADERBOARD_DECIMAL_PLACES = 3

IMPUTED_CUTOFF_PCT = 5

MODEL_RELEASE_DATE_CUTOFF = 365

N_REPLICATES = 1999 if not env.RUNNING_LOCALLY else 2

df_release_dates = pd.read_csv("model_release_dates.csv")
df_release_dates["release_date"] = pd.to_datetime(df_release_dates["release_date"], errors="coerce")


def download_question_set_save_in_cache(
    forecast_due_date: str,
    cache: Dict[str, Dict[str, Any]],
) -> None:
    """Time-saving function to only download files once per run.

    This function checks the cache for the specified forecast_due_date and, for each question set
    type ('human' and 'llm'), downloads and stores the question set file if it has not already been
    loaded.

    Args:
        forecast_due_date (str): Identifier for the forecast due date (e.g., "2024-07-21"), used to
            construct question set filenames like "{forecast_due_date}-human.json" and
            "{forecast_due_date}-llm.json".
        cache (Dict[str, Dict[str, Any]]): Nested cache structure where the first-level
            keys are forecast_due_date strings and the second-level keys are source types
            ('human' or 'llm'), mapping to the loaded question set data.

    Returns:
        None: The cache is modified in-place to include any newly downloaded question sets.
    """
    if forecast_due_date not in cache:
        cache[forecast_due_date] = {}

    for human_or_llm in ["human", "llm"]:
        if human_or_llm not in cache[forecast_due_date]:
            cache[forecast_due_date][human_or_llm] = resolution.download_and_read_question_set_file(
                filename=f"{forecast_due_date}-{human_or_llm}.json"
            )


def get_masks(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Generate boolean masks for dataset and market filters, including resolution status.

    Args:
        df (pd.DataFrame): The forecast set.

    Returns:
        Dict[str, pd.Series]: Mapping of mask names to boolean Series:
            - "dataset":      questions from DATA_SOURCES that are resolved.
            - "market":       all questions from MARKET_SOURCES.
            - "market_resolved":   market questions that are resolved.
            - "market_unresolved": market questions that are unresolved.
    """
    data_mask = df["source"].isin(question_curation.DATA_SOURCES)
    market_mask = df["source"].isin(question_curation.MARKET_SOURCES)
    resolved_mask = df["resolved"].astype(bool)
    return {
        "dataset": data_mask & resolved_mask,
        "market": market_mask,
        "market_resolved": market_mask & resolved_mask,
        "market_unresolved": market_mask & ~resolved_mask,
    }


def get_df_info(
    df: pd.DataFrame,
    org_and_model: Dict[str, str],
    forecast_due_date: str,
) -> Optional[pd.DataFrame]:
    """Preprocess a forecast set DataFrame for a given model and due date.

    Args:
        df (pd.DataFrame): Forecast set.
        org_and_model (Dict[str, str]): The organization and model associated with the forecast set.
        forecast_due_date (str): Forecast due date in 'YYYY-MM-DD' format.

    Returns:
        Optional[pd.DataFrame]: The processed DataFrame or None if the imputed percentage exceeds
            the defined cutoff.
    """
    # Ignore if too many imputed resolved market questions
    if org_and_model.get("organization") != constants.BENCHMARK_NAME:
        if df["imputed"].mean() * 100 > IMPUTED_CUTOFF_PCT:
            return None

    df = resolution.make_columns_hashable(df)

    # Drop market unresolved questions
    masks = get_masks(df)
    df = df[masks["dataset"] | masks["market_resolved"]]

    # Remove combos
    df = df[~df["id"].apply(resolution.is_combo)]

    # Set formats of columns and add columns useful for processing
    df["resolution_date"] = pd.to_datetime(df["resolution_date"]).dt.date
    df["forecast_due_date"] = pd.to_datetime(forecast_due_date).date()
    df["horizon"] = (df["resolution_date"] - df["forecast_due_date"]).apply(
        lambda delta: delta.days
    )

    # Set primary key
    df["question_pk"] = ""
    masks = get_masks(df)
    df.loc[masks["dataset"], "question_pk"] = (
        df["forecast_due_date"].astype(str)
        + "_"
        + df["source"].astype(str)
        + "_"
        + df["id"].astype(str)
        + "_"
        + df["horizon"].astype(str)
    )

    df.loc[masks["market_resolved"], "question_pk"] = (
        df["forecast_due_date"].astype(str)
        + "_"
        + df["source"].astype(str)
        + "_"
        + df["id"].astype(str)
    )

    df["organization"] = org_and_model["organization"]
    df["model"] = org_and_model["model"]
    df["model_pk"] = df["organization"] + "_" + df["model"]

    return df.sort_values(by=["forecast_due_date", "source", "id"], ignore_index=True)


def append_leaderboard_entry(
    leaderboard_entries: List[pd.DataFrame],
    org_and_model: Dict[str, str],
    df: pd.DataFrame,
    forecast_due_date: str,
) -> None:
    """Append each model's processed forecast set to the leaderboard_entry list.

    Args:
        entries (List[pd.DataFrame]): List collecting processed forecast set DataFrames.
        org_and_model (Dict[str, str]): The organization and model associated with the forecast set.
        df (pd.DataFrame): Forecast set.
        forecast_due_date (str): Forecast due date in 'YYYY-MM-DD' format.

    Returns:
        None: Modifies `entries` in place. Logs a warning if processing is skipped.
    """
    processed = get_df_info(
        df=df,
        org_and_model=org_and_model,
        forecast_due_date=forecast_due_date,
    )
    if processed is None:
        logger.warning(
            f"Ignoring {org_and_model['organization']} {org_and_model['model']}—"
            "imputed cutoff exceeded."
        )
        return

    leaderboard_entries.append(processed)


def upload_leaderboard(files: Dict[str, str]) -> None:
    """Upload leaderboard files to Cloud Storage and push updates to Git.

    Args:
        files (Dict[str, str]): Mapping of local file paths to their basenames.

    Returns:
        None
    """
    if env.RUNNING_LOCALLY:
        # Don't upload anything when running locally
        return

    # Upload files to GCP bucket
    destination_folder = "leaderboards"
    for local_filename in files.keys():
        gcp.storage.upload(
            bucket_name=env.PUBLIC_RELEASE_BUCKET,
            local_filename=local_filename,
            destination_folder=destination_folder,
        )

    # Push to git
    git_files = {k: f"{destination_folder}/{v}" for k, v in files.items()}
    mirrors = keys.get_secret_that_may_not_exist("HUGGING_FACE_REPO_URL")
    mirrors = [mirrors] if mirrors else []
    git.clone_and_push_files(
        repo_url=keys.API_GITHUB_DATASET_REPO_URL,
        files=git_files,
        commit_message="leaderboard: automatic update html & csv files.",
        mirrors=mirrors,
    )


def write_leaderboard(
    df: pd.DataFrame,
    primary_scoring_func: Callable[..., any],
) -> Dict[str, str]:
    """Generate HTML and CSV leaderboard files and return their paths.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        primary_scoring_func (Callable): Function used to compute the primary overall score;
            its __name__ determines which columns to format and sort by.

    Returns:
        Dict[str, str]: A mapping of local file paths to their basenames:
            {
                "/tmp/<basename>.html": "<basename>.html",
                "/tmp/<basename>.csv":  "<basename>.csv"
            }.
    """
    logger.info("Making HTML and CSV leaderboard file.")

    # Replace NaN with empty strings for display
    df = df.fillna("")

    # Round columns to 3 decimal places
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].round(3)

    # Add rank
    df["Rank"] = df[f"{primary_scoring_func.__name__}_overall"].rank(
        ascending=True,
        method="min",
    )

    for col in [
        "n_market",
        "n_dataset",
        "n_overall",
        "Rank",
    ]:
        df[col] = df[col].astype(int)

    # Format CI
    df[f"{primary_scoring_func.__name__}_ci_lower"] = (
        df[f"{primary_scoring_func.__name__}_ci_lower"].round(3).astype(str)
    )
    df[f"{primary_scoring_func.__name__}_ci_upper"] = (
        df[f"{primary_scoring_func.__name__}_ci_upper"].round(3).astype(str)
    )
    df[f"{primary_scoring_func.__name__}_ci"] = (
        "["
        + df[f"{primary_scoring_func.__name__}_ci_lower"]
        + ", "
        + df[f"{primary_scoring_func.__name__}_ci_upper"]
        + "]"
    )

    df = df.sort_values(by=f"{primary_scoring_func.__name__}_overall", ignore_index=True)
    df["p_value_one_sided"] = df["p_value_one_sided"].apply(
        lambda p: (
            "<0.001" if p < 0.001 else "<0.01" if p < 0.01 else "<0.05" if p < 0.05 else f"{p:.2f}"
        )
    )
    # Set the p-value for the best to N/A
    df.loc[0, "p_value_one_sided"] = "—"

    df = df[
        [
            "Rank",
            "organization",
            "model",
            f"{primary_scoring_func.__name__}_dataset",
            "n_dataset",
            f"{primary_scoring_func.__name__}_market",
            "n_market",
            f"{primary_scoring_func.__name__}_overall",
            "n_overall",
            f"{primary_scoring_func.__name__}_ci",
            "p_value_one_sided",
            "pct_times_best_performer",
            "pct_times_top_5_percentile",
            "peer_score_overall",
            "brier_skill_score_overall",
        ]
    ].rename(
        columns={
            "organization": "Organization",
            "model": "Model",
            f"{primary_scoring_func.__name__}_dataset": "Dataset",
            "n_dataset": "N dataset",
            f"{primary_scoring_func.__name__}_market": "Market",
            "n_market": "N market",
            f"{primary_scoring_func.__name__}_overall": "Overall",
            "n_overall": "N",
            f"{primary_scoring_func.__name__}_ci": "95% CI",
            "p_value_one_sided": "P-value to Best",
            "pct_times_best_performer": "Pct times № 1",
            "pct_times_top_5_percentile": "Pct times top 5%",
            "peer_score_overall": "Peer",
            "brier_skill_score_overall": "BSS",
        }
    )

    template = Template(
        """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{{ title }}</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/fomantic-ui@2.9.3/dist/semantic.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/dataTables.semanticui.min.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/fomantic-ui@2.9.3/dist/components/icon.min.css">
  <link rel="stylesheet" href="https://cdn.datatables.net/responsive/2.4.1/css/responsive.semanticui.min.css">
  <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/fomantic-ui@2.9.3/dist/semantic.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.7/js/dataTables.semanticui.min.js"></script>
  <script src="https://cdn.datatables.net/responsive/2.4.1/js/dataTables.responsive.min.js"></script>
  <style>
    body {
        font-family: Arial,
        sans-serif;
        padding: 20px;
    }
    .n-count {
        color: #b9b9b9;
    }
    #dataTable td:nth-child({{ sorting_column_number }}) {
        background-color: #feffeb;
    }
    h1 {
        font-size: 1.5em;
        font-weight: 600;
        margin-bottom: 0.75em;
        text-align: center;
    }
    .dataTables_wrapper {
        width: 100%;
    }
    #dataTable {
        width: 100% !important;
    }
    #dataTable tbody tr:hover {
      background-color: #f1f1f1 !important;
      cursor: pointer;
    }
    .info.circle.icon {
        color: rgba(185, 185, 185, 0.7) !important;
    }
    .updated-date {
         font-size: 10px;
         text-align: center;
         margin-top: -10px;
    }
  </style>
  <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
  <h1>{{ title }}</h1>
  <p class="updated-date">{{ leaderboard_update_date }}</p>
  <table id="dataTable" class="ui celled table">
    <thead>
      <tr>
        <th>Rank</th>
        <th>Organization</th>
        <th>Model</th>
        <th>Dataset (N)</th>
        <th><!-- N dataset --></th>
        <th>Market (N)</th>
        <th><!-- N market --></th>
        <th>Overall (N)</th>
        <th><!-- N --></th>
        <th>95% CI</th>
        <th>P-value to Best</th>
        <th>Pct times № 1</th>
        <th>Pct times top 5%</th>
        <th>Peer</th>
        <th>BSS</th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
  <script>
      $(document).ready(function(){
        const rawData = {{ data }};
        const cols = {{ columns }};
        const columns = cols.map(name => {
          const col = { data: name, title: name };
          if (['N dataset','N market','N'].includes(name)) col.visible = false;
          if (name==='Dataset') {
            col.title = 'Dataset (N) <i class="info circle icon" '
                        + ' data-html="{{ col_desc_dataset }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3)+
                           ' <span class="n-count">('+
                           Number(row['N dataset']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='Market') {
            col.title = 'Market (N) <i class="info circle icon" '
                        + ' data-html="{{ col_desc_market }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3) +
                ' <span class="n-count">('+
                Number(row['N market']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='Overall') {
            col.title = 'Overall (N) <i class="info circle icon" '
                        + ' data-html="{{ col_desc_overall }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3) +
                ' <span class="n-count">('+
                Number(row['N']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='P-value to Best') {
            col.title = 'P-value to Best <i class="info circle icon" '
                        + ' data-html="{{ col_desc_p_val }}"></i>';
            col.orderable=false;
          }
          if (name==='Pct times № 1') {
            col.title = 'Pct times № 1 <i class="info circle icon" '
                        + ' data-html="{{ col_desc_pct_times_num_1 }}"></i>';
            col.render = function(d,t){ return t==='display'?Math.round(d)+'%':d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==='Pct times top 5%') {
            col.title = 'Pct times top 5% <i class="info circle icon" '
                        + ' data-html="{{ col_desc_pct_top_5_percentile }}"></i>';
            col.render = function(d,t){ return t==='display'?Math.round(d)+'%':d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==='95% CI') {
            col.title = '95% CI <i class="info circle icon" data-html="{{ col_desc_ci }}"></i>';
            col.orderable=false;
          }
          if (name==="Peer") {
            col.title = 'Peer <i class="info circle icon" data-html="{{ col_desc_peer }}"></i>';
            col.render = function(d,t){ return t==='display'?parseFloat(d).toFixed(3):d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==="BSS") {
            col.title = 'BSS <i class="info circle icon" data-html="{{ col_desc_bss }}"></i>';
            col.render = function(d,t){ return t==='display'?parseFloat(d).toFixed(3):d; };
            col.orderSequence = ['desc','asc'];
          }
          return col;
        });
        $('#dataTable').DataTable({
          data: rawData,
          columns: columns,
          order: [[ cols.indexOf('Overall'), 'asc' ]],
          responsive: true,
          paging: false,
          info: true,
          search: { regex:true, smart:true },
          initComplete: function() {
            $('.info.circle.icon').popup({ html: true} );
          }
        });
      });
  </script>
  </body>
</html>"""
    )

    html = template.render(
        title=f"{constants.BENCHMARK_NAME} Leaderboard",
        data=df.to_dict(orient="records"),
        columns=json.dumps(df.columns.tolist()),
        leaderboard_update_date=LEADERBOARD_UPDATED_DATE_STR,
        sorting_column_number=6,
        col_desc_dataset=(
            "Average difficulty-adjusted Brier score on dataset-sourced questions. "
            "Lower scores are better."
        ),
        col_desc_market=(
            "Average difficulty-adjusted Brier score on market-sourced questions. "
            "Lower scores are better."
        ),
        col_desc_overall=(
            "Average difficulty-adjusted Brier score across all questions. "
            "Lower scores are better."
        ),
        col_desc_ci="Bootstrapped 95% confidence interval for the Overall score.",
        col_desc_p_val=(
            "One-sided p-value comparing each model to the top-ranked model based on "
            f"{N_REPLICATES:,} simulations, with<br>"
            "H₀: This model performs at least as well as the top-ranked model.<br>"
            "H₁: The top-ranked model outperforms this model."
        ),
        col_desc_pct_times_num_1=(
            f"Percentage of {N_REPLICATES:,} simulations in which this model was the best "
            "performer."
        ),
        col_desc_pct_top_5_percentile=(
            f"Percentage of {N_REPLICATES:,} simulations in which this model ranked in the top 5%."
        ),
        col_desc_peer=(
            "Peer score relative to the average Brier score on each question. "
            "Higher scores are better."
        ),
        col_desc_bss=(
            "Brier Skill Score using the ForecastBench Naive Forecaster. "
            "Higher scores are better."
        ),
    )

    basename = "leaderboard"
    local_filename_html = f"/tmp/{basename}.html"
    with open(local_filename_html, "w", encoding="utf-8") as f:
        f.write(html)

    local_filename_csv = f"/tmp/{basename}.csv"
    df.to_csv(local_filename_csv, index=False)

    return {
        local_filename_html: f"{basename}.html",
        local_filename_csv: f"{basename}.csv",
    }


def combine_forecasting_rounds(leaderboard: List[pd.DataFrame]) -> pd.DataFrame:
    """Combine all processed forecast DataFrames into a single DataFrame.

    Args:
        leaderboard (List[pd.DataFrame]): List of DataFrames, each containing processed
            forecasts for one model and forecast due date.

    Returns:
        pd.DataFrame: Concatenated DataFrame of all forecasts.
    """
    forecasts = [entry.copy() for entry in leaderboard]
    df = pd.concat(forecasts).reset_index(drop=True)
    return df


def brier_score(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the Brier score for each forecast entry.

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: A new DataFrame with an added 'brier_score' column:
    """
    df = df.copy()
    df["brier_score"] = (df["forecast"] - df["resolved_to"]) ** 2
    return df


def two_way_fixed_effects(df: pd.DataFrame) -> pd.DataFrame:
    """Generate the difficulty adjusted Brier score.

    Calculate "question difficulty" by estimating a two-way fixed effect model:

      brier{i, j} = a_i + b_j + u_{i,j}

    where i = forecaster, and j = question. Question difficulty is estimated with b_j. In
    pyfixest, question_pk should be provided as the first FE variable, to ensure we have an
    estimate for each question_pk (otherwise one question may be dropped to avoid perfect
    multicolinearity).

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: A new DataFrame with an added 'two_way_fixed_effects' column.
    """
    df = df.copy()
    orig_cols = df.columns.tolist()

    # Drop models that were released more than `MODEL_RELEASE_DATE_CUTOFF` days ago. Also drop some
    # dummy ForecastBench models
    df_fe = pd.merge(
        df,
        df_release_dates,
        how="inner",
        on="model",
    )
    date_mask = (
        pd.to_datetime(df_fe["forecast_due_date"]) - pd.to_datetime(df_fe["release_date"])
    ).dt.days < MODEL_RELEASE_DATE_CUTOFF
    drop_benchmark_models = [
        "Always 0",
        "Always 1",
        "Always 0.5",
        "Random Uniform",
        # Drop Imputed Forecaster so as not to:
        # * double count the crowd forecast for market questions (the Naive Forecaster uses the
        #   value at t-1).
        # * consider its 0.5 forecast for dataset questions
        "Imputed Forecaster",
    ]
    benchmark_mask = (df_fe["organization"] == constants.BENCHMARK_NAME) & (
        ~df_fe["model"].isin(drop_benchmark_models)
    )
    df_fe = df_fe[(date_mask | benchmark_mask)].reset_index(drop=True)

    mod = pf.feols("brier_score ~ 1 | question_pk + model_pk", data=df_fe)
    dict_question_fe = mod.fixef()["C(question_pk)"]
    if len(dict_question_fe) != len(df["question_pk"].unique()):
        raise ValueError(
            f"Estimated num. of question fixed effects ({len(dict_question_fe)}) not equal to num. "
            f"of questions ({len(df['question_pk'].unique())})"
        )

    df["question_fe"] = df["question_pk"].map(dict_question_fe)
    df["two_way_fixed_effects"] = df["brier_score"] - df["question_fe"]
    return df[orig_cols + ["two_way_fixed_effects"]]


def peer_score(df: pd.DataFrame) -> pd.DataFrame:
    """Compute peer scores by comparing each forecast's Brier score to the question's average.

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: pd.DataFrame: A new DataFrame with an added 'peer_score' column.
    """
    df = df.copy()
    orig_cols = df.columns.tolist()

    # For each question, calculate average Brier score
    df["question_avg_brier"] = df.groupby("question_pk")["brier_score"].transform("mean")

    # Calculate peer score (positive is better than average)
    df["peer_score"] = df["question_avg_brier"] - df["brier_score"]
    return df[orig_cols + ["peer_score"]]


def brier_skill_score(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the Brier Skill Score in absolute terms.

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: pd.DataFrame: A new DataFrame with an added 'brier_skill_score' column.
    """
    df = df.copy()
    orig_cols = df.columns.tolist()

    # Get reference model's predictions
    mask_ref_model = (df["organization"] == BASELINE_ORG_NAIVE_MODEL["organization"]) & (
        df["model"] == BASELINE_ORG_NAIVE_MODEL["model"]
    )
    ref_data = df.loc[mask_ref_model,].copy()
    assert len(ref_data) > 0, "Reference model not found in data"

    # Create mapping of question_id to reference brier score
    ref_brier_by_question = ref_data.set_index("question_pk")["brier_score"].to_dict()

    # Ensure the naive forecaster has forecast on all questions across all question sets
    assert (
        df["question_pk"].isin(ref_brier_by_question).all()
    ), "Reference model must predict all questions across all question sets"

    # Calculate Brier skill score per question
    df["ref_brier"] = df["question_pk"].map(ref_brier_by_question)
    df["brier_skill_score"] = df["ref_brier"] - df["brier_score"]

    return df[orig_cols + ["brier_skill_score"]]


def score_models(
    df: pd.DataFrame, scoring_funcs: List[Callable[[pd.DataFrame], pd.DataFrame]]
) -> pd.DataFrame:
    """Score models using the scoring functions in `scoring_funcs`.

    Args:
        df (pd.DataFrame): Combined forecast set.
        scoring_funcs (List[Callable[[pd.DataFrame], pd.DataFrame]]): List of scoring functions.

    Returns:
        pd.DataFrame: Leaderboard DataFrame with:
            - For each scoring function: '{func_name}_dataset', '{func_name}_market', and
              '{func_name}_overall'
            - Count columns for dataset, market, and all questions
            - The 'organization', 'model', 'model_pk' associated with a forecast set
    """
    df = df.copy()

    if len(scoring_funcs) == 0:
        raise ValueError("Must provide at least one scoring function.")

    if not all([callable(f) for f in scoring_funcs]):
        raise ValueError("`scoring_funcs` must contain callable scoring functions.")

    # This can be any of the above, just choose the first as we require at least one scoring
    # function in the list.
    col_to_count_to_calculate_n = scoring_funcs[0].__name__

    results = []
    for question_type in ["dataset", "market"]:
        df_qt = df[get_masks(df)[question_type]].reset_index(drop=True)
        df_qt = brier_score(df_qt)
        for func in scoring_funcs:
            name = func.__name__
            df_qt = func(df_qt).rename(columns={name: f"{name}_{question_type}"})

        # Calculate the mean score for the question type for each model
        # Also count the N for the question type
        question_type_result = (
            df_qt.groupby(
                [
                    "organization",
                    "model",
                    "model_pk",
                ]
            )
            .agg(
                **{
                    f"{func.__name__}_{question_type}": (f"{func.__name__}_{question_type}", "mean")
                    for func in scoring_funcs
                },
                **{
                    f"n_{question_type}": (
                        f"{col_to_count_to_calculate_n}_{question_type}",
                        "count",
                    )
                },
            )
            .reset_index()
        )
        results.append(question_type_result)

    assert len(results) == 2, "Results should only have 2 entries."
    df_leaderboard = results[0].merge(
        results[1],
        on=[
            "organization",
            "model",
            "model_pk",
        ],
        how="outer",
    )

    for func in scoring_funcs:
        name = func.__name__
        df_leaderboard[f"{name}_overall"] = (
            df_leaderboard[f"{name}_dataset"] + df_leaderboard[f"{name}_market"]
        ) / 2

    df_leaderboard["n_overall"] = df_leaderboard["n_dataset"] + df_leaderboard["n_market"]
    return df_leaderboard


@decorator.log_runtime
def generate_simulated_leaderboards(
    df: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
    N: int = N_REPLICATES,
) -> pd.DataFrame:
    """Generate simulated leaderboards by bootstrap sampling.

    Args:
        df (pd.DataFrame): Combined forecast set to sample from.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]):
            Function to compute the primary overall score.
        N (int): Number of bootstrap replicates to generate.

    Returns:
        pd.DataFrame: Simulated scores with each column representing a replicate.
    """
    logger.info(colored(f"Generate {N} simulated leaderboards", "red"))
    df = df.copy()
    if not callable(primary_scoring_func):
        raise ValueError("The primary scoring function must be callable.")

    def question_level_bootstrap(df):
        questions = df["question_pk"].drop_duplicates()
        questions_bs = questions.sample(frac=1, replace=True)
        return df.set_index("question_pk").loc[questions_bs]

    scores_list = []
    for i in tqdm(range(N), desc="generating simulated leaderboards"):
        df_bs = (
            df.groupby(["forecast_due_date", "source"])
            .apply(question_level_bootstrap, include_groups=False)
            .reset_index()
        )
        df_simulated_leaderboard = score_models(df=df_bs, scoring_funcs=[primary_scoring_func])
        scores = df_simulated_leaderboard.set_index("model_pk")[
            f"{primary_scoring_func.__name__}_overall"
        ]
        scores_list.append(scores.rename(f"bootstrap_{i}"))

    df_simulated_scores = pd.concat(scores_list, axis=1)
    return df_simulated_scores


def get_confidence_interval(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
    method: str = "percentile",
    show_histograms: bool = False,
) -> pd.DataFrame:
    """Calculate confidence intervals for leaderboard scores.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores (pd.DataFrame): Bootstrapped replicates of overall scores.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]):
            Function to compute the primary overall score.
        method (str): CI calculation method, either 'percentile' or 'bca'.
        show_histograms (bool): Whether to display simulated score histograms.

    Returns:
        pd.DataFrame: Leaderboard with added lower and upper CI columns.
    """
    logger.info(colored("Calculating CIs", "red"))

    if env.RUNNING_LOCALLY and show_histograms:
        models = [
            # list model_pk's to plot
        ]
        if models:
            import plotly.graph_objects as go

            fig = go.Figure()
            for model in models:
                fig.add_trace(
                    go.Histogram(
                        x=df_simulated_scores.loc[model].values, name=str(model), opacity=0.7
                    )
                )
            fig.update_layout(
                barmode="overlay",
                title="Simulated Score Distributions",
                xaxis_title="Score",
                yaxis_title="Count",
            )
            fig.show()

    alpha = 0.05
    lower_alpha = alpha / 2
    upper_alpha = 1 - lower_alpha
    method = method.lower()
    if method == "bca":
        # BCa notation from page 190 of https://hastie.su.domains/CASI/index.html
        theta_hat = df_leaderboard[f"{primary_scoring_func.__name__}_overall"].values
        bs = df_simulated_scores.reindex(df_leaderboard["model_pk"]).values
        p0 = np.mean(bs < theta_hat[:, None], axis=1)
        z0 = norm.ppf(p0)
        z_alpha = norm.ppf([lower_alpha, upper_alpha])
        alphas = norm.cdf(2 * z0[:, None] + z_alpha)
        lower = pd.Series(
            [np.percentile(bs[i], alphas[i, 0] * 100) for i in range(bs.shape[0])],
            index=df_simulated_scores.index,
        )
        upper = pd.Series(
            [np.percentile(bs[i], alphas[i, 1] * 100) for i in range(bs.shape[0])],
            index=df_simulated_scores.index,
        )
    elif method == "percentile":
        lower = df_simulated_scores.quantile(lower_alpha, axis=1)
        upper = df_simulated_scores.quantile(upper_alpha, axis=1)
    else:
        raise ValueError(f"Value passed for method ({method}) is not valid.")

    df_leaderboard[f"{primary_scoring_func.__name__}_ci_lower"] = (
        df_leaderboard["model_pk"].map(lower).values
    )
    df_leaderboard[f"{primary_scoring_func.__name__}_ci_upper"] = (
        df_leaderboard["model_pk"].map(upper).values
    )
    return df_leaderboard


def get_comparison_to_best_model(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
    is_centered: bool = False,
    bh_adjust_p_vals: bool = False,
) -> pd.DataFrame:
    """Compute one-sided p-values comparing each model to the best performer.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores (pd.DataFrame): Bootstrapped replicates of overall scores.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]):
            Function to compute the primary overall score.
        is_centered (bool): Center p-value calculation on observed score differences.
        bh_adjust_p_vals (bool): Apply Benjamini-Hochberg adjustment if True.

    Returns:
        pd.DataFrame: Leaderboard with updated p_value_one_sided (and adjusted) columns.
    """
    logger.info(colored("Comparing to best model", "red"))
    if not callable(primary_scoring_func):
        raise ValueError("The primary scoring function must be callable.")

    overall_score_col = f"{primary_scoring_func.__name__}_overall"
    if overall_score_col not in df_leaderboard.columns:
        raise ValueError(f"Metric {overall_score_col} not found in leaderboard DataFrame.")

    if primary_scoring_func.__name__ != "two_way_fixed_effects":
        raise ValueError(
            "This function only works for the 2 way fixed effects model. For other models, ensure "
            "the best model is identified by `best_idx` (2wfe best model has the lowest score, "
            "other scoring functions may identify the best model as the one with the highest "
            "score.)"
        )

    observed_best_idx = df_leaderboard[overall_score_col].idxmin()
    observed_best_model_pk = df_leaderboard.loc[observed_best_idx, "model_pk"]
    observed_best_mean_score = df_leaderboard.loc[observed_best_idx, overall_score_col]

    sim_best_scores = df_simulated_scores.loc[observed_best_model_pk]

    if is_centered:
        observed_diffs = observed_best_mean_score - df_leaderboard[overall_score_col]
        observed_diff_dict = dict(zip(df_leaderboard["model_pk"], observed_diffs))
        p_value_one_sided = {
            model_pk: np.mean(
                ((sim_best_scores.values - sim_comp_scores.values) - observed_diff_dict[model_pk])
                <= observed_diff_dict[model_pk]
            )
            for model_pk, sim_comp_scores in df_simulated_scores.iterrows()
        }
    else:
        comparison_df = df_simulated_scores.le(sim_best_scores, axis=1)
        p_value_one_sided = comparison_df.mean(axis=1)

    df_leaderboard["p_value_one_sided"] = df_leaderboard["model_pk"].map(p_value_one_sided)
    df_leaderboard.loc[observed_best_idx, "p_value_one_sided"] = -1

    if bh_adjust_p_vals:
        # P-value adjustment for multiple tests to avoid the multiple comparisons problem.
        # Drop best row for p-value adjustment
        mask = df_leaderboard.index != observed_best_idx
        _, bh_adj_pvals, _, _ = multipletests(
            pvals=df_leaderboard.loc[mask, "p_value_one_sided"],
            alpha=0.05,
            method="fdr_bh",
        )
        df_leaderboard.loc[mask, "p_value_one_sided_bh_adj"] = bh_adj_pvals
        df_leaderboard.loc[observed_best_idx, "p_value_one_sided_bh_adj"] = -1

    return df_leaderboard


def get_simulation_performance_metrics(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate model metrics in the simulation data.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores (pd.DataFrame): Bootstrapped replicates.

    Returns:
        pd.DataFrame: Leaderboard with two new columns:
            - pct_times_best_performer: percent of simulations each model was best.
            - pct_times_top_5_percentile: percent of simulations each model was in top 5th percentile.
    """
    metrics = {
        "pct_times_best_performer": lambda df: df.idxmin(axis=0).value_counts(),
        "pct_times_top_5_percentile": lambda df: df.le(df.quantile(0.05, axis=0)).sum(axis=1),
    }

    for col, func in metrics.items():
        counts = func(df_simulated_scores)
        pct = counts / df_simulated_scores.columns.size * 100
        df_leaderboard[col] = df_leaderboard["model_pk"].map(pct).fillna(0)

    return df_leaderboard


def make_leaderboard(
    leaderboard_entries: List[pd.DataFrame],
) -> Dict[str, str]:
    """Create and write the full leaderboard from processed entries.

    Args:
        leaderboard_entries (List[pd.DataFrame]): Processed forecasts by model and date.

    Returns:
        None
    """
    logger.info(colored("Making leaderboard", "red"))

    df = combine_forecasting_rounds(leaderboard_entries)

    # The scoring functions to consider
    primary_scoring_func = two_way_fixed_effects
    scoring_funcs = [
        primary_scoring_func,
        peer_score,
        brier_skill_score,
    ]

    # Score
    df_leaderboard = score_models(df=df, scoring_funcs=scoring_funcs)

    df_simulated_scores = generate_simulated_leaderboards(
        df=df,
        primary_scoring_func=primary_scoring_func,
        N=N_REPLICATES,
    )

    # CIs
    df_leaderboard = get_confidence_interval(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores,
        primary_scoring_func=primary_scoring_func,
    )

    # Compare to best model
    df_leaderboard = get_comparison_to_best_model(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores,
        primary_scoring_func=primary_scoring_func,
        is_centered=False,
    )

    # Simulation performance measures
    df_leaderboard = get_simulation_performance_metrics(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores,
    )

    # Write leaderboard
    files = write_leaderboard(
        df=df_leaderboard,
        primary_scoring_func=primary_scoring_func,
    )

    upload_leaderboard(files)


def read_local_file_and_run(only_keep_date: str = "") -> bool:
    """Load cached leaderboard entries and run processing if cache exists.

    Only works when running locally.

    Args:
        only_keep_date (str): Date string for the cached pickle filename.

    Returns:
        bool: True if local cache was found and processed, False otherwise.
    """
    if not env.RUNNING_LOCALLY:
        return False

    pickle_file = f"leaderboard_{only_keep_date}.pkl"
    if not os.path.exists(pickle_file):
        return False

    with open(pickle_file, "rb") as file:
        leaderboard_entries = pickle.load(file)

    make_leaderboard(leaderboard_entries=leaderboard_entries)
    return True


def download_and_compile_processed_forecast_files(
    bucket: str, only_keep_date: str = ""
) -> List[pd.DataFrame]:
    """Download and compile processed forecast files into entries list.

    Args:
        None

    Returns:
        List[pd.DataFrame]: List of DataFrames for each processed forecast file,
            ready for leaderboard aggregation.
    """
    forecast_files, valid_dates = resolution.get_valid_forecast_files_and_dates(bucket=bucket)
    local_forecast_set_dir = data_utils.get_local_file_dir(bucket=bucket)
    leaderboard_entries = []
    for f in forecast_files:
        logger.info(f"Ranking {f}")
        data = resolution.read_forecast_file(filename=f"{local_forecast_set_dir}/{f}")
        if data is None:
            continue

        organization = data.get("organization")
        model = data.get("model")
        forecast_due_date = data.get("forecast_due_date")
        df = data.get("df")

        append_leaderboard_entry(
            leaderboard_entries=leaderboard_entries,
            org_and_model={"organization": organization, "model": model},
            df=df,
            forecast_due_date=forecast_due_date,
        )

    return leaderboard_entries


@decorator.log_runtime
def driver(_: Any) -> None:
    """Create a new leaderboard.

    Args:
        _ (Any): Unused placeholder argument for GCP Cloud Run Job.

    Returns:
        None: Exits the process on completion.
    """
    logger.info(colored("Making leaderboard.", "red"))
    only_keep_date = ""
    leaderboard_entries = download_and_compile_processed_forecast_files(
        bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
        only_keep_date=only_keep_date,
    )
    make_leaderboard(leaderboard_entries=leaderboard_entries)
    logger.info(colored("Done.", "red"))


if __name__ == "__main__":
    driver(None)
