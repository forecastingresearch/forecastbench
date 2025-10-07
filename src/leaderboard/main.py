"""Create leaderboard."""

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyfixest as pf
from jinja2 import Template
from joblib import Parallel, delayed
from pandas._libs.tslibs.nattype import NaTType
from scipy.stats import norm
from statsmodels.stats.multitest import multipletests
from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    dates,
    decorator,
    env,
    git,
    question_curation,
    resolution,
    slack,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LeaderboardType(str, Enum):
    """Enumeration of leaderboard types.

    This enum distinguishes between the two supported leaderboard variants:
    * BASELINE: The baseline leaderboard: FB forecast files w/o freeze values.
    * TOURNAMENT: The tournament leaderboard. All forecast files.
    """

    BASELINE = "baseline"
    TOURNAMENT = "tournament"


LEADERBOARD_UPDATED_DATE_STR = "Updated " + datetime.now().strftime("%b. %-d, %Y")

BASELINE_ORG_NAIVE_MODEL = {
    "organization": constants.BENCHMARK_NAME,
    "model": "Naive Forecaster",
    "model_organization": constants.BENCHMARK_NAME,
}

HUMAN_SUPERFORECASTER = {
    "organization": constants.BENCHMARK_NAME,
    "model": "Superforecaster median forecast",
    "model_organization": constants.BENCHMARK_NAME,
}
HUMAN_PUBLIC = {
    "organization": constants.BENCHMARK_NAME,
    "model": "Public median forecast",
    "model_organization": constants.BENCHMARK_NAME,
}

HUMAN_MODELS = [HUMAN_SUPERFORECASTER, HUMAN_PUBLIC]
HUMAN_MODELS_TO_HIGHLIGHT = [m["model"] for m in HUMAN_MODELS]

LEADERBOARD_DECIMAL_PLACES = 3

IMPUTED_CUTOFF_PCT = 5

MIN_DAYS_BEFORE_QUESTION_SET_IS_INCLUDED = 50

MODEL_RELEASE_DAYS_CUTOFF = 365

SIM_BOOTSTRAP_COL_PREFIX = "bootstrap"

N_REPLICATES = 1999 if not env.RUNNING_LOCALLY else 5

df_release_dates = pd.read_csv("model_release_dates.csv")
df_release_dates["model_release_date"] = pd.to_datetime(
    df_release_dates["model_release_date"], errors="coerce"
)

ALWAYS_05_MODEL = {
    "organization": "ForecastBench",
    "model": "Always 0.5",
}

LAST_UPDATED_DATE = dates.get_date_today_as_iso()

TOOLTIP_COLUMN_DESCRIPTIONS = {
    "Organization": "The team that submitted forecasts.",
    "Model Organization": "The organization that developed the model.",
    "Model": "The name of the model that was used to generate the forecasts.",
    "Dataset": (
        "Average difficulty-adjusted Brier score on dataset questions. "
        "Rescaled so that Always 0.5 has a score of 0.25. "
        "Lower scores are better."
    ),
    "Dataset 95% CI": "Bootstrapped 95% confidence interval for the Dataset score.",
    "Market": (
        "Average difficulty-adjusted Brier score on market questions. "
        "Rescaled so that Always 0.5 has a score of 0.25. "
        "Lower scores are better."
    ),
    "Market 95% CI": "Bootstrapped 95% confidence interval for the Market score.",
    "Overall": (
        "Average difficulty-adjusted Brier score across all questions. "
        "Rescaled so that the Always 0.5 forecaster has a score of 0.25. "
        "Lower scores are better."
    ),
    "Overall 95% CI": "Bootstrapped 95% confidence interval for the Overall score.",
    "Supers > Forecaster?": (
        "One-sided p-value comparing each forecaster to the superforecaster median based on "
        f"{N_REPLICATES:,} simulations, with<br>"
        "H₀: This forecaster performs at least as well as the superforecaster median.<br>"
        "H₁: The superforecaster median outperforms this forecaster."
    ),
    "Forecaster > Public?": (
        "One-sided p-value comparing each forecaster to the public median based on "
        f"{N_REPLICATES:,} simulations, with<br>"
        "H₀: The public median performs at least as well as this forecaster.<br>"
        "H₁: This forecaster outperforms the public median."
    ),
    "Pct times № 1": (
        f"Percentage of {N_REPLICATES:,} simulations in which this model was the best " "performer."
    ),
    "Pct times top 5%": (
        f"Percentage of {N_REPLICATES:,} simulations in which this model ranked in the top 5%."
    ),
    "x% oracle equiv": (
        "This model is most similar to a reference model that forecasts x% when the question "
        "resolved to 1 and (1-x)% when the question resolved to 0. x moves in increments of 1 "
        "from 0 - 100 inclusive. The 100% forecaster can be viewed as an oracle."
    ),
    "Peer": (
        "Peer score relative to the average Brier score on each question. "
        "Higher scores are better."
    ),
    "BSS": (
        "Brier Skill Score using the ForecastBench Naive Forecaster. " "Higher scores are better."
    ),
}


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


def get_dataset_mask(df: pd.DataFrame) -> pd.Series:
    """Generate boolean masks for market questions.

    Args:
        df (pd.DataFrame): The forecast set.

    Returns:
        pd.Series: questions from DATA_SOURCES
    """
    return df["source"].isin(question_curation.DATA_SOURCES)


def get_market_mask(df: pd.DataFrame) -> pd.Series:
    """Generate boolean masks for market questions.

    Args:
        df (pd.DataFrame): The forecast set.

    Returns:
        pd.Series: all questions from MARKET_SOURCES.
    """
    return df["source"].isin(question_curation.MARKET_SOURCES)


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
    data_mask = get_dataset_mask(df=df)
    market_mask = get_market_mask(df=df)
    resolved_mask = df["resolved"].astype(bool)
    return {
        "dataset": data_mask & resolved_mask,
        "market": market_mask,
        "market_resolved": market_mask & resolved_mask,
        "market_unresolved": market_mask & ~resolved_mask,
    }


def set_model_pk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the model primary key.

    Args:
        df (pd.DataFrame): Forecast set.

    Returns:
        df (pd.DataFrame): Forecast set with `model_pk` field.
    """
    df["model_pk"] = df["organization"] + "_" + df["model_organization"] + "_" + df["model"]
    return df


def filter_forecast_files_by_forecast_due_date(
    forecast_files: List[str],
    valid_dates: List[str],
) -> Tuple[List[str], List[str]]:
    """Filter forecast files to include only those from sufficiently old date folders.

    The cutoff is determined by `MIN_DAYS_BEFORE_QUESTION_SET_IS_INCLUDED`.

    Args:
        forecast_files (List[str]): List of forecast file paths on GCP bucket, where each path
                                    begins with a date folder in the format YYYY-MM-DD.
        valid_dates (List[str]): List of valid dates (YYYY-MM-DD) associated with the forecast
                                 files.

    Returns:
        tuple(List[str], List[str]): A tuple containing:
            - forecast_files (List[str]): Filtered forecast files, keeping only those in date
                                          folders older than the cutoff date.
            - valid_dates (List[str]): The input valid_dates, passed through unchanged.
    """
    cutoff = dates.get_date_today() - timedelta(days=MIN_DAYS_BEFORE_QUESTION_SET_IS_INCLUDED)
    valid_dates = sorted(
        [d for d in valid_dates if datetime.strptime(d, "%Y-%m-%d").date() <= cutoff]
    )
    forecast_files = [f for f in forecast_files if f.split("/")[0] in valid_dates]
    return forecast_files, valid_dates


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

    def over_imputed_cutoff(d: pd.DataFrame) -> bool:
        return d["imputed"].mean() * 100 > IMPUTED_CUTOFF_PCT

    df = resolution.make_columns_hashable(df)

    # Do not run test for the dummy models ForecastBench produces:
    #   e.g. Imputed Forecaster, Naive Forecaster, ...
    run_imputed_test_for_model = not (
        org_and_model.get("organization") == constants.BENCHMARK_NAME
        and org_and_model.get("model_organization") == constants.BENCHMARK_NAME
    )

    # Ignore if too many imputed forecasts overall
    if run_imputed_test_for_model and over_imputed_cutoff(d=df):
        return None

    # Remove combination questions
    df = df[~df["id"].apply(resolution.is_combo)]

    # Drop market unresolved questions
    masks = get_masks(df)

    # Ignore if too many imputed forecasts for questions we will _eventually_ score
    if run_imputed_test_for_model:
        for mask in ["market", "dataset"]:
            if over_imputed_cutoff(d=df[masks[mask]]):
                return None

    df = df[masks["dataset"] | masks["market_resolved"]]

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
    if not df[df["question_pk"] == ""].empty:
        raise ValueError(f"Error assigning `question_pk` {org_and_model}.")

    # Set team info
    df["organization"] = org_and_model["organization"]
    df["model"] = org_and_model["model"]
    df["model_organization"] = org_and_model["model_organization"]
    df = set_model_pk(df)

    return df.sort_values(by=["forecast_due_date", "source", "id"], ignore_index=True)


def process_forecast_file(
    leaderboard_entries: List[pd.DataFrame],
    org_and_model: Dict[str, str],
    df: pd.DataFrame,
    forecast_due_date: str,
) -> None:
    """Append each model's processed forecast set to the leaderboard_entries list.

    Args:
        leaderboard_entries (List[pd.DataFrame]): List collecting processed forecast set DataFrames.
        org_and_model (Dict[str, str]): The organization and model associated with the forecast set.
        df (pd.DataFrame): Forecast set.
        forecast_due_date (str): Forecast due date in 'YYYY-MM-DD' format.

    Returns:
        None: Modifies `leaderboard_entries` in place. Logs a warning if processing is skipped.
    """
    processed = get_df_info(
        df=df,
        org_and_model=org_and_model,
        forecast_due_date=forecast_due_date,
    )
    if processed is None:
        logger.warning(
            colored(
                f"Ignoring {org_and_model['organization']} {org_and_model['model']}—"
                "imputed cutoff exceeded.",
                "yellow",
            )
        )
        return

    leaderboard_entries.append(processed)


def write_llm_super_parity_dates(parity_dates: dict) -> None:
    """Write LLM-Super parity dates for SOTA graph.

    Args:
        parity_dates (dict[str, dict[object, dict[str, str]]]): Nested mapping of
            question types to leaderboard identifiers to summary strings
            (e.g., {'lower': 'Aug 2025', 'median': 'Nov 2025', 'upper': 'Apr 2026'}).

    Returns:
        None
    """
    directory = data_utils.get_mounted_bucket(bucket=env.PUBLIC_RELEASE_BUCKET)
    local_filename = f"{directory}/simulated_llm_parity/parity_dates.json"
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    with open(local_filename, "w", encoding="utf-8") as f:
        json.dump(parity_dates, f, default=str, ensure_ascii=False, indent=2, sort_keys=True)


def write_question_fixed_effects(question_fixed_effects: Dict[str, pd.DataFrame]) -> None:
    """Write and upload question fixed effects.

    Args:
        question_fixed_effects (Dict[str, pd.DataFrame]): A mapping from label
            (e.g., "dataset", "market") to a DataFrame containing question-level
            fixed effects.

    Returns:
        None: Concatenated DataFrame is created (and can be written or processed
        further inside the function).
    """
    logger.info(colored("Writing question fixed effects to WEBSITE.", "yellow"))

    df = pd.concat(question_fixed_effects.values(), ignore_index=True)
    df.loc[get_market_mask(df), "horizon"] = None

    directory = data_utils.get_mounted_bucket(bucket=env.PUBLIC_RELEASE_BUCKET)
    iso_date = dates.get_date_today_as_iso()
    local_filename = f"{directory}/question-fixed-effects/question_fixed_effects.{iso_date}.json"
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)
    df.to_json(local_filename, orient="records")


def write_leaderboard_html_file(
    df: pd.DataFrame,
    sorting_column_number: int,
    leaderboard_type: LeaderboardType,
) -> None:
    """Generate HTML and CSV leaderboard files and upload to Bucket & git repo.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        sorting_column_number (int): column to sort by.
        leaderboard_type (LeaderboardType): The type of leaderboard to generate
                                            (e.g., baseline or tournament).

    Returns:
        None.
    """
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
        <th>Model Organization</th>
        <th>Model</th>
        <th>Dataset (N)</th>
        <th><!-- N dataset --></th>
        <th>Dataset 95% CI</th>
        <th>Market (N)</th>
        <th><!-- N market --></th>
        <th>Market 95% CI</th>
        <th>Overall (N)</th>
        <th><!-- N --></th>
        <th>Overall 95% CI</th>
        <th>Supers > Forecaster?</th>
        <th>p-val Supers > Forecaster?</th>
        <th>Forecaster > Public?</th>
        <th>p-val Forecaster > Public?</th>
        <th>Pct times № 1</th>
        <th>Pct times top 5%</th>
        <th>x% oracle equiv</th>
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
                        + ' data-html="{{ col_desc["Dataset"] }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3)+
                           ' <span class="n-count">('+
                           Number(row['N dataset']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='Market') {
            col.title = 'Market (N) <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Market"] }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3) +
                ' <span class="n-count">('+
                Number(row['N market']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='Overall') {
            col.title = 'Overall (N) <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Overall"] }}"></i>';
            col.render = function(d,t,row){
                return t==='display'?parseFloat(d).toFixed(3) +
                ' <span class="n-count">('+
                Number(row['N']).toLocaleString()+')</span>':d; };
            col.orderSequence = ['asc','desc'];
          }
          if (name==='Supers > Forecaster?') {
            col.title = 'Supers > Forecaster? <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Supers > Forecaster?"] }}"></i>';
            col.orderable=false;
          }
          if (name==='Forecaster > Public?') {
            col.title = 'Forecaster > Public? <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Forecaster > Public?"] }}"></i>';
            col.orderable=false;
          }
          if (name==='Pct times № 1') {
            col.title = 'Pct times № 1 <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Pct times № 1"] }}"></i>';
            col.render = function(d,t){ return t==='display'?Math.round(d)+'%':d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==='Pct times top 5%') {
            col.title = 'Pct times top 5% <i class="info circle icon" '
                        + ' data-html="{{ col_desc["Pct times top 5%"] }}"></i>';
            col.render = function(d,t){ return t==='display'?Math.round(d)+'%':d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==='Dataset 95% CI') {
            col.title = 'Dataset 95% CI <i class="info circle icon" ' +
                        'data-html="{{ col_desc["Dataset 95% CI"] }}"></i>';
            col.orderable=false;
          }
          if (name==='Market 95% CI') {
            col.title = 'Market 95% CI <i class="info circle icon" ' +
                        'data-html="{{ col_desc["Market 95% CI"] }}"></i>';
            col.orderable=false;
          }
          if (name==='Overall 95% CI') {
            col.title = 'Overall 95% CI <i class="info circle icon" ' +
                        'data-html="{{ col_desc["Overall 95% CI"] }}"></i>';
            col.orderable=false;
          }
          if (name==='x% oracle equiv') {
            col.title = 'x% oracle equiv <i class="info circle icon" '
                        + ' data-html="{{ col_desc["x% oracle equiv"] }}"></i>';
            col.orderable=false;
          }
          if (name==="Peer") {
            col.title = 'Peer <i class="info circle icon" data-html="{{ col_desc["Peer"] }}"></i>';
            col.render = function(d,t){ return t==='display'?parseFloat(d).toFixed(3):d; };
            col.orderSequence = ['desc','asc'];
          }
          if (name==="BSS") {
            col.title = 'BSS <i class="info circle icon" data-html="{{ col_desc["BSS"] }}"></i>';
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
        sorting_column_number=sorting_column_number,
        col_desc=TOOLTIP_COLUMN_DESCRIPTIONS,
    )

    stem = f"leaderboard_{leaderboard_type.value}"
    destination_folder = "leaderboards/html"
    local_filename_html, destination_filename_html = data_utils.write_file_to_bucket(
        bucket=env.PUBLIC_RELEASE_BUCKET,
        basename=f"{stem}.html",
        destination_folder=f"{destination_folder}",
        data=html,
    )

    directory = data_utils.get_mounted_bucket(bucket=env.PUBLIC_RELEASE_BUCKET)
    destination_folder = "leaderboards/csv"
    os.makedirs(f"{directory}/{destination_folder}", exist_ok=True)
    destination_filename_csv = f"{destination_folder}/{stem}.csv"
    local_filename_csv = f"{directory}/{destination_filename_csv}"
    df.to_csv(local_filename_csv, index=False)

    git.clone_commit_and_push(
        files={
            local_filename_html: destination_filename_html,
            local_filename_csv: destination_filename_csv,
        },
        commit_message=f"leaderboard {leaderboard_type.value}: automatic update html & csv files.",
    )


def write_leaderboard_js_file_full(
    df: pd.DataFrame,
    leaderboard_type: LeaderboardType,
) -> Dict[str, str]:
    """Generate JS file for website Leaderboard page.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        leaderboard_type (LeaderboardType): The type of leaderboard to generate
                                            (e.g., baseline or tournament).

    Returns:
        None.
    """
    template = Template(
        """
        $(function()
        {
            const data = {{ data }};
            const cols = ["Rank", {% if include_team %} "Team",{% endif %}
                          "Model Organization", "Model Organization Logo", "Model",
                          "Dataset", "N dataset", "Dataset 95% CI",
                          "Market", "N market", "Market 95% CI",
                          "Overall", "N", "Overall 95% CI",
                          "Supers > Forecaster?", "p-val Supers > Forecaster?",
                          "Forecaster > Public?", "p-val Forecaster > Public?"];
            const columns = cols.map(name => {
                const col = { data: name, title: name };
                if (name === "Rank") {
                  col.className = 'dt-center';
                }
                if (name === "Team") {
                  col.className = 'dt-center';
                  col.render = d =>
                      d
                      ? `<img src="/assets/images/org_logos/${d}" alt="" style="height:20px">`
                      : '';
                }

                if (name === "Model Organization") {
                  col.title = "Org";
                  col.className = 'dt-center';
                  col.render = (d, t, row) => {
                    if (t === 'display') {
                      return row['Model Organization Logo']
                        ? `<img src="/assets/images/org_logos/${row['Model Organization Logo']}"
                                alt="${d}" style="height:20px">`
                        : d;
                    }
                    return d; // Use text value for search/sort
                  };
                }

                if (["N dataset", "N market", "N", "Model Organization Logo",
                     "p-val Supers > Forecaster?", "p-val Forecaster > Public?"].includes(name)) {
                  col.visible = false;
                }

                if (name === "Dataset") {
                  col.title = "Dataset (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N dataset"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "Market") {
                  col.title = "Market (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N market"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "Overall") {
                  col.title = "Overall (N)";
                  col.render = (d, t, row) =>
                    t === "display"
                      ? parseFloat(d).toFixed(3) +
                        ' <span class="n-count">(' +
                        Number(row["N"]).toLocaleString() +
                        ")</span>"
                      : d;
                  col.orderSequence = ["asc", "desc"];
                }

                if (name === "Supers > Forecaster?"
                    || name === "Forecaster > Public?") {
                      col.orderable = false;
                }

                // Add cell tooltips that show the hidden p-values
                // Supers > Forecaster?
                if (name === "Supers > Forecaster?") {
                  col.className = (col.className ? col.className + ' ' : '') + 'dt-center';
                  col.render = (d, t, row) => {
                    if (t !== 'display') return d;
                    const p = row['p-val Supers > Forecaster?'];
                    const tip = (p == null || p === '') ? 'p-value unavailable' : 'p-val: ' + String(p);
                    const val = (d ?? '') === '' ? '' : d;
                    return `<span class="cell-tooltip" data-tooltip="${tip}"
                                  style="cursor:help">${val}</span>`;
                  };
                }

                // Forecaster > Public?
                if (name === "Forecaster > Public?") {
                  col.className = (col.className ? col.className + ' ' : '') + 'dt-center';
                  col.render = (d, t, row) => {
                    if (t !== 'display') return d;
                    const p = row['p-val Forecaster > Public?'];
                    const tip = (p == null || p === '') ? 'p-value unavailable' : 'p-val: ' + String(p);
                    const val = (d ?? '') === '' ? '' : d;
                    return `<span class="cell-tooltip" data-tooltip="${tip}"
                                  style="cursor:help">${val}</span>`;
                  };
                }

                if (["Dataset 95% CI", "Market 95% CI", "Overall 95% CI"].includes(name)) {
                  col.orderable = false;
                }

                return col;
            });

            $('#leaderboard-table-full').html(`
               <table id="lb" class="display compact hover" style="width:100%">
               <thead>
                 <tr>
                   <th>Rank</th>
                   {% if include_team %}
                   <th class="column-header-tooltip" data-tooltip="Team">Team</th>
                   {% endif %}
                   <th class="column-header-tooltip" data-tooltip="Org">Org</th>
                   <th><!-- Model Organization Logo --></th>
                   <th class="column-header-tooltip" data-tooltip="Model">Model</th>
                   <th class="column-header-tooltip" data-tooltip="Dataset (N)">Dataset (N)</th>
                   <th><!-- N dataset --></th>
                   <th class="column-header-tooltip" data-tooltip="Dataset 95% CI">Dataset 95% CI</th>
                   <th class="column-header-tooltip" data-tooltip="Market (N)">Market (N)</th>
                   <th><!-- N market --></th>
                   <th class="column-header-tooltip" data-tooltip="Market 95% CI">Market 95% CI</th>
                   <th class="column-header-tooltip" data-tooltip="Overall (N)">Overall (N)</th>
                   <th><!-- N --></th>
                   <th class="column-header-tooltip" data-tooltip="Overall 95% CI">Overall 95% CI</th>
                   <th class="column-header-tooltip"
                       data-tooltip="Supers > Forecaster?">Supers > Forecaster?</th>
                   <th><!-- p-val Supers > Forecaster? --></th>
                   <th class="column-header-tooltip"
                       data-tooltip="Forecaster > Public?">Forecaster > Public?</th>
                   <th><!-- p-val Forecaster > Public? --></th>
                 </tr>
               </thead>
               <tbody></tbody>
             </table>
             `);
             const table = $("#lb").DataTable({
               data: data,
               columns: columns,
               order: [[cols.indexOf("Overall"), "asc"]],
               pageLength:25,
               lengthMenu:[[10,25,50,100,-1],[10,25,50,100,"All"]],
               paging: true,
               info: true,
               dom:'<"top"lfr>t<"bottom"<"info-pagination-wrapper"ip>>',
               responsive: true,
               search: { regex: true, smart: true },
               createdRow: function(row, data, dataIndex) {
                 if ({{ model_highlight_rows | tojson }}.includes(data.Model)) {
                   $(row).css('background-color', '#fdece8');
                 }
               },
               infoCallback: function(settings, start, end, max, total, pre) {
                   return pre + '<br>last updated {{ last_updated_date }}';
               }
           });
           table.on('draw.dt', function () {
             initializeTooltips();
           });
           // Initialize tooltips after table is created
           initializeTooltips();
        });
        // Tooltip content object (defined globally for access)
        const tooltipContent = {
          'Team': `{{ col_desc["Organization"] }}`,
          'Org': `{{ col_desc["Model Organization"] }}`,
          'Model': `{{ col_desc["Model"] }}`,
          'Dataset (N)': `{{ col_desc["Dataset"] }}`,
          'Dataset 95% CI': `{{ col_desc["Dataset 95% CI"] }}`,
          'Market (N)': `{{ col_desc["Market"] }}`,
          'Market 95% CI': `{{ col_desc["Market 95% CI"] }}`,
          'Overall (N)': `{{ col_desc["Overall"] }}`,
          'Overall 95% CI': `{{ col_desc["Overall 95% CI"] }}`,
          'Supers > Forecaster?': `{{ col_desc["Supers > Forecaster?"] }}`,
          'Forecaster > Public?': `{{ col_desc["Forecaster > Public?"] }}`
        };"""
    )

    js = template.render(
        data=df.to_dict(orient="records"),
        last_updated_date=LAST_UPDATED_DATE,
        model_highlight_rows=HUMAN_MODELS_TO_HIGHLIGHT,
        col_desc=TOOLTIP_COLUMN_DESCRIPTIONS,
        include_team=leaderboard_type != LeaderboardType.BASELINE,
    )

    return {
        "filename": f"leaderboard_{leaderboard_type.value}_full.js",
        "js": js,
    }


def write_leaderboard_js_file_compact(
    df: pd.DataFrame,
    leaderboard_type: LeaderboardType,
) -> Dict[str, str]:
    """Generate JS file for website Home page.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        leaderboard_type (LeaderboardType): The type of leaderboard to generate
                                            (e.g., baseline or tournament).

    Returns:
        None.
    """
    template = Template(
        """
        ;(function(){ if(!document.getElementById('leaderboard-{{ leaderboard_type }}-compact')) return;
        $(function()
        {
            const data = {{ data }};
            $('#leaderboard-{{ leaderboard_type }}-compact').html(`
            <table id="lb-{{ leaderboard_type }}" class="display stripe hover" style="width:100%">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th class="column-header-tooltip" data-tooltip="Model Organization">Org</th>
                  <th class="column-header-tooltip" data-tooltip="Model">Model</th>
                  <th class="column-header-tooltip" data-tooltip="Overall">Overall</th>
                </tr>
              </thead>
            </table>
            `);
            const table = $('#lb-{{ leaderboard_type }}').DataTable({
              data:data,
              columns:[
                {data:'Rank', className:'dt-center', width:'5%'},
                {
                  data:'Model Organization',
                  className:'dt-center',
                  width:'10%',
                  render: (d, type, row) => {
                    if (type === 'display') {
                      return row['Model Organization Logo']
                        ? `<img src="/assets/images/org_logos/${row['Model Organization Logo']}"
                                alt="${d}" style="height:20px">`
                        : d;
                    }
                    return d; // Use text value for search/sort
                  }
                },
                {data:'Model', width:'70%'},
                {data:'Overall', render:d=>parseFloat(d).toFixed(3), width:'15%'}
              ],
              autoWidth:false,
              order:[[3,'asc']],
              pageLength:10,
              pagingType:'simple',
              lengthMenu:[[10,25,50,100,-1],[10,25,50,100,"All"]],
              paging:true,
              info:true,
              dom:'<"top"lfr>t<"bottom"<"info-pagination-wrapper"ip>>',
              responsive:true,
              createdRow: function(row, data, dataIndex) {
                 if ({{ model_highlight_rows | tojson }}.includes(data.Model)) {
                   $(row).css('background-color', '#fdece8');
                 }
               },
              infoCallback: function(settings, start, end, max, total, pre) {
                  return pre + '<br>last updated {{ last_updated_date }}';
              }
            });
            table.on('draw.dt', function () {
              initializeTooltips();
            });
            // Initialize tooltips after table is created
            initializeTooltips();
          });
        // Tooltip content object (defined globally for access)
        // Keys MUST match the <th data-tooltip="..."> values
        window.tooltipContent = Object.assign(window.tooltipContent || {}, {
          'Model Organization': `{{ col_desc["Model Organization"] }}`,
          'Model': `{{ col_desc["Model"] }}`,
          'Overall': `{{ col_desc["Overall"] }}`
        });
        })();"""
    )

    js = template.render(
        data=df[
            ["Rank", "Model Organization", "Model Organization Logo", "Model", "Overall"]
        ].to_dict(orient="records"),
        last_updated_date=LAST_UPDATED_DATE,
        model_highlight_rows=HUMAN_MODELS_TO_HIGHLIGHT,
        col_desc=TOOLTIP_COLUMN_DESCRIPTIONS,
        leaderboard_type=leaderboard_type.value,
    )

    return {
        "filename": f"leaderboard_{leaderboard_type.value}_compact.js",
        "js": js,
    }


def write_leaderboard_js_files(
    df: pd.DataFrame,
    leaderboard_type: LeaderboardType,
) -> None:
    """Wrap functions to create JS files for website.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        leaderboard_type (LeaderboardType): The type of leaderboard to generate
                                            (e.g., baseline or tournament).

    Returns:
        None.
    """
    df = df.copy()
    df["Model Organization Logo"] = df["Model Organization"].map(constants.ORG_TO_LOGO).fillna("")
    df["Team"] = df["Team"].apply(lambda x: constants.ORG_TO_LOGO.get(x, x))

    leaderboards = [
        write_leaderboard_js_file_compact(df=df, leaderboard_type=leaderboard_type),
        write_leaderboard_js_file_full(df=df, leaderboard_type=leaderboard_type),
    ]
    # destination_folder = "assets/js/"
    # os.makedirs(destination_folder, exist_ok=True)
    for leaderboard in leaderboards:
        data_utils.write_file_to_bucket(
            bucket=env.PUBLIC_RELEASE_BUCKET,
            basename=leaderboard["filename"],
            destination_folder="leaderboards/js",
            data=leaderboard["js"],
        )


def write_sota_graph_csv(
    df: pd.DataFrame,
    leaderboard_type: LeaderboardType,
) -> None:
    """Write CSV for SOTA graph on website Explore page.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        leaderboard_type (LeaderboardType): The type of leaderboard to generate
             (e.g., baseline or tournament). Only the baseline is used for the graph.

    Returns:
        None.
    """
    df = df[
        [
            "Team",
            "Model Organization",
            "Model",
            "Dataset",
            "N dataset",
            "Dataset 95% CI",
            "Market",
            "N market",
            "Market 95% CI",
            "Overall",
            "N",
            "Overall 95% CI",
            "Model release date",
        ]
    ]
    directory = data_utils.get_mounted_bucket(bucket=env.PUBLIC_RELEASE_BUCKET)
    destination_folder = "leaderboards/csv"
    os.makedirs(f"{directory}/{destination_folder}", exist_ok=True)
    destination_filename_csv = f"{destination_folder}/sota_graph_{leaderboard_type.value}.csv"
    local_filename_csv = f"{directory}/{destination_filename_csv}"
    df.to_csv(local_filename_csv, index=False)


def write_leaderboard(
    df: pd.DataFrame,
    primary_scoring_func: Callable[..., any],
    leaderboard_type: LeaderboardType,
) -> None:
    """Generate HTML and CSV leaderboard files for git and JS files for website.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        primary_scoring_func (Callable): Function used to compute the primary overall score;
            its __name__ determines which columns to format and sort by.

    Returns:
        None.
    """
    logger.info("Making HTML and CSV leaderboard file.")

    # Replace NaN with empty strings for display
    df = df.fillna("")

    # Round columns to LEADERBOARD_DECIMAL_PLACES decimal places
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].round(LEADERBOARD_DECIMAL_PLACES)

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
    def format_ci(df, question_type):
        col_prefix = f"{primary_scoring_func.__name__}_{question_type}"
        df[f"{col_prefix}_ci_lower"] = (
            df[f"{col_prefix}_ci_lower"].round(LEADERBOARD_DECIMAL_PLACES).astype(str)
        )
        df[f"{col_prefix}_ci_upper"] = (
            df[f"{col_prefix}_ci_upper"].round(LEADERBOARD_DECIMAL_PLACES).astype(str)
        )
        df[f"{col_prefix}_ci"] = (
            "[" + df[f"{col_prefix}_ci_lower"] + ", " + df[f"{col_prefix}_ci_upper"] + "]"
        )
        return df

    df = format_ci(df, "dataset")
    df = format_ci(df, "market")
    df = format_ci(df, "overall")

    df = df.sort_values(by=f"{primary_scoring_func.__name__}_overall", ignore_index=True)

    p_value_cols = {}
    for comparison in HUMAN_MODELS:
        col_name = get_comparison_p_val_col(comparison)
        col_name_simple = col_name + "_simple"
        df[col_name_simple] = df[col_name].apply(
            lambda p: (
                "Yes" if p < 0.001 else "Yes" if p < 0.01 else "Likely" if p < 0.05 else "No"
            )
        )
        df[col_name] = df[col_name].apply(
            lambda p: (
                "<0.001"
                if p < 0.001
                else "<0.01" if p < 0.01 else "<0.05" if p < 0.05 else f"{p:.2f}"
            )
        )
        # Set the p-value for the best to N/A
        comparison_idx = get_comparison_model_index(df=df, comparison=comparison)
        df.loc[comparison_idx, col_name] = "—"
        df.loc[comparison_idx, col_name_simple] = "—"
        if comparison == HUMAN_SUPERFORECASTER:
            p_value_cols[col_name_simple] = "Supers > Forecaster?"
            p_value_cols[col_name] = "p-val Supers > Forecaster?"
        elif comparison == HUMAN_PUBLIC:
            p_value_cols[col_name_simple] = "Forecaster > Public?"
            p_value_cols[col_name] = "p-val Forecaster > Public?"
        else:
            raise ValueError("Comparison model not handled")

    df["x_pct_oracle_equivalent"] = df["x_pct_oracle_equivalent"].map("{:.0%}".format)

    # For website communication purposes, change "freeze values" to "crowd forecast"
    benchmark_mask = df["organization"] == constants.BENCHMARK_NAME
    df.loc[benchmark_mask, "model"] = df.loc[benchmark_mask, "model"].str.replace(
        "freeze values", "crowd forecast"
    )

    df = df[
        [
            "Rank",
            "organization",
            "model_organization",
            "model",
            f"{primary_scoring_func.__name__}_dataset",
            "n_dataset",
            f"{primary_scoring_func.__name__}_dataset_ci",
            f"{primary_scoring_func.__name__}_market",
            "n_market",
            f"{primary_scoring_func.__name__}_market_ci",
            f"{primary_scoring_func.__name__}_overall",
            "n_overall",
            f"{primary_scoring_func.__name__}_overall_ci",
            *p_value_cols.keys(),
            "pct_times_best_performer",
            "pct_times_top_5_percentile",
            "x_pct_oracle_equivalent",
            "peer_score_overall",
            "brier_skill_score_overall",
            "model_release_date",
        ]
    ].rename(
        columns={
            "organization": "Team",
            "model_organization": "Model Organization",
            "model": "Model",
            f"{primary_scoring_func.__name__}_dataset": "Dataset",
            "n_dataset": "N dataset",
            f"{primary_scoring_func.__name__}_dataset_ci": "Dataset 95% CI",
            f"{primary_scoring_func.__name__}_market": "Market",
            "n_market": "N market",
            f"{primary_scoring_func.__name__}_market_ci": "Market 95% CI",
            f"{primary_scoring_func.__name__}_overall": "Overall",
            "n_overall": "N",
            f"{primary_scoring_func.__name__}_overall_ci": "Overall 95% CI",
            **p_value_cols,
            "pct_times_best_performer": "Pct times № 1",
            "pct_times_top_5_percentile": "Pct times top 5%",
            "x_pct_oracle_equivalent": "x% oracle equiv",
            "peer_score_overall": "Peer",
            "brier_skill_score_overall": "BSS",
            "model_release_date": "Model release date",
        }
    )

    # Write CSV for SOTA graph for website
    write_sota_graph_csv(
        df=df,
        leaderboard_type=leaderboard_type,
    )
    df = df.drop(columns="Model release date")

    # Write HTML and CSV leaderboard for datasets repo
    write_leaderboard_html_file(
        df=df,
        sorting_column_number=9,
        leaderboard_type=leaderboard_type,
    )

    # Write JS leaderboard for website
    df = df.drop(
        columns=[
            "Pct times № 1",
            "Pct times top 5%",
            "x% oracle equiv",
            "Peer",
            "BSS",
        ]
    )
    write_leaderboard_js_files(
        df=df,
        leaderboard_type=leaderboard_type,
    )


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


def remove_tournament_models(df: pd.DataFrame) -> pd.DataFrame:
    """Remove models that only belong on the Tournament Leaderboard.

    This means return ForecastBench models that do _not_ contain the words:
    * with freeze values
    * with news
    * with SECOND news

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: Filtered dataframe with all models for Tournament Leaderboard removed.
    """
    df = df.copy()
    org_mask = df["organization"] == constants.BENCHMARK_NAME
    vanilla_model_mask = ~df["model"].str.contains("with freeze values|with news|with SECOND news")
    mask = org_mask & vanilla_model_mask
    return df[mask].reset_index(drop=True)


def two_way_fixed_effects(df: pd.DataFrame, question_type) -> pd.DataFrame:
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

    # Remove x pct forecasters
    df_fe = remove_x_pct_oracles(df=df)

    # Remove models that only belong in the Tournament Leaderboard
    # e.g. with freeze values
    # After this, all models were submitted by the ForecastBench organization
    df_fe = remove_tournament_models(df=df_fe)

    if question_type == "dataset":
        # Drop some Benchmark models
        benchmark_models_to_drop = [
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
        benchmark_model_mask = ~df_fe["model"].isin(benchmark_models_to_drop)
        df_fe = df_fe[benchmark_model_mask]

        # Remove models with old release dates
        org_mask = df_fe["model_organization"] == constants.BENCHMARK_NAME
        date_mask = df_fe["days_since_model_release"] < MODEL_RELEASE_DAYS_CUTOFF
        df_fe = df_fe[org_mask | date_mask].reset_index(drop=True)

        mod = pf.feols("brier_score ~ 1 | question_pk + model_pk", data=df_fe)
        dict_question_fe = mod.fixef()["C(question_pk)"]
    elif question_type == "market":
        # Estimated question fixed effects are eequivalent to the market Brier
        mask = (
            (df_fe["organization"] == constants.BENCHMARK_NAME)
            & (df_fe["model_organization"] == constants.BENCHMARK_NAME)
            & (df_fe["model"] == "Imputed Forecaster")
        )
        dict_question_fe = df_fe[mask].set_index("question_pk")["brier_score"].to_dict()
    else:
        raise ValueError(f"Question Type: {question_type} not found.")

    if len(dict_question_fe) != len(df["question_pk"].unique()):
        raise ValueError(
            f"Estimated num. of question fixed effects ({len(dict_question_fe)}) not equal to num. "
            f"of questions ({len(df['question_pk'].unique())})"
        )

    df["question_fixed_effect"] = df["question_pk"].map(dict_question_fe)
    df["two_way_fixed_effects"] = df["brier_score"] - df["question_fixed_effect"]
    return df[orig_cols + ["two_way_fixed_effects", "question_fixed_effect"]]


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
    df: pd.DataFrame,
    scoring_funcs: List[Callable[[pd.DataFrame], pd.DataFrame]],
) -> pd.DataFrame:
    """Score models using the scoring functions in `scoring_funcs`.

    Args:
        df (pd.DataFrame): Combined forecast set.
        scoring_funcs (List[Callable[[pd.DataFrame], pd.DataFrame]]): List of scoring functions.

    Returns:
        Tuple[pd.DataFrame, Dict[str, pd.Series]]:
            - df_leaderboard: Leaderboard with
                - For each scoring function: '{func_name}_dataset', '{func_name}_market', and
                  '{func_name}_overall'
                - Count columns for dataset, market, and all questions
                - The 'organization', 'model', 'model_pk' associated with a forecast set
            - question_fixed_effects: Dict with optional entries
              {'dataset': Series, 'market': Series} containing per-question fixed effects when
              `save_question_fe=True`; otherwise {}.
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
    question_fixed_effects = {}
    for question_type in ["dataset", "market"]:
        df_qt = df[get_masks(df)[question_type]].reset_index(drop=True)
        df_qt = brier_score(df_qt)
        for func in scoring_funcs:
            name = func.__name__
            if func is two_way_fixed_effects:
                df_qt = func(df_qt, question_type).rename(columns={name: f"{name}_{question_type}"})
            else:
                df_qt = func(df_qt).rename(columns={name: f"{name}_{question_type}"})

        if two_way_fixed_effects in scoring_funcs:
            question_fixed_effects[question_type] = df_qt[
                [
                    "forecast_due_date",
                    "source",
                    "id",
                    "horizon",
                    "question_fixed_effect",
                ]
            ].drop_duplicates(ignore_index=True)
            df_qt = df_qt.drop(columns="question_fixed_effect")

        # Calculate the mean score for the question type for each model
        # Also count the N for the question type
        question_type_result = (
            df_qt.groupby(
                [
                    "organization",
                    "model_organization",
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
    df_leaderboard = pd.merge(
        results[0],
        results[1],
        on=[
            "organization",
            "model_organization",
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

    if two_way_fixed_effects in scoring_funcs:
        df_leaderboard = rescale_difficulty_adjusted_brier(
            df_leaderboard=df_leaderboard,
            primary_scoring_func=two_way_fixed_effects,
        )
    return df_leaderboard, question_fixed_effects


@decorator.log_runtime
def generate_simulated_leaderboards(
    df: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
    N: int = N_REPLICATES,
) -> pd.DataFrame:
    """Generate simulated leaderboards by bootstrap sampling.

    Args:
        df (pd.DataFrame): Combined forecast set to sample from.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]): Function to compute the
                     primary overall score.
        N (int): Number of bootstrap replicates to generate.

    Returns:
        pd.DataFrame: Simulated scores with each column representing a replicate.
    """
    logger.info(colored(f"Generate {N} simulated leaderboards", "red"))
    if not callable(primary_scoring_func):
        raise ValueError("The primary scoring function must be callable.")

    df = df.copy()

    def question_level_bootstrap(df: pd.DataFrame) -> pd.DataFrame:
        questions = df["question_pk"].drop_duplicates()
        questions_bs = questions.sample(frac=1, replace=True)
        sample = questions_bs.to_frame(name="question_pk")
        sample["draw"] = sample.groupby("question_pk").cumcount()
        retval = pd.merge(
            sample,
            df,
            on="question_pk",
            how="left",
        )
        # `question_pk` must be overwritten with a unique id in case it was sampled more than once.
        # This ensures that `two_way_fixed_effects()` treats each drawn question separately (instead
        # of treating multiple draws as one question).
        retval["question_pk"] = (
            retval["question_pk"].astype(str) + "_sim_id_" + retval["draw"].astype(str)
        )
        return retval.drop(columns=["draw"])

    def bootstrap_and_score(idx):
        logger.info(f"[replicate {idx+1}/{N}] starting...")
        df_bs = (
            df.groupby(["forecast_due_date", "source"])
            .apply(question_level_bootstrap, include_groups=False)
            .reset_index()
        )
        try:
            df_simulated_leaderboard, _ = score_models(
                df=df_bs,
                scoring_funcs=[primary_scoring_func],
            )
        except Exception as e:
            traceback.print_exc()
            raise e

        df_scores = df_simulated_leaderboard.set_index("model_pk")
        return (
            df_scores[f"{primary_scoring_func.__name__}_dataset"].rename(
                f"{SIM_BOOTSTRAP_COL_PREFIX}_{idx}"
            ),
            df_scores[f"{primary_scoring_func.__name__}_market"].rename(
                f"{SIM_BOOTSTRAP_COL_PREFIX}_{idx}"
            ),
            df_scores[f"{primary_scoring_func.__name__}_overall"].rename(
                f"{SIM_BOOTSTRAP_COL_PREFIX}_{idx}"
            ),
        )

    logger.info(f"Simulating using {env.NUM_CPUS} CPU(s).")
    results = Parallel(
        n_jobs=env.NUM_CPUS,
        backend="loky",
        verbose=0,
        batch_size="auto",
    )(delayed(bootstrap_and_score)(i) for i in range(N))

    df_simulated_scores_dataset = pd.concat([r[0] for r in results], axis=1)
    df_simulated_scores_market = pd.concat([r[1] for r in results], axis=1)
    df_simulated_scores_overall = pd.concat([r[2] for r in results], axis=1)
    logger.info("Done creating df_simulated_scores!")

    assert (
        df_simulated_scores_dataset.shape[1] == df_simulated_scores_market.shape[1]
        and df_simulated_scores_overall.shape[1] == df_simulated_scores_market.shape[1]
    ), "Assertion failed: Check simulation in `generate_simulated_leaderboards`."
    logger.info(f"Simulated {df_simulated_scores_overall.shape[1]} / {N}.")
    return df_simulated_scores_dataset, df_simulated_scores_market, df_simulated_scores_overall


def get_confidence_interval(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores: pd.DataFrame,
    question_type: str,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
    method: str = "percentile",
    show_histograms: bool = False,
) -> pd.DataFrame:
    """Calculate confidence intervals for leaderboard scores.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores (pd.DataFrame): Bootstrapped replicates of overall scores.
        question_type (str): one of "dataset", "market", "overall"
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]): Function to compute the
                     primary overall score.
        method (str): CI calculation method, either 'percentile' or 'bca'.
        show_histograms (bool): Whether to display simulated score histograms.

    Returns:
        pd.DataFrame: Leaderboard with added lower and upper CI columns.
    """
    logger.info(colored("Calculating CIs", "red"))
    if question_type not in ["dataset", "market", "overall"]:
        raise ValueError(f"question type {question_type} not valid.")

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
        theta_hat = df_leaderboard[f"{primary_scoring_func.__name__}_{question_type}"].values
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

    col_prefix = f"{primary_scoring_func.__name__}_{question_type}"
    df_leaderboard[f"{col_prefix}_ci_lower"] = df_leaderboard["model_pk"].map(lower).values
    df_leaderboard[f"{col_prefix}_ci_upper"] = df_leaderboard["model_pk"].map(upper).values
    return df_leaderboard


def get_comparison_p_val_col(comparison: dict) -> str:
    """Return the column name returned by `get_comparison_p_val()`.

    Args:
        comparison (dict): A dictionary containing model comparison details

    Returns:
        str: The column name
    """
    return f"p_value_one_sided_{comparison['model']}"


def get_comparison_model_index(df: pd.DataFrame, comparison: dict) -> int:
    """
    Return the index of a row in a DataFrame matching comparison.

    Args:
        df (pandas.DataFrame): DataFrame containing model data with 'model', 'organization', and
            'model_organization' columns.
        comparison (dict): Dictionary with 'model', 'organization', and 'model_organization' keys
            specifying the model to find.

    Returns:
        int: The index of the matching row in the DataFrame.
    """
    comparison_mask = (
        (df["model"] == comparison["model"])
        & (df["organization"] == comparison["organization"])
        & (df["model_organization"] == comparison["model_organization"])
    )
    idx = df.index[comparison_mask]
    if len(idx) != 1:
        raise ValueError(f"Error with provided comparison model: {comparison}.")
    return idx[0]


def get_comparison_p_val(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores: pd.DataFrame,
    comparison: dict,
    is_centered: bool = False,
    bh_adjust_p_vals: bool = False,
) -> pd.DataFrame:
    """Compute one-sided p-values comparing each model to the human comparison groups.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores (pd.DataFrame): Bootstrapped replicates of overall scores.
        comparison (dict): dict showing model to use for comparison.
        is_centered (bool): Center p-value calculation on observed score differences.
        bh_adjust_p_vals (bool): Apply Benjamini-Hochberg adjustment if True.

    Returns:
        pd.DataFrame: Leaderboard with updated p_value column.
    """
    if not comparison:
        raise ValueError("Must provide comparison")
    logger.info(colored(f"Comparing to {comparison['model']}", "red"))

    overall_score_col = "two_way_fixed_effects_overall"
    if overall_score_col not in df_leaderboard.columns:
        raise ValueError(
            f"Metric {overall_score_col} not found in leaderboard DataFrame."
            "This function only works for scoring methods where lower scores are "
            "better. Adjust if testing for scoring methods where higher is better."
        )

    out_col = get_comparison_p_val_col(comparison)
    comparison_idx = get_comparison_model_index(df=df_leaderboard, comparison=comparison)
    comparison_model_pk = df_leaderboard.loc[comparison_idx, "model_pk"]
    comparison_mean_score = df_leaderboard.loc[comparison_idx, overall_score_col]

    sim_comparison_scores = df_simulated_scores.loc[comparison_model_pk]

    if is_centered:
        observed_diffs = comparison_mean_score - df_leaderboard[overall_score_col]
        observed_diff_dict = dict(zip(df_leaderboard["model_pk"], observed_diffs))
        p_value_one_sided = {
            model_pk: np.mean(
                (
                    (sim_comparison_scores.values - sim_comp_scores.values)
                    - observed_diff_dict[model_pk]
                )
                <= observed_diff_dict[model_pk]
            )
            for model_pk, sim_comp_scores in df_simulated_scores.iterrows()
        }
    else:
        comparison_df = df_simulated_scores.le(sim_comparison_scores, axis=1)
        p_value_one_sided = comparison_df.mean(axis=1)

    df_leaderboard[out_col] = df_leaderboard["model_pk"].map(p_value_one_sided)
    df_leaderboard.loc[comparison_idx, out_col] = -1

    if bh_adjust_p_vals:
        # P-value adjustment for multiple tests to avoid the multiple comparisons problem.
        # Drop best row for p-value adjustment
        mask = df_leaderboard.index != comparison_idx
        _, bh_adj_pvals, _, _ = multipletests(
            pvals=df_leaderboard.loc[mask, out_col],
            alpha=0.05,
            method="fdr_bh",
        )
        df_leaderboard.loc[mask, f"{out_col}_bh_adj"] = bh_adj_pvals
        df_leaderboard.loc[comparison_idx, f"{out_col}_bh_adj"] = -1

    if comparison == HUMAN_PUBLIC:
        # Switch directions for the one-sided test for the general public as LLMs have already
        # surpassed them.
        df_leaderboard[out_col] = 1 - df_leaderboard[out_col]

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


def rescale_difficulty_adjusted_brier(
    df_leaderboard: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Rescale scores such that Always 0.5 has an average score of 0.25.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]):
            Function used to compute the primary overall score.

    Returns:
        pd.DataFrame: Leaderboard with updated scores.
    """
    columns_to_rescale = [
        f"{primary_scoring_func.__name__}_dataset",
        f"{primary_scoring_func.__name__}_market",
        f"{primary_scoring_func.__name__}_overall",
    ]

    for col in columns_to_rescale:
        always_0p5_score = df_leaderboard.loc[
            (df_leaderboard["organization"] == ALWAYS_05_MODEL["organization"])
            & (df_leaderboard["model"] == ALWAYS_05_MODEL["model"]),
            col,
        ].iloc[0]
        df_leaderboard[col] += 0.25 - always_0p5_score

    return df_leaderboard


def get_x_pct_oracle_model_name(pct: float):
    """
    Get the name of the pct forecaster.

    Args:
        pct (float): the x% of `add_x_pct_oracles`.

    Returns:
        str: the model name associated with `pct`.
    """
    return f"{round(pct*100, 1)}% forecaster"


def get_x_pct_oracle_increments() -> List[float]:
    """
    Get the increments for the x% oracles.

    Args:
        None.

    Returns:
        List[float]: the threshholds for the x% oracles.
    """
    return [round(i * 0.005, 3) for i in range(201)]


def add_x_pct_oracles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add x% oracles to the combined forecast set.

    Args:
        df (pd.DataFrame): Combined forecast set.

    Returns:
        pd.DataFrame: Combined forecast set with x% oracles.
    """
    # Copy a model that has forecast on every question
    dummy_model_to_copy = BASELINE_ORG_NAIVE_MODEL

    df_dummy = df[
        (df["organization"] == dummy_model_to_copy["organization"])
        & (df["model"] == dummy_model_to_copy["model"])
    ].reset_index(drop=True)

    for pct in get_x_pct_oracle_increments():
        x_pct_oracle = df_dummy.copy()
        x_pct_oracle["model"] = get_x_pct_oracle_model_name(pct)
        x_pct_oracle["organization"] = constants.BENCHMARK_NAME
        x_pct_oracle["model_organization"] = constants.BENCHMARK_NAME
        x_pct_oracle = set_model_pk(df=x_pct_oracle)
        x_pct_oracle["forecast"] = -1.0
        x_pct_oracle.loc[x_pct_oracle["resolved_to"] == 1, "forecast"] = pct
        x_pct_oracle.loc[x_pct_oracle["resolved_to"] == 0, "forecast"] = 1 - pct
        df_unset = x_pct_oracle[x_pct_oracle["forecast"] < 0]
        if not df_unset.empty:
            logger.warning(
                df_unset[
                    [
                        "forecast_due_date",
                        "model",
                        "forecast",
                        "resolved",
                        "source",
                        "id",
                        "resolved_to",
                    ]
                ]
            )
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", None)

            for _, row in df_unset.iterrows():
                print()
                print(row.to_string())

            raise ValueError("One of the resolved_to values was not 0 or 1")

        df = pd.concat([df, x_pct_oracle], ignore_index=True)

    return df


def remove_x_pct_oracles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the x% forecasters from the dataframe.

    Args:
        df (pd.DataFrame): The combined forecast set or the leaderboard.

    Returns:
        pd.DataFrame: The same dataframe without the x% forecasters.
    """
    org_mask = df["organization"] == constants.BENCHMARK_NAME

    x_pct_oracle_models = [
        get_x_pct_oracle_model_name(pct) for pct in get_x_pct_oracle_increments()
    ]
    x_pct_oracle_mask = df["model"].isin(x_pct_oracle_models)

    mask = org_mask & x_pct_oracle_mask
    return df[~mask].reset_index(drop=True)


def get_x_pct_oracle_equivalent(
    df_leaderboard: pd.DataFrame,
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Set the value for the pct forecaster equivalent column.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]):
            Function used to compute the primary overall score.

    Returns:
        pd.DataFrame: Leaderboard with pct forecaster equivalent column
    """
    sorting_col = f"{primary_scoring_func.__name__}_overall"
    df_leaderboard["x_pct_oracle_equivalent"] = -1.0

    for pct in get_x_pct_oracle_increments():
        x_pct_model_name = get_x_pct_oracle_model_name(pct)
        x_pct_ref_model = df_leaderboard[
            (df_leaderboard["organization"] == constants.BENCHMARK_NAME)
            & (df_leaderboard["model"] == x_pct_model_name)
        ]
        if x_pct_ref_model.empty:
            raise ValueError(f"Problem finding x% model for {x_pct_model_name}, using `pct={pct}`.")
        threshold = x_pct_ref_model[sorting_col].iloc[0]
        df_leaderboard.loc[df_leaderboard[sorting_col] <= threshold, "x_pct_oracle_equivalent"] = (
            pct
        )

    df_leaderboard["x_pct_oracle_equivalent"] = (
        np.ceil(df_leaderboard["x_pct_oracle_equivalent"] * 100) / 100
    )

    df_unset = df_leaderboard[df_leaderboard["x_pct_oracle_equivalent"] < 0]
    if not df_unset.empty:
        logger.warning(df_unset)
        raise ValueError("Unable to set the x% oracle equivalents for the above models.")

    return df_leaderboard


def get_model_release_date_info(
    df: pd.DataFrame,
    days_since_release: bool = True,
    model_release_date: bool = False,
) -> pd.DataFrame:
    """Add the requested column(s) related to the model release date.

    Args:
        df (pd.DataFrame): Combined forecast set.
        days_since_release (bool, optional): If True, adds the 'days_since_model_release' column
            with the number of days between the model release date and forecast due date.
            Defaults to True.
        model_release_date (bool, optional): If True, includes the 'model_release_date' column in
            the output. Defaults to False.

    Returns:
        pd.DataFrame: The input DataFrame with additional columns 'days_since_model_release'
            (if `days_since_release` is True) and/or 'model_release_date' (if `model_release_date`
            is True). Rows with missing release dates are excluded.
    """
    df = df.copy()
    cols_to_return = df.columns.tolist()

    df_with_release_dates = pd.merge(
        df,
        df_release_dates,
        how="inner",
        on="model",
    )

    # Send a message to Slack if models were dropped from the df because their release date was
    # missing in `df_release_dates`. Prefer this to stopping processing.
    outer = pd.merge(
        df,
        df_release_dates,
        how="outer",
        on="model",
        indicator=True,
    )
    dropped_models = sorted(outer.loc[outer["_merge"] == "left_only", "model"].unique())
    if dropped_models:
        slack.send_message(
            "\n*Models dropped from consideration in 2wfe estimation:*\n```"
            + "\n".join(dropped_models)
            + "```"
        )

    if model_release_date:
        cols_to_return += ["model_release_date"]

    if days_since_release:
        cols_to_return += ["days_since_model_release"]
        df_with_release_dates["days_since_model_release"] = (
            pd.to_datetime(df_with_release_dates["forecast_due_date"])
            - pd.to_datetime(df_with_release_dates["model_release_date"])
        ).dt.days

    return df_with_release_dates[cols_to_return].reset_index(drop=True)


def find_sota_models(
    df: pd.DataFrame,
    bootstrap_col: str,
) -> pd.DataFrame:
    """Identify SOTA models for a given bootstrap column, allowing co-SOTAs.

    Args:
        df (pd.DataFrame): Simulated data for models to consider, sorted by model_release_date
            with nan model_release_dates dropped
        bootstrap_col (str): Name of the column holding scores for a single bootstraps
            iteration (e.g., 'bootstrap_0').

    Returns:
        pd.DataFrame: Subset of rows corresponding to SOTA/co-SOTA models with columns
            ['model_pk', 'model_release_date', bootstrap_col], sorted by release date.
            Rows with missing release dates or scores are excluded.
    """
    cols = ["model_pk", "model", "model_release_date", bootstrap_col]
    if "model_release_date_ordinal" in df.columns:
        cols += ["model_release_date_ordinal"]
    df = df[cols].copy()
    best = np.inf
    tol = 1e-12
    chosen_idx = []
    for _, g in df.groupby("model_release_date", sort=True):
        g_sorted = g.sort_values([bootstrap_col, "model"], ascending=[True, True])
        best_of_day = g_sorted.iloc[0][bootstrap_col]
        if best_of_day < best - tol:
            chosen_idx.append(g_sorted.index[0])
            best = best_of_day

    return df.loc[chosen_idx, cols].sort_values("model_release_date").reset_index(drop=True)


def calculate_sota_super_intersection_date(
    df: pd.DataFrame,
    superforecaster_median: float,
    bootstrap_col: str,
) -> Union[float, NaTType]:
    """Estimate when the SOTA trend intersects the superforecaster median.

    Args:
        df (pd.DataFrame): DataFrame containing 'model_release_date' and the given
            `bootstrap_col`. Rows with missing values are ignored implicitly by NumPy.
        superforecaster_median (float): The superforecaster benchmark score to intersect.
        bootstrap_col (str): Name of the score column to regress (e.g., 'bootstrap_0').

    Returns:
        Union[float, pd.NaT]: The intersection expressed as an ordinal day number if
            finite, otherwise `pd.NaT`. To convert an ordinal to a calendar date, use:
            `pd.Timestamp.fromordinal(int(round(ordinal))).normalize()`.
    """
    if df.empty:
        return pd.NaT

    x = df["model_release_date_ordinal"].to_numpy(dtype=float)
    y = df[bootstrap_col].to_numpy(dtype=float)
    if x.size < 3 or np.unique(x).size < 3:
        return pd.NaT

    xm = x.mean()
    ym = y.mean()
    xc = x - xm
    yc = y - ym
    denom = np.dot(xc, xc)
    if denom == 0:
        return pd.NaT

    m = np.dot(xc, yc) / denom
    if not np.isfinite(m) or m >= 0:
        return pd.NaT

    b = ym - m * xm
    xi = (superforecaster_median - b) / m
    return float(xi) if np.isfinite(xi) else pd.NaT


def summarize_parity_dates(all_dates: dict) -> dict:
    """Summarize bootstrap parity date samples into a 95% interval and median per group.

    Args:
        all_dates (dict[str, dict[object, Sequence[float]]]): Mapping from
            question type to leaderboard to a sequence of ordinal day numbers.

    Returns:
        dict[str, dict[object, Optional[dict[str, str]]]]: For each question type
            and leaderboard, either None (no finite samples) or a dict with:
            {'lower': 'Mon YYYY', 'median': 'Mon YYYY', 'upper': 'Mon YYYY'}.

    Notes:
        - Ordinals are rounded to the nearest day before formatting.
        - Percentiles are computed via np.quantile at [0.025, 0.5, 0.975].
    """

    def fmt(ordinal: float) -> str:
        return pd.Timestamp.fromordinal(int(round(ordinal))).strftime("%B %Y")

    retval = {}
    for question_type, leaderboards in all_dates.items():
        retval[question_type] = {}
        for leaderboard, super_parity_dates in leaderboards.items():
            a = np.asarray(super_parity_dates, dtype=float)
            a = a[np.isfinite(a)]
            logger.info(f"Summarizing parity date {question_type} {leaderboard} {len(a)}")
            if a.size == 0:
                logger.error(
                    colored(
                        f"PROBLEM CALCULATING PARITY DATE for {question_type}, {leaderboard}", "red"
                    )
                )
                retval[question_type][leaderboard] = None
                continue
            q = np.quantile(a, [0.025, 0.5, 0.975])
            retval[question_type][leaderboard] = {
                "lower": fmt(q[0]),
                "median": fmt(q[1]),
                "upper": fmt(q[2]),
            }
    return retval


def get_sota_super_parity_expected_dates(
    df_leaderboard: pd.DataFrame,
    df_simulated_scores_dataset: pd.DataFrame,
    df_simulated_scores_market: pd.DataFrame,
    df_simulated_scores_overall: pd.DataFrame,
) -> dict:
    """Compute LLM–superforecaster parity dates per question type and leaderboard.

    Args:
        df_leaderboard (pd.DataFrame): Leaderboard.
        df_simulated_scores_dataset (pd.DataFrame): Simulated scores for dataset questions.
        df_simulated_scores_market (pd.DataFrame): Simulated scores for market questions.
        df_simulated_scores_overall (pd.DataFrame): Simulated scores for overall questions.

    Returns:
        Dict[str, Dict[LeaderboardType, List[float]]]: Mapping from question type
            ('dataset' | 'market' | 'overall') to leaderboard type (LeaderboardType)
            to a list of intersection dates expressed as ordinal day numbers. Lists may
            be empty when an intersection cannot be estimated for a bootstrap.
    """
    logger.info("Get SOTA LLM Super parity dates.")
    df_leaderboard = df_leaderboard.copy()

    # Join dates to simulated output
    if "model_release_date" not in df_leaderboard.columns:
        df_leaderboard = get_model_release_date_info(
            df=df_leaderboard,
            days_since_release=False,
            model_release_date=True,
        )

    df_model = df_leaderboard[
        [
            "organization",
            "model_organization",
            "model_pk",
            "model",
            "model_release_date",
        ]
    ].drop_duplicates(ignore_index=True)
    df_model = df_model[df_model["organization"] == constants.BENCHMARK_NAME].reset_index(drop=True)
    df_model["release_date"] = pd.to_datetime(df_model["model_release_date"]).dt.date

    dataframes = {
        "dataset": df_simulated_scores_dataset.copy(),
        "market": df_simulated_scores_market.copy(),
        "overall": df_simulated_scores_overall.copy(),
    }
    for key in dataframes.keys():
        df_tmp = pd.merge(
            dataframes[key],
            df_model,
            on="model_pk",
            how="inner",
        )

        df_tmp["model_release_date_ordinal"] = np.nan
        df_model_release_date_datetime = pd.to_datetime(
            df_tmp["model_release_date"], errors="coerce"
        )
        mask = df_model_release_date_datetime.notna()
        df_tmp.loc[mask, "model_release_date_ordinal"] = df_model_release_date_datetime[mask].map(
            pd.Timestamp.toordinal
        )
        cols_to_keep = list(dataframes[key].columns) + [
            "organization",
            "model_organization",
            "model",
            "model_pk",
            "model_release_date",
            "model_release_date_ordinal",
        ]
        dataframes[key] = df_tmp.sort_values("model_release_date", ignore_index=True)[cols_to_keep]

    def prep_df_to_find_sota_models(df: pd.DataFrame):
        super_mask = (
            (df["model"] == HUMAN_SUPERFORECASTER["model"])
            & (df["organization"] == HUMAN_SUPERFORECASTER["organization"])
            & (df["model_organization"] == HUMAN_SUPERFORECASTER["model_organization"])
        )
        df_super = df.loc[super_mask]
        if df_super.shape[0] != 1:
            raise ValueError("Could not find supers in simulated data")
        return df.dropna(subset=["model_release_date"], ignore_index=True), df_super

    # Compile LLM-Super parity dates
    question_types = dataframes.keys()
    retval = {
        question_type: {leaderboard_type.value: None for leaderboard_type in LeaderboardType}
        for question_type in question_types
    }

    for question_type in question_types:
        # Get current last SOTA release date
        df_leaderboard_prepped, _ = prep_df_to_find_sota_models(df=df_leaderboard)
        df_leaderboard_sota_models = find_sota_models(
            df=df_leaderboard_prepped,
            bootstrap_col=f"two_way_fixed_effects_{question_type}",
        )
        leaderboard_last_sota_release_date_ordinal = float(
            pd.to_datetime(df_leaderboard_sota_models["model_release_date"]).max().toordinal()
        )
        for leaderboard_type in LeaderboardType:
            if question_type == "dataset" and leaderboard_type == LeaderboardType.TOURNAMENT:
                # For dataset questions, the FB tournament models just repeat the forecasts from,
                # the FB baseline models, so just copy those results over at the end.
                break
            df_sim_data = (
                remove_tournament_models(df=dataframes[question_type])
                if leaderboard_type == LeaderboardType.BASELINE
                else dataframes[question_type]
            )
            bootstrap_cols = [
                c for c in df_sim_data.columns if c.startswith(SIM_BOOTSTRAP_COL_PREFIX)
            ]
            superforecaster_parity_dates = []
            df_sim_data_prepped, df_sim_data_super = prep_df_to_find_sota_models(df=df_sim_data)
            for col in bootstrap_cols:
                df_sota_models = find_sota_models(
                    df=df_sim_data_prepped,
                    bootstrap_col=col,
                )
                df_sim_data_super_boot = df_sim_data_super[col]
                if df_sim_data_super_boot.shape[0] != 1:
                    raise ValueError("Could not find supers in simulated data")
                superforecaster_median = float(df_sim_data_super_boot.iloc[0])
                intersection_date_ordinal = calculate_sota_super_intersection_date(
                    df=df_sota_models,
                    superforecaster_median=superforecaster_median,
                    bootstrap_col=col,
                )
                if isinstance(intersection_date_ordinal, float) and np.isfinite(
                    intersection_date_ordinal
                ):
                    if intersection_date_ordinal > leaderboard_last_sota_release_date_ordinal:
                        superforecaster_parity_dates.append(intersection_date_ordinal)
                elif pd.isna(intersection_date_ordinal):
                    logger.warning(f"Skipping invalid intersection_date_ordinal (pd.NaT) for {col}")
                else:
                    logger.warning(
                        "Unexpected intersection_date_ordinal type: "
                        f"{type(intersection_date_ordinal)} for {col}"
                    )

            if not superforecaster_parity_dates:
                slack.send_message("*PROBLEM CALCULATING LLM PARITY DATES*")
            retval[question_type][leaderboard_type] = superforecaster_parity_dates

    retval["dataset"][leaderboard_type.TOURNAMENT] = retval["dataset"][leaderboard_type.BASELINE]
    logger.info("Done getting SOTA LLM Super parity dates.")
    return summarize_parity_dates(retval)


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
    df = get_model_release_date_info(
        df=df,
        days_since_release=True,
        model_release_date=False,
    )
    df = add_x_pct_oracles(df=df)

    # The scoring functions to consider
    primary_scoring_func = two_way_fixed_effects
    scoring_funcs = [
        primary_scoring_func,
        peer_score,
        brier_skill_score,
    ]

    # Score
    df_leaderboard, question_fixed_effects = score_models(
        df=df,
        scoring_funcs=scoring_funcs,
    )

    # x% oracle equivalent
    df_leaderboard = get_x_pct_oracle_equivalent(
        df_leaderboard=df_leaderboard,
        primary_scoring_func=primary_scoring_func,
    )

    # Remove x% oracles
    df = remove_x_pct_oracles(df=df)
    df_leaderboard = remove_x_pct_oracles(df=df_leaderboard)

    # Get simulated scores
    df_simulated_scores_dataset, df_simulated_scores_market, df_simulated_scores_overall = (
        generate_simulated_leaderboards(
            df=df,
            primary_scoring_func=primary_scoring_func,
            N=N_REPLICATES,
        )
    )

    # CIs
    df_leaderboard = get_confidence_interval(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_dataset,
        question_type="dataset",
        primary_scoring_func=primary_scoring_func,
    )
    df_leaderboard = get_confidence_interval(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_market,
        question_type="market",
        primary_scoring_func=primary_scoring_func,
    )
    df_leaderboard = get_confidence_interval(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_overall,
        question_type="overall",
        primary_scoring_func=primary_scoring_func,
    )

    # Explore SOTA graph trend line
    llm_super_parity_dates = get_sota_super_parity_expected_dates(
        df_leaderboard=df_leaderboard,
        df_simulated_scores_dataset=df_simulated_scores_dataset,
        df_simulated_scores_market=df_simulated_scores_market,
        df_simulated_scores_overall=df_simulated_scores_overall,
    )

    # Compare to human models
    for comparison in HUMAN_MODELS:
        df_leaderboard = get_comparison_p_val(
            df_leaderboard=df_leaderboard,
            df_simulated_scores=df_simulated_scores_overall,
            comparison=comparison,
        )

    # Simulation performance measures
    df_leaderboard = get_simulation_performance_metrics(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_overall,
    )

    # Write question fixed effects
    write_question_fixed_effects(
        question_fixed_effects=question_fixed_effects,
    )

    # Write LLM Super parity dates
    write_llm_super_parity_dates(
        parity_dates=llm_super_parity_dates,
    )

    # Write leaderboard
    df_leaderboard = get_model_release_date_info(
        df=df_leaderboard,
        days_since_release=False,
        model_release_date=True,
    )
    for leaderboard_type in LeaderboardType:
        df_leaderboard_lt = (
            remove_tournament_models(df=df_leaderboard)
            if leaderboard_type == LeaderboardType.BASELINE
            else df_leaderboard
        )
        write_leaderboard(
            df=df_leaderboard_lt,
            primary_scoring_func=primary_scoring_func,
            leaderboard_type=leaderboard_type,
        )


def download_and_compile_processed_forecast_files(bucket: str) -> List[pd.DataFrame]:
    """Download and compile processed forecast files into entries list.

    Args:
        None

    Returns:
        List[pd.DataFrame]: List of DataFrames for each processed forecast file,
            ready for leaderboard aggregation.
    """
    forecast_files, valid_dates = resolution.get_valid_forecast_files_and_dates(bucket=bucket)
    forecast_files, valid_dates = filter_forecast_files_by_forecast_due_date(
        forecast_files=forecast_files,
        valid_dates=valid_dates,
    )
    # forecast_files = [f for f in forecast_files if f.startswith("2024")]
    logger.info(f"Processing forecast due dates: {valid_dates}.")
    local_forecast_set_dir = data_utils.get_local_file_dir(bucket=bucket)
    leaderboard_entries = []
    for f in forecast_files:
        data = resolution.read_forecast_file(filename=f"{local_forecast_set_dir}/{f}")
        if data is None:
            continue

        organization = data.get("organization")
        model = data.get("model")
        model_organization = data.get("model_organization")
        forecast_due_date = data.get("forecast_due_date")
        df = data.get("df")

        process_forecast_file(
            leaderboard_entries=leaderboard_entries,
            org_and_model={
                "organization": organization,
                "model": model,
                "model_organization": model_organization,
            },
            df=df,
            forecast_due_date=forecast_due_date,
        )

    return leaderboard_entries, valid_dates


@decorator.log_runtime
def driver(_: Any) -> None:
    """Create a new leaderboard.

    Args:
        _ (Any): Unused placeholder argument for GCP Cloud Run Job.

    Returns:
        None: Exits the process on completion.
    """
    logger.info(colored("Making leaderboards.", "red"))
    leaderboard_entries, valid_dates = download_and_compile_processed_forecast_files(
        bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
    )
    make_leaderboard(leaderboard_entries=leaderboard_entries)
    logger.info(f"Made leaderboard for forecast due dates: {sorted(valid_dates)}.")
    logger.info(colored("Done.", "red"))


if __name__ == "__main__":
    driver(None)
