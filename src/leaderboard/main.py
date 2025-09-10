"""Create leaderboard."""

import inspect
import json
import logging
import os
import shutil
import sys
import traceback
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import pyfixest as pf
from jinja2 import Template
from joblib import Parallel, delayed
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
    keys,
    question_curation,
    resolution,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LeaderboardType(str, Enum):
    """Enumeration of leaderboard types.

    This enum distinguishes between the two supported leaderboard variants:
    * BASELINE: The baseline leaderboard: FB forecast files w/o freeze values.
                Used when CLOUD_RUN_TASK_INDEX == 0.
    * TOURNAMENT: The tournament leaderboard. All forecast files.
                  Used when CLOUD_RUN_TASK_INDEX == 1.
    """

    BASELINE = "baseline"
    TOURNAMENT = "tournament"


try:
    env_var = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
    TASK_NUMBER = int(env_var)
    LEADERBOARD_TO_CREATE = list(LeaderboardType)[TASK_NUMBER]
except (ValueError, IndexError) as e:
    logger.error(
        f"Improperly set environment variable: CLOUD_RUN_TASK_INDEX = {env_var}"
        f"Valid values are in [0, {len(list(LeaderboardType))-1}]."
    )
    logger.error(e)
    sys.exit(0)


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
HUMAN_GENERAL_PUBLIC = {
    "organization": constants.BENCHMARK_NAME,
    "model": "Public median forecast",
    "model_organization": constants.BENCHMARK_NAME,
}

HUMAN_MODELS_TO_HIGHLIGHT = [HUMAN_SUPERFORECASTER["model"], HUMAN_GENERAL_PUBLIC["model"]]

LEADERBOARD_DECIMAL_PLACES = 3

IMPUTED_CUTOFF_PCT = 5

MODEL_RELEASE_DATE_CUTOFF = 365

N_REPLICATES = 1999 if not env.RUNNING_LOCALLY else 2

df_release_dates = pd.read_csv("model_release_dates.csv")
df_release_dates["release_date"] = pd.to_datetime(df_release_dates["release_date"], errors="coerce")


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
    "95% CI": "Bootstrapped 95% confidence interval for the Overall score.",
    "P-value to best": (
        "One-sided p-value comparing each model to the top-ranked model based on "
        f"{N_REPLICATES:,} simulations, with<br>"
        "H₀: This model performs at least as well as the top-ranked model.<br>"
        "H₁: The top-ranked model outperforms this model."
    ),
    "Pct times № 1": (
        f"Percentage of {N_REPLICATES:,} simulations in which this model was the best " "performer."
    ),
    "Pct times top 5%": (
        f"Percentage of {N_REPLICATES:,} simulations in which this model ranked in the top 5%."
    ),
    "x% oracle equiv": (
        "This model is most similar to a reference model that forecasts x% when the question "
        "resolves to 1 and (1-x)% when the question resolved to 0. x moves in increments of 1 "
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


def set_model_pk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the model primary key.

    Args:
        df (pd.DataFrame): Forecast set.

    Returns:
        df (pd.DataFrame): Forecast set with `model_pk` field.
    """
    df["model_pk"] = df["organization"] + "_" + df["model"]
    return df


def filter_ForecastBench_freeze_value_file(org_and_model: Dict[str, str]) -> bool:
    """Process a forecast file based on the leaderboard type and model criteria.

    This function filters forecast files as we're creating two leaderboards:
    * Baseline Leaderboard: FB files w/o freeze values
    * Tournament Leaderboard: all forecast files

    Args:
        org_and_model (Dict[str, str]): Dictionary containing 'organization' and 'model' keys
                                       identifying the forecast source.

    Returns:
        bool: True if the file should be filtered out:
              * filter if this is the Baseline leaderboard and
                * this is not a ForecastBench file
                * this is a ForecastBench file with freeze values
              * don't filter otherwise

    Raises:
        Exception: If CLOUD_RUN_TASK_INDEX environment variable is improperly formatted.
    """
    if LEADERBOARD_TO_CREATE == LeaderboardType.TOURNAMENT:
        return False

    if org_and_model["organization"] != constants.BENCHMARK_NAME:
        return True

    if any(
        x in org_and_model["model"] for x in ("with freeze values", "with news", "with SECOND news")
    ):
        return True

    return False


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

    if filter_ForecastBench_freeze_value_file(org_and_model):
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
    df["model_organization"] = org_and_model["model_organization"]
    df = set_model_pk(df)

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


def write_leaderboard_html_file(df: pd.DataFrame, sorting_column_number: int) -> None:
    """Generate HTML and CSV leaderboard files and upload to Bucket & git repo.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.
        sorting_column_number (int): column to sort by.

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
        <th>95% CI</th>
        <th>P-value to best</th>
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
          if (name==='P-value to best') {
            col.title = 'P-value to best <i class="info circle icon" '
                        + ' data-html="{{ col_desc["P-value to best"] }}"></i>';
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
          if (name==='95% CI') {
            col.title = '95% CI <i class="info circle icon" ' +
                        'data-html="{{ col_desc["95% CI"] }}"></i>';
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

    basename = f"leaderboard_{LEADERBOARD_TO_CREATE.value}"
    local_filename_html = f"/tmp/{basename}.html"
    with open(local_filename_html, "w", encoding="utf-8") as f:
        f.write(html)

    local_filename_csv = f"/tmp/{basename}.csv"
    df.to_csv(local_filename_csv, index=False)

    upload_leaderboard(
        files={
            local_filename_html: f"{basename}.html",
            local_filename_csv: f"{basename}.csv",
        }
    )


def write_leaderboard_js_file_full(df: pd.DataFrame) -> Dict[str, str]:
    """Generate JS file for website Leaderboard page.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.

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
                          "Overall", "N", "95% CI", "P-value to best",
                          "Pct times № 1", "Pct times top 5%", "x% oracle equiv",
                          "Peer", "BSS"];
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

                if (["N dataset", "N market", "N", "Model Organization Logo"].includes(name)) {
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

                if (name === "P-value to best" || name === "x% oracle equiv") col.orderable = false;

                if (name === "Pct times № 1") {
                  col.render = (d, t) => (t === "display" ? Math.round(d) + "%" : d);
                  col.orderSequence = ["desc", "asc"];
                }

                if (name === "Pct times top 5%") {
                  col.render = (d, t) => (t === "display" ? Math.round(d) + "%" : d);
                  col.orderSequence = ["desc", "asc"];
                }

                if (["Dataset 95% CI", "Market 95% CI", "95% CI"].includes(name)) {
                  col.orderable = false;
                }

                if (name === "Peer" || name === "BSS") {
                  col.render = (d, t) => (t === "display" ? parseFloat(d).toFixed(3) : d);
                  col.orderSequence = ["desc", "asc"];
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
                   <th class="column-header-tooltip" data-tooltip="95% CI">95% CI</th>
                   <th class="column-header-tooltip" data-tooltip="P-value to best">P-value to best</th>
                   <th class="column-header-tooltip" data-tooltip="Pct times № 1">Pct times № 1</th>
                   <th class="column-header-tooltip" data-tooltip="Pct times top 5%">Pct times top 5%</th>
                   <th class="column-header-tooltip" data-tooltip="x% oracle equiv">x% oracle equiv</th>
                   <th class="column-header-tooltip" data-tooltip="Peer">Peer</th>
                   <th class="column-header-tooltip" data-tooltip="BSS">BSS</th>
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
          '95% CI': `{{ col_desc["95% CI"] }}`,
          'P-value to best': `{{ col_desc["P-value to best"] }}`,
          'Pct times № 1': `{{ col_desc["Pct times № 1"] }}`,
          'Pct times top 5%': `{{ col_desc["Pct times top 5%"] }}`,
          'x% oracle equiv': `{{ col_desc["x% oracle equiv"] }}`,
          'Peer': `{{ col_desc["Peer"] }}`,
          'BSS': `{{ col_desc["BSS"] }}`
        };"""
    )

    js = template.render(
        data=df.to_dict(orient="records"),
        last_updated_date=LAST_UPDATED_DATE,
        model_highlight_rows=HUMAN_MODELS_TO_HIGHLIGHT,
        col_desc=TOOLTIP_COLUMN_DESCRIPTIONS,
        include_team = leaderboard_type != LeaderboardType.BASELINE,
    )

    return {
        "filename": f"leaderboard_{LEADERBOARD_TO_CREATE.value}_full.js",
        "js": js,
    }


def write_leaderboard_js_file_compact(df: pd.DataFrame) -> Dict[str, str]:
    """Generate JS file for website Home page.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.

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
            // Initialize tooltips after table is created
            initializeTooltips();
          });
        // Tooltip content object (defined globally for access)
        const tooltipContent = {
          'Organization': `{{ col_desc["Model Organization"] }}`,
          'Model': `{{ col_desc["Model"] }}`,
          'Overall': `{{ col_desc["Overall"] }}`
        };
        })();"""
    )

    js = template.render(
        data=df[
            ["Rank", "Model Organization", "Model Organization Logo", "Model", "Overall"]
        ].to_dict(orient="records"),
        last_updated_date=LAST_UPDATED_DATE,
        model_highlight_rows=HUMAN_MODELS_TO_HIGHLIGHT,
        col_desc=TOOLTIP_COLUMN_DESCRIPTIONS,
        leaderboard_type=LEADERBOARD_TO_CREATE.value,
    )

    return {
        "filename": f"leaderboard_{LEADERBOARD_TO_CREATE.value}_compact.js",
        "js": js,
    }


def write_leaderboard_js_files(df) -> None:
    """Wrap functions to create JS files for website.

    Args:
        df (pd.DataFrame): DataFrame containing the leaderboard.

    Returns:
        None.
    """
    df = df.copy()
    df["Model Organization Logo"] = df["Model Organization"].map(constants.ORG_TO_LOGO).fillna("")
    df["Team"] = df["Team"].apply(lambda x: constants.ORG_TO_LOGO.get(x, x))

    leaderboards = [
        write_leaderboard_js_file_compact(df=df),
        write_leaderboard_js_file_full(df=df),
    ]
    for leaderboard in leaderboards:
        local_filename = f"/tmp/{leaderboard['filename']}"
        with open(local_filename, "w", encoding="utf-8") as f:
            f.write(leaderboard["js"])

        if not env.RUNNING_LOCALLY:
            destination_folder = "assets/js"
            gcp.storage.upload(
                bucket_name=env.WEBSITE_BUCKET,
                local_filename=local_filename,
                destination_folder=destination_folder,
            )


def write_leaderboard(
    df: pd.DataFrame,
    primary_scoring_func: Callable[..., any],
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
    def format_ci(df, question_type):
        col_prefix = (
            f"{primary_scoring_func.__name__}"
            if question_type == "overall"
            else f"{primary_scoring_func.__name__}_{question_type}"
        )
        df[f"{col_prefix}_ci_lower"] = df[f"{col_prefix}_ci_lower"].round(3).astype(str)
        df[f"{col_prefix}_ci_upper"] = df[f"{col_prefix}_ci_upper"].round(3).astype(str)
        df[f"{col_prefix}_ci"] = (
            "[" + df[f"{col_prefix}_ci_lower"] + ", " + df[f"{col_prefix}_ci_upper"] + "]"
        )
        return df

    df = format_ci(df, "dataset")
    df = format_ci(df, "market")
    df = format_ci(df, "overall")

    df = df.sort_values(by=f"{primary_scoring_func.__name__}_overall", ignore_index=True)
    df["p_value_one_sided"] = df["p_value_one_sided"].apply(
        lambda p: (
            "<0.001" if p < 0.001 else "<0.01" if p < 0.01 else "<0.05" if p < 0.05 else f"{p:.2f}"
        )
    )
    df["x_pct_oracle_equivalent"] = df["x_pct_oracle_equivalent"].map("{:.0%}".format)

    # Set the p-value for the best to N/A
    df.loc[0, "p_value_one_sided"] = "—"

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
            f"{primary_scoring_func.__name__}_ci",
            "p_value_one_sided",
            "pct_times_best_performer",
            "pct_times_top_5_percentile",
            "x_pct_oracle_equivalent",
            "peer_score_overall",
            "brier_skill_score_overall",
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
            f"{primary_scoring_func.__name__}_ci": "95% CI",
            "p_value_one_sided": "P-value to best",
            "pct_times_best_performer": "Pct times № 1",
            "pct_times_top_5_percentile": "Pct times top 5%",
            "x_pct_oracle_equivalent": "x% oracle equiv",
            "peer_score_overall": "Peer",
            "brier_skill_score_overall": "BSS",
        }
    )

    write_leaderboard_html_file(
        df=df,
        sorting_column_number=9,
    )
    write_leaderboard_js_files(df)


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
    df: pd.DataFrame,
    scoring_funcs: List[Callable[[pd.DataFrame], pd.DataFrame]],
    primary_scoring_func: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Score models using the scoring functions in `scoring_funcs`.

    Args:
        df (pd.DataFrame): Combined forecast set.
        scoring_funcs (List[Callable[[pd.DataFrame], pd.DataFrame]]): List of scoring functions.
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]): Function to compute the
                     primary overall score.

    Returns:
        pd.DataFrame: Leaderboard DataFrame with:
            - For each scoring function: '{func_name}_dataset', '{func_name}_market', and
              '{func_name}_overall'
            - Count columns for dataset, market, and all questions
            - The 'organization', 'model', 'model_pk' associated with a forecast set
            - Rescaled scores for those calculated by the `primary_scoring_func`
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
    df_leaderboard = results[0].merge(
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

    df_leaderboard = rescale_difficulty_adjusted_brier(
        df_leaderboard=df_leaderboard,
        primary_scoring_func=primary_scoring_func,
    )
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

    workspace_folder = f"{inspect.currentframe().f_code.co_name}/{LEADERBOARD_TO_CREATE.value}"
    workspace_dir = data_utils.get_workspace_dir(
        bucket=env.WORKSPACE_BUCKET,
        folder=workspace_folder,
        recreate_folder=True,
    )

    def question_level_bootstrap(df):
        questions = df["question_pk"].drop_duplicates()
        questions_bs = questions.sample(frac=1, replace=True)
        return df.set_index("question_pk").loc[questions_bs]

    def bootstrap_and_score(idx):
        logger.info(f"[replicate {idx+1}/{N}] starting...")
        out_path_overall = Path(workspace_dir) / f"bootstrap_{idx}_overall.parquet"
        out_path_dataset = Path(workspace_dir) / f"bootstrap_{idx}_dataset.parquet"
        out_path_market = Path(workspace_dir) / f"bootstrap_{idx}_market.parquet"
        if out_path_overall.exists() and out_path_dataset.exists() and out_path_market.exists():
            return (
                out_path_dataset,
                out_path_market,
                out_path_overall,
            )

        df_bs = (
            df.groupby(["forecast_due_date", "source"])
            .apply(question_level_bootstrap, include_groups=False)
            .reset_index()
        )
        try:
            df_simulated_leaderboard = score_models(
                df=df_bs,
                scoring_funcs=[primary_scoring_func],
                primary_scoring_func=primary_scoring_func,
            )
        except Exception as e:
            traceback.print_exc()
            raise e

        df_simulated_leaderboard.set_index("model_pk")[
            f"{primary_scoring_func.__name__}_dataset"
        ].rename(f"bootstrap_{idx}").to_frame().to_parquet(out_path_dataset)
        df_simulated_leaderboard.set_index("model_pk")[
            f"{primary_scoring_func.__name__}_market"
        ].rename(f"bootstrap_{idx}").to_frame().to_parquet(out_path_market)
        df_simulated_leaderboard.set_index("model_pk")[
            f"{primary_scoring_func.__name__}_overall"
        ].rename(f"bootstrap_{idx}").to_frame().to_parquet(out_path_overall)
        return (
            out_path_dataset,
            out_path_market,
            out_path_overall,
        )

    logger.info(f"Simulating using {env.NUM_CPUS} CPU(s).")
    paths = Parallel(
        n_jobs=env.NUM_CPUS,
        backend="loky",
        verbose=5,
        batch_size=min(20, N),
    )(delayed(bootstrap_and_score)(i) for i in range(N))
    logger.info("Done simulating!")

    df_simulated_scores_dataset = pd.concat(
        [pd.read_parquet(p[0]).squeeze() for p in paths], axis=1
    )
    df_simulated_scores_market = pd.concat([pd.read_parquet(p[1]).squeeze() for p in paths], axis=1)
    df_simulated_scores_overall = pd.concat(
        [pd.read_parquet(p[2]).squeeze() for p in paths], axis=1
    )
    logger.info("Done creating df_simulated_scores!")

    # cleanup
    shutil.rmtree(workspace_dir)

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
        primary_scoring_func (Callable[[pd.DataFrame], pd.DataFrame]): Function to compute the
                     primary overall score.
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

    col_prefix = (
        f"{primary_scoring_func.__name__}"
        if question_type == "overall"
        else f"{primary_scoring_func.__name__}_{question_type}"
    )
    df_leaderboard[f"{col_prefix}_ci_lower"] = df_leaderboard["model_pk"].map(lower).values
    df_leaderboard[f"{col_prefix}_ci_upper"] = df_leaderboard["model_pk"].map(upper).values
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


def remove_x_pct_oracles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the x% forecasters from the dataframe.

    Args:
        df (pd.DataFrame): The combined forecast set or the leaderboard.

    Returns:
        pd.DataFrame: The same dataframe without the x% forecasters.
    """
    for pct in get_x_pct_oracle_increments():
        df = df[
            ~(
                (df["organization"] == constants.BENCHMARK_NAME)
                & (df["model"] == get_x_pct_oracle_model_name(pct))
            )
        ]

    return df.reset_index(drop=True)


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
    df = add_x_pct_oracles(df=df)

    # The scoring functions to consider
    primary_scoring_func = two_way_fixed_effects
    scoring_funcs = [
        primary_scoring_func,
        peer_score,
        brier_skill_score,
    ]

    # Score
    df_leaderboard = score_models(
        df=df,
        scoring_funcs=scoring_funcs,
        primary_scoring_func=primary_scoring_func,
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

    # Compare to best model
    df_leaderboard = get_comparison_to_best_model(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_overall,
        primary_scoring_func=primary_scoring_func,
        is_centered=False,
    )

    # Simulation performance measures
    df_leaderboard = get_simulation_performance_metrics(
        df_leaderboard=df_leaderboard,
        df_simulated_scores=df_simulated_scores_overall,
    )

    # Write leaderboard
    write_leaderboard(
        df=df_leaderboard,
        primary_scoring_func=primary_scoring_func,
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
    logger.info(f"Processing forecast due dates: {valid_dates}.")
    local_forecast_set_dir = data_utils.get_local_file_dir(bucket=bucket)
    leaderboard_entries = []
    for f in forecast_files:
        logger.info(f"Ranking {f}")
        data = resolution.read_forecast_file(filename=f"{local_forecast_set_dir}/{f}")
        if data is None:
            continue

        organization = data.get("organization")
        model = data.get("model")
        model_organization = data.get("model_organization")
        forecast_due_date = data.get("forecast_due_date")
        df = data.get("df")

        append_leaderboard_entry(
            leaderboard_entries=leaderboard_entries,
            org_and_model={
                "organization": organization,
                "model": model,
                "model_organization": model_organization,
            },
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
    logger.info(colored(f"Making {LEADERBOARD_TO_CREATE.value.upper()} leaderboard.", "red"))
    leaderboard_entries = download_and_compile_processed_forecast_files(
        bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
    )
    make_leaderboard(leaderboard_entries=leaderboard_entries)
    logger.info(colored(f"Done making {LEADERBOARD_TO_CREATE.value.upper()} leaderboard.", "red"))


if __name__ == "__main__":
    driver(None)
