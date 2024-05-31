"""Create leaderboard."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import constants, decorator, resolution  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASELINE_ORG_MODEL = {"organization": constants.BENCHMARK_NAME, "model": "Naive Forecast"}


def download_and_read_forecast_file(filename):
    """Download forecast file."""
    local_filename = "/tmp/tmp.json"
    gcp.storage.download(
        bucket_name=constants.PROCESSED_FORECAST_BUCKET_NAME,
        filename=filename,
        local_filename=local_filename,
    )
    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def upload_leaderboard(df, basename):
    """Upload leaderboard."""
    local_filename = f"/tmp/{basename}.csv"
    df.to_csv(local_filename, index=False)
    gcp.storage.upload(
        bucket_name=constants.PROCESSED_FORECAST_BUCKET_NAME,
        local_filename=local_filename,
    )


def download_human_question_set(forecast_date):
    """Download the question set that was given to humans."""
    df = pd.read_json(
        f"gs://{constants.PUBLIC_BUCKET_NAME}/{forecast_date}-human.jsonl",
        lines=True,
        convert_dates=False,
    )
    df = resolution.make_columns_hashable(df)
    # DROP COMBO QUESTIONS FOR MARKETS
    df = df[
        ~df.apply(
            lambda x: resolution.is_combo(x) and x["source"] not in constants.DATA_SOURCES,
            axis=1,
        )
    ].reset_index(drop=True)
    return df


def add_to_leaderboard(leaderboard, org_and_model, df):
    """Add scores to the leaderboard."""
    # Masks
    data_mask = df["source"].isin(constants.DATA_SOURCES)
    market_mask = ~data_mask

    resolved_mask = df["resolved"].astype(bool)
    unresolved_mask = ~resolved_mask

    def get_scores(df, mask):
        scores = df[mask]["score"]
        return scores.mean(), len(scores)

    # Datasets
    data_resolved_score, n_data_resolved = get_scores(df, data_mask & resolved_mask)

    # Markets
    market_resolved_score, n_market_resolved = get_scores(df, market_mask & resolved_mask)
    market_unresolved_score, n_market_unresolved = get_scores(df, market_mask & unresolved_mask)
    market_overall_score, n_market_overall = get_scores(df, market_mask)

    # Overall
    overall_score = df["score"].mean()
    overall_std_dev = df["score"].std()
    n_overall = len(df)

    leaderboard += [
        {
            **org_and_model,
            "data": data_resolved_score,
            "n_data": n_data_resolved,
            "market_resolved": market_resolved_score,
            "n_market_resolved": n_market_resolved,
            "market_unresolved": market_unresolved_score,
            "n_market_unresolved": n_market_unresolved,
            "market_overall": market_overall_score,
            "n_market_overall": n_market_overall,
            "overall": overall_score,
            "n_overall": n_overall,
            "std_dev": overall_std_dev,
        }
    ]
    return leaderboard


def add_to_llm_leaderboard(*args, **kwargs):
    """Wrap `add_to_leaderboard` for easy reading of driver."""
    return add_to_leaderboard(*args, **kwargs)


def add_to_llm_and_human_leaderboard(leaderboard, org_and_model, df, forecast_date):
    """Parse the forecasts to include only those questions that were in the human question set."""
    df_human_question_set = download_human_question_set(forecast_date)
    df_only_human_question_set = pd.merge(
        df, df_human_question_set[["id", "source"]], on=["id", "source"], how="right"
    )
    return add_to_leaderboard(
        leaderboard=leaderboard, org_and_model=org_and_model, df=df_only_human_question_set
    )


def make_html_table(df, basename):
    """Make and upload HTLM leaderboard."""
    # Round columns to 3 decimal places
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].round(3)

    # Replace NaN with empty strings for display
    df = df.fillna("")

    df = df.sort_values(by="overall", ignore_index=True)

    # Rename columns
    n_data = df["n_data"].max()
    n_market_resolved = df["n_market_resolved"].max()
    n_market_unresolved = df["n_market_unresolved"].max()
    n_market_overall = df["n_market_overall"].max()
    n_overall = df["n_overall"].max()
    df = df.rename(
        columns={
            "organization": "Organization",
            "model": "Model",
            "data": f"Dataset Score (N={n_data:,})",
            "market_resolved": f"Market Score (resolved) (N={n_market_resolved:,})",
            "market_unresolved": f"Market Score (unresolved) (N={n_market_unresolved:,})",
            "market_overall": f"Market Score (overall) (N={n_market_overall:,})",
            "overall": f"Overall Score (N={n_overall:,})",
            "std_dev": "Std. Dev.",
            "z_score_wrt_naive_mean": "Z-score",
        }
    )

    # Remove lengths from df
    df = df[[c for c in df.columns if not c.startswith("n_")]]

    df.insert(0, "Ranking", range(1, len(df) + 1))
    html_code = df.to_html(
        classes="table table-striped table-bordered", index=False, table_id="myTable"
    )
    html_code = (
        """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>LLM Data Table</title>
        <link rel="stylesheet"
          href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <link rel="stylesheet" type="text/css"
          href="https://cdn.datatables.net/1.10.21/css/jquery.dataTables.css">
        <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
        <script type="text/javascript" charset="utf8"
          src="https://cdn.datatables.net/1.10.21/js/jquery.dataTables.js"></script>
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
        </style>
    </head>
    <body>
        <div class="container mt-4">
    """
        + html_code
        + """
        </div>
        <script>
        $(document).ready(function() {
            $('#myTable').DataTable({
                "pageLength": -1,
                "lengthMenu": [[-1], ["All"]],
                "order": [[7, 'asc']],
                "paging": false,
                "info": false,
                "search": {
                    "regex": true,
                    "smart": true
                },
                "columnDefs": [
                    {
                        "targets": '_all',
                        "searchable": true
                    }
                ]
            });
        });
        </script>
    </body>
    </html>
    """
    )

    local_filename = f"/tmp/{basename}.html"
    with open(local_filename, "w") as file:
        file.write(html_code)
    gcp.storage.upload(
        bucket_name=constants.LEADERBOARD_BUCKET_NAME,
        local_filename=local_filename,
    )


@decorator.log_runtime
def driver(_):
    """Create new leaderboard."""
    llm_leaderboard = []
    llm_and_human_leaderboard = []
    files = gcp.storage.list(constants.PROCESSED_FORECAST_BUCKET_NAME)
    files = [file for file in files if file.endswith(".json")]
    for f in files:

        logger.info(f"Downloading, reading, and scoring forecasts in `{f}`...")

        data = download_and_read_forecast_file(filename=f)
        if not data or not isinstance(data, dict):
            continue

        organization = data.get("organization")
        model = data.get("model")
        question_set_filename = data.get("question_set")
        forecast_date = data.get("forecast_date")
        forecasts = data.get("forecasts")
        if (
            not organization
            or not model
            or not question_set_filename
            or not forecast_date
            or not forecasts
        ):
            continue

        df = pd.DataFrame(forecasts)
        if df.empty:
            continue
        df = resolution.make_columns_hashable(df)

        is_human_question_set = "human" in question_set_filename
        org_and_model = {"organization": organization, "model": model}
        if not is_human_question_set:
            llm_leaderboard = add_to_llm_leaderboard(llm_leaderboard, org_and_model, df)
        llm_and_human_leaderboard = add_to_llm_and_human_leaderboard(
            llm_and_human_leaderboard, org_and_model, df, forecast_date
        )

    def get_z_score(df):
        mask = (df["organization"] == BASELINE_ORG_MODEL["organization"]) & (
            df["model"] == BASELINE_ORG_MODEL["model"]
        )
        naive_baseline_mean = df[mask]["overall"].values[0]
        naive_std_dev = df[mask]["std_dev"].values[0]
        df["z_score_wrt_naive_mean"] = (df["overall"] - naive_baseline_mean) / naive_std_dev
        return df

    df_llm = get_z_score(pd.DataFrame(llm_leaderboard)).sort_values(by="overall", ignore_index=True)
    df_llm_human = get_z_score(pd.DataFrame(llm_and_human_leaderboard)).sort_values(
        by="overall", ignore_index=True
    )

    upload_leaderboard(df=df_llm, basename="leaderboard_llm")
    upload_leaderboard(df=df_llm_human, basename="leaderboard_llm_human")

    make_html_table(df=df_llm, basename="lm_leaderboard")
    make_html_table(df=df_llm_human, basename="lm_human_leaderboard")

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
