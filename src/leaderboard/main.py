"""Create leaderboard."""

import itertools
import json
import logging
import os
import sys

import numpy as np
import pandas as pd

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

BASELINE_ORG_MODEL = {"organization": constants.BENCHMARK_NAME, "model": "Naive Forecast"}


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

    # Markets
    market_resolved_score, n_market_resolved = get_scores(df, market_mask & resolved_mask)
    market_unresolved_score, n_market_unresolved = get_scores(df, market_mask & unresolved_mask)
    market_overall_score, n_market_overall = get_scores(df, market_mask)

    # Overall Resolved
    overall_resolved_score = (data_resolved_score + market_resolved_score) / 2
    n_overall_resolved = n_data_resolved + n_market_resolved

    # Overall
    overall_score = (data_resolved_score + market_overall_score) / 2
    overall_std_dev = df["score"].std()
    n_overall = len(df)

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
        "n_overall": n_overall,
        "pct_imputed": pct_imputed,
        #        "std_dev": overall_std_dev,
    }


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
    logger.info(f"Making leaderboard table: {title} {basename}.")
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

    df["pct_imputed"] = df["pct_imputed"].astype(str) + "%"

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
            "pct_imputed": "Pct. Imputed",
            #            "std_dev": "Old Mean",  # Std. Dev.", # DELETE
            #            "z_score_wrt_naive_mean": "Z-score",
        }
    )

    # Remove lengths from df
    df = df[[c for c in df.columns if not c.startswith("n_")]]

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
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
              integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
              crossorigin="anonymous">
        <link rel="stylesheet" type="text/css"
              href="https://cdn.datatables.net/2.1.6/css/dataTables.jqueryui.min.css">
        <script src="https://code.jquery.com/jquery-3.7.1.js"></script>
        <script type="text/javascript" charset="utf8"
                src="https://cdn.datatables.net/2.1.6/js/dataTables.min.js"></script>
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
            h1 {
              text-align: center;
              margin-top: 10px;
              font-family: 'Arial', sans-serif;
              font-size: 16px;
           }
        </style>
    </head>
    <body>
        <div class="container mt-4">
    """
        + "<h1>"
        + title
        + "</h1>"
        + html_code
        + """
        </div>
        <script>
        $(document).ready(function() {
            $('#myTable').DataTable({
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
    files = [
        "2024-07-21/2024-07-21.ForecastBench.human_super.json",
        "2024-07-21/2024-07-21.ForecastBench.human_public.json",
    ]
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
        df = pd.DataFrame(d)
        upload_leaderboard(df=df, basename=basename)
        make_html_table(
            df=df,
            title=title,
            basename=basename,
        )

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

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
