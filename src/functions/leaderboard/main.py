"""Create leaderboards."""

import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
import gcp.storage.run as storage  # noqa: E402

ref_storage_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")

LLM_MODEL_KEYS = {
    "gpt3.5": "FRI GPT-3.5",
    "gpt4": "FRI GPT-4",
}


def _read_forecasts_from_files(filename: str):
    """Download forecast file and read the forecasts into a dataframe."""
    local_forecast_filename = "/tmp/forecast.json"
    try:
        print(f"Downloading forecast file: {filename}")
        storage.download(
            bucket_name=ref_storage_bucket,
            filename=filename,
            local_filename=local_forecast_filename,
        )
        with open(local_forecast_filename, "r") as file:
            data = json.load(file)
    except Exception:
        print(f"Error downloading {filename}")
        return []

    # Only account for the last forecast in the day
    latest_forecasts_by_day = {}
    for forecast in data:
        forecast_datetime = datetime.strptime(forecast["datetime"], "%Y-%m-%d %H:%M:%S")
        forecast_date = forecast_datetime.date()
        if forecast_date not in latest_forecasts_by_day or forecast_datetime > datetime.strptime(
            latest_forecasts_by_day[forecast_date]["datetime"], "%Y-%m-%d %H:%M:%S"
        ):
            forecast["date"] = forecast_date
            latest_forecasts_by_day[forecast_date] = forecast
    return sorted(
        latest_forecasts_by_day.values(),
        key=lambda x: datetime.strptime(x["datetime"], "%Y-%m-%d %H:%M:%S"),
    )


def _get_market_id_and_horizon_from_manifold_filename(filename):
    """Return the market id and the horizon from the filename.

    Expecting filename in the form: `forecasts/manifold/yVAEXGniXeFkCfHQajGo_14_days.json`.

    returns ("yVAEXGniXeFkCfHQajGo", 14)
    """
    filename = filename.split("/")[-1]
    filename_split = filename.split("_")
    return filename_split[0], int(filename_split[1])


def _get_market_id_and_horizon_from_wikidata_filename(filename):
    """Return the market id and the horizon from the filename.

    Expecting filename in the form: `forecasts/wikidata/Afghanistan_head_of_government_14_days.json`

    returns ("yVAEXGniXeFkCfHQajGo", 14)
    """
    filename = filename.split("/")[-1]
    parts = filename.split("_")[:-2]
    market_id = "_".join(parts)
    horizon = filename.split("_")[-2]
    return market_id, int(horizon)


def _get_llms(data):
    """Get the names of all LLMs that have forecast on a given question."""
    return {key for item in data for key in item["llms"].keys()}


def _get_manifold_forecasts():
    """Get all Manifold Markets forecasts into a dataframe."""
    forecasts = []
    for filename in storage.list_with_prefix(
        bucket_name=ref_storage_bucket, prefix="forecasts/manifold"
    ):
        market_id, horizon = _get_market_id_and_horizon_from_manifold_filename(filename)
        forecast_data = _read_forecasts_from_files(filename)
        llms = _get_llms(forecast_data)
        for forecast in forecast_data:
            forecast_to_append = {
                "question_source": "manifold",
                "id": market_id,
                "horizon": horizon,
                "date": forecast["date"],
                "market_value": forecast["manifold_market_value"],
            }
            for llm in llms:
                if llm in forecast["llms"]:
                    forecast_to_append["forecast_source"] = llm
                    forecast_to_append["forecast_value"] = forecast["llms"][llm]["forecast"]
            forecasts.append(forecast_to_append)
    return pd.DataFrame(forecasts)


def _get_wikidata_forecasts():
    """Get all Wikidata Markets forecasts into a dataframe."""
    forecasts = []
    for filename in storage.list_with_prefix(
        bucket_name=ref_storage_bucket, prefix="forecasts/wikidata"
    ):
        market_id, horizon = _get_market_id_and_horizon_from_wikidata_filename(filename)
        forecast_data = _read_forecasts_from_files(filename)
        llms = _get_llms(forecast_data)
        for forecast in forecast_data:
            forecast_to_append = {
                "question_source": "wikidata",
                "id": market_id,
                "horizon": horizon,
                "date": forecast["date"],
                "market_value": forecast["heads"],
            }
            for llm in llms:
                if llm in forecast["llms"]:
                    forecast_to_append["forecast_source"] = llm
                    forecast_to_append["forecast_value"] = forecast["llms"][llm]["forecast"]
            forecasts.append(forecast_to_append)

    return pd.DataFrame(forecasts)


def _merge_sanity_check(df_current_horizon):
    aligned_data = []
    for index, row in df_current_horizon.iterrows():
        actual_row = df_current_horizon[
            (df_current_horizon["id"] == row["id"])
            & (df_current_horizon["date"] == row["shifted_date"])
        ]
        if not actual_row.empty:
            # If exists, create a new record combining forecast and actual market values
            aligned_data.append(
                {
                    "id": row["id"],
                    "shifted_date": row["shifted_date"],
                    "forecast_value": row["forecast_value"],
                    "market_value_forecast": row[
                        "market_value"
                    ],  # Assuming this exists in your initial dataframe
                    "forecast_source": row["forecast_source"],
                    "market_value_actual": actual_row["market_value"].values[
                        0
                    ],  # Get the actual market value
                }
            )
    return pd.DataFrame(aligned_data)


def _calculate_mean_squared_error(df):
    df["date"] = pd.to_datetime(df["date"])
    mean_squared_error = []
    for horizon in sorted(df["horizon"].unique()):
        df_current_horizon = df[df["horizon"] == horizon].copy()
        df_current_horizon["shifted_date"] = df_current_horizon["date"] + pd.to_timedelta(
            df_current_horizon["horizon"], unit="d"
        )

        # For forecasts, align forecast_value with actual market_value
        # For Random Walk forecast, create market_value_forecast
        df_current_horizon = pd.merge(
            df_current_horizon[
                ["id", "shifted_date", "forecast_value", "market_value", "forecast_source"]
            ],
            df_current_horizon[["id", "date", "market_value"]],
            left_on=["id", "shifted_date"],
            right_on=["id", "date"],
            suffixes=("_forecast", "_actual"),
        )

        if not df_current_horizon.empty and isinstance(
            df_current_horizon["market_value_forecast"].iloc[0], list
        ):
            # Add resolution column when the question has resolved (instead of using the market
            # value as resolution criteria)
            # e.g. for now, this is only for wikidata where resolution is a comparison of heads of
            #      state/gov
            df_current_horizon["resolution"] = np.where(
                df_current_horizon["market_value_forecast"].astype(str)
                == df_current_horizon["market_value_actual"].astype(str),
                1,
                0,
            )

        # Do this to drop the rows where multiple llms have forecast on the same question on the
        # same day
        df_baseline = df_current_horizon.drop_duplicates(
            subset=["id", "shifted_date"], keep="first"
        )
        num_comparison_model_forecasts = len(df_baseline)
        if num_comparison_model_forecasts > 0:
            if "resolution" in df_baseline.columns:
                random_walk_mse = ((1 - df_baseline["resolution"]) ** 2).mean()
                always_1_mse = random_walk_mse
                always_0_mse = ((0 - df_baseline["resolution"]) ** 2).mean()
                always_05_mse = ((0.5 - df_baseline["resolution"]) ** 2).mean()
            else:
                # Case for numeric values
                random_walk_mse = (
                    (df_baseline["market_value_forecast"] - df_baseline["market_value_actual"]) ** 2
                ).mean()
                always_1_mse = ((1 - df_baseline["market_value_actual"]) ** 2).mean()
                always_0_mse = ((0 - df_baseline["market_value_actual"]) ** 2).mean()
                always_05_mse = ((0.5 - df_baseline["market_value_actual"]) ** 2).mean()

            entry_to_append = {
                "horizon": horizon,
                "random_walk": (random_walk_mse, num_comparison_model_forecasts),
                "always_1": (always_1_mse, num_comparison_model_forecasts),
                "always_0": (always_0_mse, num_comparison_model_forecasts),
                "always_0.5": (always_05_mse, num_comparison_model_forecasts),
            }

            # dropna for the case where no LLMs forecast
            df_current_horizon = df_current_horizon.dropna(subset=["forecast_source"])
            for llm in df_current_horizon["forecast_source"].unique():
                df_llms = df_current_horizon[df_current_horizon["forecast_source"] == llm]
                if df_llms.empty:
                    entry_to_append[llm] = (np.nan, 0)
                else:
                    if "resolution" in df_llms.columns:
                        entry_to_append[llm] = (
                            ((df_llms["forecast_value"] - df_llms["resolution"]) ** 2).mean(),
                            len(df_llms),
                        )
                    else:
                        entry_to_append[llm] = (
                            (
                                (df_llms["forecast_value"] - df_llms["market_value_actual"]) ** 2
                            ).mean(),
                            len(df_llms),
                        )
            mean_squared_error.append(entry_to_append)

    return pd.DataFrame(mean_squared_error).sort_values(by="horizon")


def _calculate_overall_mean_squared_error(dataframes):
    # Find common horizons across all DataFrames
    horizons = set.intersection(*(set(df["horizon"].unique()) for df in dataframes))
    # Identify common columns (excluding 'horizon')
    common_columns = set.intersection(*(set(df.columns) for df in dataframes))
    common_columns.discard("horizon")

    overall_mse_results = []
    for horizon in sorted(horizons):
        result_row = {"horizon": horizon}
        for col in common_columns:
            mse_total = 0
            n_total = 0
            for df in dataframes:
                if horizon in df["horizon"].values and col in df.columns:
                    row = df[(df["horizon"] == horizon) & (~df[col].isna())]
                    if not row.empty:
                        mse, n = row.iloc[0][col]
                        mse_total += mse * n
                        n_total += n
            if n_total > 0:
                result_row[col] = (mse_total / n_total, n_total)
            else:
                result_row[col] = (np.nan, 0)
        overall_mse_results.append(result_row)

    return pd.DataFrame(overall_mse_results)


def _make_horizon_leaderboard(df_dict, df_overall):
    """Generate the leaderboard in Plotly given the Brier scores."""

    def process_df(title, df):
        df.rename(
            columns={
                "random_walk": "Random Walk",
                "always_1": "Always 1",
                "always_0": "Always 0",
                "always_0.5": "Always 0.5",
            },
            inplace=True,
        )

        df.rename(columns=LLM_MODEL_KEYS, inplace=True)
        df = df.sort_values(by="horizon")

        html_content = ""
        for _, row in df.iterrows():
            sorted_row = row.drop(["horizon"]).sort_values().to_frame().reset_index()
            sorted_row.columns = ["Model", "MSE"]
            sorted_row[["MSE", "№ forecasts"]] = pd.DataFrame(
                sorted_row["MSE"].tolist(), index=sorted_row.index
            )
            sorted_row = sorted_row.dropna()

            row_colors = ["lavender" if i % 2 == 0 else "white" for i in range(len(sorted_row))]
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=["Model", "MSE", "№ forecasts"],
                            align="left",
                            fill_color="paleturquoise",
                        ),
                        cells=dict(
                            values=[
                                sorted_row["Model"],
                                sorted_row["MSE"].round(6),
                                sorted_row["№ forecasts"],
                            ],
                            align="left",
                            fill_color=[row_colors, row_colors],
                        ),
                    )
                ]
            )

            days_text = "Days" if int(row["horizon"]) > 1 else "Day"
            fig.update_layout(title_text=(f"Horizon {int(row['horizon'])} {days_text}. "))
            plot_html = fig.to_html(
                full_html=False, include_plotlyjs=False, config={"displayModeBar": False}
            )
            html_content += f'<div class="table-container">{plot_html}</div>'
        return html_content

    combined_html_content = """<div class="row">"""
    for title, df in df_dict.items():
        html = process_df(title, df)
        combined_html_content += (
            f"""<div class="column"><h2>{title.capitalize()}</h2>{html}</div>"""
        )

    html = process_df(title, df_overall)
    combined_html_content += f"""<div class="column"><h2>Overall</h2>{html}</div>"""
    combined_html_content += "</div>"

    utc_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    html_content = (
        """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body {
                font-family: Arial, sans-serif;
            }
            h2 {
                font-weight: normal;
                font-size: 24px;
                color: #333;
            }
            .plotly-graph-div {
                margin-bottom: -200px !important;
            }
            .column {"""
        + f"""float: left;
                width: {100/(len(df_dict.keys())+1)}%;"""
        """}
            /* Clear floats after the columns */
            .row:after {
                content: "";
                display: table;
                clear: both;
            }
            .table-container {
                margin: auto;
            }
            #leaderboard-header {
                text-align: left;
                font-size: 22px;
                padding-top: 10px;
                padding-bottom: 0px;
            }
            footer {
                clear: both;
                position: relative;
                color: black;
                padding: 10px;
                font-size: 0.8em;
            }
        </style>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
        <div id="leaderboard-header">Leaderboard</div>
        """
        + combined_html_content
        + f"""
        <footer><span id="datetime">{utc_datetime} UTC</span></footer>
        </body>
        </html>
        """
    )

    leaderboard_filename = "/tmp/leaderboard.html"
    with open(leaderboard_filename, "w") as file:
        file.write(html_content)
    print(f"Saved: {leaderboard_filename}")

    storage.upload(
        bucket_name=ref_storage_bucket,
        local_filename=leaderboard_filename,
    )


def _create_leaderboards():
    """Create Leaderboard."""
    dfm = _get_manifold_forecasts()
    dfw = _get_wikidata_forecasts()

    dfm_mse = _calculate_mean_squared_error(dfm)
    dfw_mse = _calculate_mean_squared_error(dfw)
    df_mse_overall = _calculate_overall_mean_squared_error([dfm_mse, dfw_mse])

    _make_horizon_leaderboard({"manifold": dfm_mse, "wikidata": dfw_mse}, df_mse_overall)


def driver(request):
    """Google Cloud Function Driver."""
    _create_leaderboards()
    return "OK", 200


if __name__ == "__main__":
    driver(None)
