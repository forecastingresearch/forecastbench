"""Create leaderboards."""

import json
import os
import sys
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

import gcp.storage.run as storage  # noqa: E402

ref_storage_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")


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
    filename = filename.split("/")[2]
    filename_split = filename.split("_")
    return filename_split[0], int(filename_split[1])


def _get_manifold_forecasts():
    """Get all Manifold Markets forecasts into a dataframe."""
    forecasts = []
    for filename in storage.list_with_prefix(
        bucket_name=ref_storage_bucket, prefix="forecasts/manifold"
    ):
        market_id, horizon = _get_market_id_and_horizon_from_manifold_filename(filename)
        forecast_data = _read_forecasts_from_files(filename)
        for forecast in forecast_data:
            forecast_to_append = {
                "question_source": "manifold",
                "id": market_id,
                "horizon": horizon,
                "date": forecast["date"],
                "market_value": forecast["manifold_market_value"],
            }
            if "gpt4" in forecast["llms"]:
                forecast_to_append["forecast_source"] = "gpt4"
                forecast_to_append["forecast_value"] = forecast["llms"]["gpt4"]["forecast"]
            forecasts.append(forecast_to_append)

    df = pd.DataFrame(forecasts)
    return df


def _calculate_brier_scores(df):
    df["date"] = pd.to_datetime(df["date"])
    brier_scores = []

    for horizon in df["horizon"].unique():
        # Prepare data for the current horizon
        df_current_horizon = df[df["horizon"] == horizon].copy()
        df_current_horizon["shifted_date"] = df_current_horizon["date"] + pd.to_timedelta(
            df_current_horizon["horizon"], unit="d"
        )

        # For forecasts, align forecast_value with actual market_value
        # For Random Walk forecast, create market_value_forecast
        df_current_horizon = pd.merge(
            df_current_horizon[["id", "shifted_date", "forecast_value", "market_value"]],
            df_current_horizon[["id", "date", "market_value"]],
            left_on=["id", "shifted_date"],
            right_on=["id", "date"],
            suffixes=("_forecast", "_actual"),
        )

        # Calculate Brier scores for the gpt forecast, fixed forecasts, and random walk
        gpt4_forecast_brier = (
            (df_current_horizon["forecast_value"] - df_current_horizon["market_value_actual"]) ** 2
        ).mean()
        random_walk_brier = (
            (
                df_current_horizon["market_value_forecast"]
                - df_current_horizon["market_value_actual"]
            )
            ** 2
        ).mean()
        always_1_brier = ((1 - df_current_horizon["market_value_actual"]) ** 2).mean()
        always_0_brier = ((0 - df_current_horizon["market_value_actual"]) ** 2).mean()
        always_05_brier = ((0.5 - df_current_horizon["market_value_actual"]) ** 2).mean()

        brier_scores.append(
            {
                "horizon": horizon,
                "gpt4_forecast": gpt4_forecast_brier,
                "random_walk": random_walk_brier,
                "always_1": always_1_brier,
                "always_0": always_0_brier,
                "always_0.5": always_05_brier,
                "num_forecasts": len(df_current_horizon),
            }
        )
    return pd.DataFrame(brier_scores).sort_values(by="horizon")


def _make_horizon_leaderboard(df):
    """Generate the leaderboard in Plotly given the Brier scores."""
    df.rename(
        columns={
            "random_walk": "Random Walk",
            "always_1": "Always 1",
            "always_0": "Always 0",
            "always_0.5": "Always 0.5",
            "gpt4_forecast": "GPT-4",
        },
        inplace=True,
    )
    df = df.sort_values(by="horizon")

    html_content = ""
    for _, row in df.iterrows():
        sorted_row = (
            row.drop(
                [
                    "horizon",
                    "num_forecasts",
                ]
            )
            .sort_values()
            .to_frame()
            .reset_index()
        )
        sorted_row.columns = ["Model", "Brier Score"]

        row_colors = ["lavender" if i % 2 == 0 else "white" for i in range(len(sorted_row))]
        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=["Model", "Brier Score"], align="left", fill_color="paleturquoise"
                    ),
                    cells=dict(
                        values=[sorted_row["Model"], sorted_row["Brier Score"].round(6)],
                        align="left",
                        fill_color=[row_colors, row_colors],
                    ),
                )
            ]
        )

        days_text = "Days" if int(row["horizon"]) > 1 else "Day"
        fig.update_layout(
            title_text=(
                f"Horizon {int(row['horizon'])} {days_text}. "
                f"Number of forecasts: {int(row['num_forecasts'])}."
            )
        )
        plot_html = fig.to_html(
            full_html=False, include_plotlyjs=False, config={"displayModeBar": False}
        )
        html_content += f'<div class="table-container">{plot_html}</div>'

    html_content = (
        """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body {
                font-family: Arial, sans-serif;
            }
            .plotly-graph-div {
                margin-bottom: -200px !important;
            }
            .table-container {
                width: 50%;
                margin: auto;
            }
            #leaderboard-header {
                width: 50%;
                margin: auto;
                text-align: left;
                font-size: 22px;
                padding-top: 10px;
                padding-bottom: 0px;
            }
        </style>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
        <div id="leaderboard-header">Leaderboard</div>
        """
        + html_content
        + """
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
    df = _get_manifold_forecasts()
    print(df)

    df = _calculate_brier_scores(df)
    print(df)

    df = df[df["num_forecasts"] > 0]
    if df.empty:
        print("No Brier Scores to report")
    else:
        _make_horizon_leaderboard(df)


def driver(request):
    """Google Cloud Function Driver."""
    _create_leaderboards()
    return "OK", 200


if __name__ == "__main__":
    driver(None)
