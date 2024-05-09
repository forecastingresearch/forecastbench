"""Create a test forecast file from a generated question file."""

import itertools
import json

import markets
import numpy as np
import pandas as pd

llm_or_human = "llm"
question_set_filename = f"2024-05-03-{llm_or_human}.jsonl"
forecast_date = question_set_filename[:10]


def write_json_file(filename, output):
    """Write JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f)


def expand_directions(row):
    """Expand combo questions for all directions."""
    if isinstance(row["id"], list) and len(row["id"]) > 1:
        directions = [list(tup) for tup in itertools.product([-1, 1], repeat=len(row["id"]))]
        return pd.DataFrame([row] * len(directions)).assign(direction=directions)
    row["direction"] = []
    return pd.DataFrame([row])


df = pd.read_json(question_set_filename, lines=True)
df = df[["id", "source", "forecast_horizons"]]
df = df.explode("forecast_horizons", ignore_index=True)
df.rename(columns={"forecast_horizons": "horizon"}, inplace=True)

df_market, df_non_market = (
    df[df["source"].isin(markets.MARKET_SOURCES)].copy(),
    df[~df["source"].isin(markets.MARKET_SOURCES)].copy(),
)
df_market["id_str"] = df_market["id"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
df_market = df_market.drop_duplicates(subset=["source", "id_str"])
df_market.drop(columns=["id_str"], inplace=True)
df_market["horizon"] = "N/A"

df = pd.concat([df_market, df_non_market], ignore_index=True)
df = pd.concat([expand_directions(row) for index, row in df.iterrows()], ignore_index=True)

df["forecast"] = np.random.rand(len(df))

print(df)

output = {
    "organization": "FRI/Berkeley",
    "question_set": question_set_filename,
    "forecast_date": forecast_date,
}

output["model"] = "Random Uniform"
output["forecasts"] = json.loads(df.to_json(orient="records"))
write_json_file(f"{forecast_date}.fri-berkeley.{llm_or_human}-random-forecast.json", output)


df["forecast"] = 0.5
output["model"] = "Always 0.5"
output["forecasts"] = json.loads(df.to_json(orient="records"))
write_json_file(f"{forecast_date}.fri-berkeley.{llm_or_human}-always-0.5-forecast.json", output)


df["forecast"] = 0
output["model"] = "Always 0"
output["forecasts"] = json.loads(df.to_json(orient="records"))
write_json_file(f"{forecast_date}.fri-berkeley.{llm_or_human}-always-zero-forecast.json", output)


df["forecast"] = 1
output["model"] = "Always 1"
output["forecasts"] = json.loads(df.to_json(orient="records"))
write_json_file(f"{forecast_date}.fri-berkeley.{llm_or_human}-always-one-forecast.json", output)
