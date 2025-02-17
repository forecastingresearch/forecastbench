"""Provide entries to be pasted into the Results tables (Table 2, Table 3) and in the appendix."""

import os
import sys

import pandas as pd
from variables import BUCKET_NAME

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402


def process_leaderboard_data(filename, n):
    """Get top `n` results from the csv."""
    print(f"Processing {filename}...")
    df = pd.read_csv(
        f"gs://{BUCKET_NAME}/{filename}",
    )

    dataset_col = [col for col in df.columns if col.startswith("Dataset Score")][0]
    market_col = [col for col in df.columns if col.startswith("Market Score (overall)")][0]
    overall_col = [col for col in df.columns if col.startswith("Overall Score (N=")][0]

    result_df = pd.DataFrame(
        {
            "Model": df["Model"].apply(lambda x: x.split("(")[0].strip()),
            "Organization": df["Organization"],
            "Information provided": df.apply(lambda row: get_info_provided(row["Model"]), axis=1),
            "Prompt": df.apply(lambda row: get_prompt_type(row["Model"]), axis=1),
            "Dataset": df[dataset_col].round(3),
            "Market": df[market_col].round(3),
            "Overall": df[overall_col].round(3),
        }
    )

    result_df["Confidence Interval"] = (
        df["Overall Score 95% CI"]
        .apply(lambda x: x.strip("[]").split())
        .apply(lambda x: f"[{', '.join(f'{float(v):.3f}' for v in x)}]")
    )

    result_df["Pairwise p-value"] = df["Pairwise p-value comparing to No. 1 (bootstrapped)"].apply(
        lambda x: "--" if pd.isna(x) else x if x == "<0.001" or x == "<0.01" else f"{float(x):.3f}"
    )
    result_df["Pct. more accurate"] = df["Pct. more accurate than No. 1"].map(str)

    return result_df.head(n)


def get_info_provided(model_str):
    """Extract information provided from model string."""
    if "news" in model_str.lower():
        if "freeze values" in model_str.lower():
            return "News with freeze values"
        return "News"
    elif "freeze values" in model_str.lower():
        return "Freeze values"
    return "--"


def get_prompt_type(model_str):
    """Extract prompt type from model string."""
    if "scratchpad" in model_str.lower():
        return "Scratchpad"
    elif "zero shot" in model_str.lower():
        return "Zero shot"
    elif "(superforecaster" in model_str.lower():
        retval = "Superforecaster "
        if "1" in model_str.lower():
            retval += "1"
        elif "2" in model_str.lower():
            retval += "2"
        elif "3" in model_str.lower():
            retval += "3"
        return retval
    return "--"


def print_latex_rows(f, df, n):
    """Print rows in latex to be pasted into table."""
    filename = os.path.basename(f).replace(".csv", f".{n}.csv")
    print(f"Writing {filename}.")
    with open(filename, "w") as f:
        for _, row in df.head(n).iterrows():
            f.write(
                f"{row['Model']} & {row['Organization']} & {row['Information provided']} & "
                f"{row['Prompt']} & {row['Dataset']:.3f} & {row['Market']:.3f} "
                f"& {row['Overall']:.3f} & {row['Confidence Interval']} "
                f"& {row['Pairwise p-value']} & {row['Pct. more accurate'].replace('%', '\\%')} "
                "\\\\"
                "\n"
            )


if __name__ == "__main__":
    prefix = "leaderboards/csv"
    files = gcp.storage.list_with_prefix(bucket_name=BUCKET_NAME, prefix=prefix)

    for f in files:
        result = process_leaderboard_data(f, 50)
        print_latex_rows(f, result, 10)
        print_latex_rows(f, result, 50)
        print()
