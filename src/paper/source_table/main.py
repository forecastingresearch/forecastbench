"""Get list of sources and number of unresolved questions."""

import math
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import constants  # noqa: E402

question_counts = {}
df = pd.DataFrame()
for source in sorted(
    list(constants.FREEZE_QUESTION_SOURCES.keys())
    + [
        "dbnomics",
        "fred",
    ]
):
    print(source)
    filename = f"{source}_questions.jsonl"
    dfq = pd.read_json(
        f"gs://{constants.BUCKET_NAME}/{filename}",
        lines=True,
        convert_dates=False,
    )
    dfq = dfq[~dfq["resolved"]].reset_index(drop=True)
    df = pd.concat([df, pd.DataFrame([{"source": source, "n": len(dfq)}])], ignore_index=True)

df["comb"] = df["n"].apply(lambda x: math.comb(x, 2))
df_market = df[~df["source"].isin(constants.DATA_SOURCES)].reset_index(drop=True)
df_dataset = df[df["source"].isin(constants.DATA_SOURCES)].reset_index(drop=True)


def dfsum(df):
    """Sum df and add Total row."""
    sum_row = pd.DataFrame(df[["n", "comb"]].sum()).transpose()
    sum_row["source"] = "Total"

    df["n"] = df["n"].astype(int)
    df["comb"] = df["comb"].astype(int)

    df["n"] = df["n"].apply(lambda x: f"{x:,}")
    df["comb"] = df["comb"].apply(lambda x: f"{x:,}")

    sum_row["n"] = sum_row["n"].astype(int).apply(lambda x: f"{x:,}")
    sum_row["comb"] = sum_row["comb"].astype(int).apply(lambda x: f"{x:,}")

    df = pd.concat([df, sum_row], ignore_index=True)

    return df


print("\n\nFORECASTING PLATFORM")
df_market = dfsum(df_market)
print(df_market)

print("\n\nDATASET")
df_dataset = dfsum(df_dataset)
print(df_dataset)


print("\n\nTOTAL")
df = dfsum(df)
print(df)
