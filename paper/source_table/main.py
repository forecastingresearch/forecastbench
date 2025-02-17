"""Get list of sources and number of unresolved questions."""

import math
import os
import sys

import pandas as pd
from variables import BUCKET_NAME

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from src.helpers import resolution  # noqa: E402

question_counts = {}
df = pd.DataFrame()
for source in sorted(set(resolution.MARKET_SOURCES).union(set(resolution.DATA_SOURCES))):
    print(source)
    filename = f"{source}_questions.jsonl"
    dfq = pd.read_json(
        f"gs://{BUCKET_NAME}/{filename}",
        lines=True,
        convert_dates=False,
    )
    dfq = dfq[~dfq["resolved"]].reset_index(drop=True)
    df = pd.concat([df, pd.DataFrame([{"source": source, "n": len(dfq)}])], ignore_index=True)

df["comb"] = df["n"].apply(lambda x: math.comb(x, 2))
df_market = df[df["source"].isin(resolution.MARKET_SOURCES)].reset_index(drop=True)
df_dataset = df[df["source"].isin(resolution.DATA_SOURCES)].reset_index(drop=True)


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


def get_source_values(df, source_name):
    """Return counts for source from df."""
    row = df.loc[df["source"] == source_name]
    return row["n"].iloc[0], row["comb"].iloc[0]


# Market values
rfi_n, rfi_pairs = get_source_values(df_market, "infer")
manifold_n, manifold_pairs = get_source_values(df_market, "manifold")
metaculus_n, metaculus_pairs = get_source_values(df_market, "metaculus")
poly_n, poly_pairs = get_source_values(df_market, "polymarket")
market_total_n, market_total_pairs = get_source_values(df_market, "Total")

# Dataset values
acled_n, acled_pairs = get_source_values(df_dataset, "acled")
db_n, db_pairs = get_source_values(df_dataset, "dbnomics")
fred_n, fred_pairs = get_source_values(df_dataset, "fred")
wiki_n, wiki_pairs = get_source_values(df_dataset, "wikipedia")
yahoo_n, yahoo_pairs = get_source_values(df_dataset, "yfinance")
dataset_total_n, dataset_total_pairs = get_source_values(df_dataset, "Total")

# Overall total
total_n, total_pairs = get_source_values(df, "Total")


print(
    rf"""
  \begin{{tabular}}{{llrr}}
    \toprule
    Source & URL & $N$ & $\binom{{N}}{{2}}$ \\
    \midrule
    RFI & \href{{https://www.randforecastinginitiative.org/}}{{randforecastinginitiative.org}} & {rfi_n} & {rfi_pairs} \\
    Manifold Markets & \href{{https://manifold.markets}}{{manifold.markets}} & {manifold_n} & {manifold_pairs} \\
    Metaculus & \href{{https://www.metaculus.com}}{{metaculus.com}} & {metaculus_n} & {metaculus_pairs} \\
    Polymarket & \href{{https://polymarket.com}}{{polymarket.com}} & {poly_n} & {poly_pairs} \\
    \cmidrule{{3-4}}
    \multicolumn{{2}}{{l}}{{\textbf{{Market Total}}}} & {market_total_n} & {market_total_pairs} \\
    \midrule
    \midrule
    ACLED & \href{{https://acleddata.com}}{{acleddata.com}} & {acled_n} & {acled_pairs} \\
    DBnomics & \href{{https://db.nomics.world/}}{{db.nomics.world}} & {db_n} & {db_pairs} \\
    FRED & \href{{https://fred.stlouisfed.org}}{{fred.stlouisfed.org}} & {fred_n} & {fred_pairs} \\
    Wikipedia & \href{{https://www.wikipedia.org}}{{wikipedia.org}} & {wiki_n} & {wiki_pairs} \\
    Yahoo! Finance & \href{{https://finance.yahoo.com}}{{finance.yahoo.com}} & {yahoo_n} & {yahoo_pairs} \\
    \cmidrule{{3-4}}
    \multicolumn{{2}}{{l}}{{\textbf{{Dataset Total}}}} & {dataset_total_n} & {dataset_total_pairs} \\
    \midrule
    \midrule
    \multicolumn{{2}}{{l}}{{\textbf{{Question Bank Total}}}} & {total_n} & {total_pairs} \\
    \bottomrule
  \end{{tabular}}
"""  # noqa: B950
)
