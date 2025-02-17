"""Get list of sources and number of unresolved questions."""

import os
import sys

import pandas as pd
from variables import BUCKET_NAME

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from src.helpers import resolution  # noqa: E402

dfm = pd.read_json(
    f"gs://{BUCKET_NAME}/question_metadata.jsonl",
    lines=True,
    convert_dates=False,
)

dfm = dfm[dfm["valid_question"].astype(bool)].reset_index(drop=True)

unresolved_ids = []
for source in sorted(set(resolution.MARKET_SOURCES).union(set(resolution.DATA_SOURCES))):
    print(source)
    filename = f"{source}_questions.jsonl"
    dfq = pd.read_json(
        f"gs://{BUCKET_NAME}/{filename}",
        lines=True,
        convert_dates=False,
    )
    new_ids = list(dfq[~(dfq["resolved"].astype(bool))]["id"])
    unresolved_ids += new_ids

unresolved_ids = [str(uid) for uid in unresolved_ids]
dfm = dfm[dfm["id"].astype(str).isin(unresolved_ids)].reset_index(drop=True)

# Make pivot table
dfm = dfm.pivot_table(index="category", columns="source", aggfunc="size", fill_value=0)

not_in_data_sources = sorted([col for col in dfm.columns if col not in resolution.DATA_SOURCES])
ordered_columns = not_in_data_sources + sorted(
    [col for col in resolution.DATA_SOURCES if col in dfm.columns]
)
dfm = dfm.reindex(columns=ordered_columns)
dfm["Total"] = dfm.sum(axis=1)
dfm_print = dfm.copy()
dfm_print.loc["Total"] = dfm_print.sum(axis=0)
print("\n\n", dfm_print)


def escape_latex_special_chars(text):
    """Escape latex chars."""
    return text.replace("&", r"\&")


def format_number(n):
    """Format each number."""
    return f"{n:,}"


latex_table = r""" \begin{tabularx}{.95\textwidth}{r*{10}{>{\raggedleft\arraybackslash}X}}\toprule &
     \rotatebox{90}{RFI} & \rotatebox{90}{Manifold} & \rotatebox{90}{Metaculus} &
     \rotatebox{90}{Polymarket} & \rotatebox{90}{ACLED} & \rotatebox{90}{DBnomics} &
     \rotatebox{90}{FRED} & \rotatebox{90}{Wikipedia} & \rotatebox{90}{Yahoo!} &
     \rotatebox{90}{Total} \\
     \midrule"""

other_row = ""
for index, row in dfm.iterrows():
    row_str = " & ".join(map(format_number, row.values))
    index_str = escape_latex_special_chars(str(index))
    row_to_add = f"\n    {index_str} & {row_str} \\\\"
    if str(index).lower() == "other":
        other_row = row_to_add
    else:
        latex_table += row_to_add

if other_row:
    latex_table += other_row

total_row_values = " & ".join(map(format_number, dfm.sum(axis=0)))
latex_table += rf"""
     \midrule
    Total & {total_row_values} \\
     \bottomrule
  \end{{tabularx}}"""

print("\n\n", latex_table)
