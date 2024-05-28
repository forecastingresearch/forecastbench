"""Get list of sources and number of unresolved questions."""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import constants  # noqa: E402

dfm = pd.read_json(
    f"gs://{constants.BUCKET_NAME}/question_metadata.jsonl",
    lines=True,
    convert_dates=False,
)

dfm = dfm[dfm["valid_question"].astype(bool)].reset_index(drop=True)

unresolved_ids = []
for source in [
    "infer",
    "manifold",
    "metaculus",
    "polymarket",
    "acled",
    "dbnomics",
    "wikipedia",
    "yfinance",
]:
    print(source)
    filename = f"{source}_questions.jsonl"
    dfq = pd.read_json(
        f"gs://{constants.BUCKET_NAME}/{filename}",
        lines=True,
        convert_dates=False,
    )
    new_ids = list(dfq[~(dfq["resolved"].astype(bool))]["id"])
    unresolved_ids += new_ids

unresolved_ids = [str(uid) for uid in unresolved_ids]
dfm = dfm[dfm["id"].astype(str).isin(unresolved_ids)].reset_index(drop=True)

# Make pivot table
dfm = dfm.pivot_table(index="category", columns="source", aggfunc="size", fill_value=0)

not_in_data_sources = sorted([col for col in dfm.columns if col not in constants.DATA_SOURCES])
ordered_columns = not_in_data_sources + sorted(
    [col for col in constants.DATA_SOURCES if col in dfm.columns]
)
dfm = dfm.reindex(columns=ordered_columns)
dfm["fred"] = 0
dfm.loc["Economics & Business", "fred"] = 157
dfm["Total"] = dfm.sum(axis=1)
dfm.loc["Total"] = dfm.sum(axis=0)
print(dfm)


# %                        infer  manifold  metaculus  polymarket  acled  dbnomics  wikipedia  yfinance  fred  Total # noqa: B950
# % Arts & Recreation          0        46         12          28      0         0          0         0     0     86 # noqa: B950
# % Economics & Business       1        15         82          56      0         0          0       501   157    812 # noqa: B950
# % Environment & Energy       0         2         46           1      0        61          0         0     0    110 # noqa: B950
# % Healthcare & Biology       0         9         75           1      0         0        213         0     0    298 # noqa: B950
# % Other                      0         0          2           0      0         0          0         0     0      2 # noqa: B950
# % Politics & Governance     11        37        223         470      0         0          0         0     0    741 # noqa: B950
# % Science & Tech             7        91        207          58      0         0         18         0     0    381 # noqa: B950
# % Security & Defense        13        13        153          10   3108         0          0         0     0   3297 # noqa: B950
# % Sports                     0        63         23          33      0         0         90         0     0    209 # noqa: B950
# % Total                     32       276        823         657   3108        61        321       501   157   5936 # noqa: B950
