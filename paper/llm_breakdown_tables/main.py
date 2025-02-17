"""Compare Super performance to best LLM performance."""

import os
import sys
from datetime import datetime

import pandas as pd
from variables import (
    BEST_LLM,
    BEST_LLM_FILE,
    FORECAST_DUE_DATE,
    PROCESSED_FORECAST_SETS_BUCKET_NAME,
    QUESTION_BANK_BUCKET_NAME,
    SUPER_FILE,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from src.helpers import resolution  # noqa: E402


def make_list_hashable(df, col):
    """Turn list into tuple to make it hashable."""
    df[col] = df[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df[col] = df[col].apply(lambda x: tuple() if pd.isna(x) else x)
    return df


def load_forecasts(filename):
    """Load forecast files."""
    gcp_filename = f"{PROCESSED_FORECAST_SETS_BUCKET_NAME}/{filename}"
    print(f"Downloading gs://{gcp_filename}")
    df = pd.read_json(f"gs://{gcp_filename}")["forecasts"].apply(pd.Series)
    df = df[
        [
            "id",
            "source",
            "score",
            "resolution_date",
        ]
    ]
    df = make_list_hashable(df, "id")
    return df


def escape_latex_special_chars(text):
    """Escape latex chars."""
    return text.replace("&", r"\&")


def make_category_table(df):
    """Print performance by category."""
    latex_table = rf"""\begin{{tabular}}{{lrrrr}}
   \toprule
   Category & N & {BEST_LLM} & Superforecaster & Difference \\
   \midrule"""

    for index, row in df.iterrows():
        latex_table += (
            f"\n    {escape_latex_special_chars(str(index))} & {int(row['N'])} & "
            f"{row['best_llm']:.3f} & {row['super']:.3f} & {row['diff']:.3f} \\\\"
        )

    latex_table += r"""
   \bottomrule
 \end{tabular}"""

    print("\n\n", latex_table)


def make_horizon_table(df):
    """Print horizon tables."""
    latex_table = rf"""\begin{{tabular}}{{lrrrr}}
   \toprule
   Forecast Horizon & N & {BEST_LLM} & Superforecaster & Difference \\
   \midrule"""

    forecast_due = datetime.fromisoformat(FORECAST_DUE_DATE)
    for index, row in df.iterrows():
        print(index)
        days = (index - forecast_due).days
        latex_table += (
            f"\n    {days}-day & {int(row['N'])} & {row['best_llm']:.3f} & "
            f"{row['super']:.3f} & {row['diff']:.3f} \\\\"
        )

    latex_table += r"""
    \bottomrule
 \end{tabular}"""
    print("\n\n", latex_table)


def get_correct_resolution_dates_for_market_questions(df):
    """Get the correct resolution date for market questions.

    This depends on when the question closed/resolved. Get this from the question bank and keep the
    first entry that has a resolution date after the question closed date.
    """
    columns = df.columns
    results = []
    for source in resolution.MARKET_SOURCES:
        print(source)
        df_source = df[df["source"] == source].copy()
        filename = f"{source}_questions.jsonl"
        dfq = pd.read_json(
            f"gs://{QUESTION_BANK_BUCKET_NAME}/{filename}",
            lines=True,
            convert_dates=False,
        )

        dfq["source"] = source
        dfq["id"] = dfq["id"].astype(str)

        # For unresolved market questions, only keep the last entry
        df_unresolved = dfq[~(dfq["resolved"].astype(bool))]
        df_source_unresolved = (
            pd.merge(
                df_source,
                df_unresolved,
                on=["source", "id"],
            )
            .sort_values(by=["source", "id", "resolution_date"])
            .drop_duplicates(subset=["id", "source"], keep="last")
        )

        # For unresolved market questions, only keep the last entry
        df_resolved = dfq[(dfq["resolved"].astype(bool))]
        df_source_resolved = pd.merge(
            df_source,
            df_resolved,
            on=["source", "id"],
        )
        df_source_resolved["market_info_close_datetime"] = pd.to_datetime(
            df_source_resolved["market_info_close_datetime"]
        )
        df_source_resolved["market_info_resolution_datetime"] = pd.to_datetime(
            df_source_resolved["market_info_resolution_datetime"]
        )
        df_source_resolved["last_date"] = df_source_resolved[
            ["market_info_close_datetime", "market_info_resolution_datetime"]
        ].min(axis=1)
        mask = (
            df_source_resolved["resolution_date"].values >= df_source_resolved["last_date"].values
        )
        df_source_resolved = (
            df_source_resolved[mask]
            .sort_values("resolution_date")
            .groupby("id", as_index=False)
            .first()
        )

        results.append(pd.concat([df_source_resolved[columns], df_source_unresolved[columns]]))
    return pd.concat(results, ignore_index=True)


def main():
    """Driver."""
    df_super = load_forecasts(SUPER_FILE)
    df_best_llm = load_forecasts(BEST_LLM_FILE)
    df = pd.merge(
        df_super,
        df_best_llm,
        on=[
            "source",
            "id",
            "resolution_date",
        ],
        suffixes=(
            "_super",
            "_llm",
        ),
    )
    df_metadata = pd.read_json(
        f"gs://{QUESTION_BANK_BUCKET_NAME}/question_metadata.jsonl",
        lines=True,
    )
    df = pd.merge(
        df,
        df_metadata,
        on=[
            "source",
            "id",
        ],
    ).drop(columns=["valid_question"])

    df["resolution_date"] = pd.to_datetime(df["resolution_date"])
    filtered_df = get_correct_resolution_dates_for_market_questions(df.copy())
    non_market_df = df[df["source"].isin(resolution.DATA_SOURCES)]
    df = (
        pd.concat([filtered_df, non_market_df], ignore_index=True)
        .sort_values(
            by=[
                "source",
                "id",
            ]
        )
        .reset_index(drop=True)
    )

    def get_scores(group, score_col):
        market_mean = group[group["source"].isin(resolution.MARKET_SOURCES)][score_col].mean()
        non_market_mean = group[~group["source"].isin(resolution.MARKET_SOURCES)][score_col].mean()

        if pd.isna(market_mean):
            return non_market_mean
        elif pd.isna(non_market_mean):
            return market_mean

        return (market_mean + non_market_mean) / 2

    def get_summary_table(grouping):
        summary_table = df.groupby(grouping)[["id", "score_llm", "score_super", "source"]].apply(
            lambda group: pd.Series(
                {
                    "N": group["id"].count(),
                    "best_llm": get_scores(
                        group,
                        "score_llm",
                    ),
                    "super": get_scores(
                        group,
                        "score_super",
                    ),
                }
            )
        )
        summary_table = summary_table.astype({"N": int})
        summary_table = summary_table.round(3)
        summary_table["diff"] = summary_table["best_llm"] - summary_table["super"]
        print("\n\n", summary_table)
        if grouping == "category":
            make_category_table(summary_table)
        elif grouping == "resolution_date":
            make_horizon_table(summary_table)

    get_summary_table("category")
    get_summary_table("source")
    get_summary_table("resolution_date")


if __name__ == "__main__":
    main()
