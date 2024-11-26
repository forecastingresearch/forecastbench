"""Compare Super performance to best LLM performance."""

import pandas as pd
from variables import (
    BEST_LLM_FILE,
    PROCESSED_FORECAST_SETS_BUCKET_NAME,
    QUESTION_BANK_BUCKET_NAME,
    SUPER_FILE,
)


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

    MARKET_SOURCES = [
        "manifold",
        "metaculus",
        "infer",
        "polymarket",
    ]

    filtered_df = (
        df[df["source"].isin(MARKET_SOURCES)]
        .sort_values(by=["source", "id", "resolution_date"])
        .drop_duplicates(subset=["id", "source"], keep="last")
    )
    non_market_df = df[~df["source"].isin(MARKET_SOURCES)]
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
    print(df)

    def get_scores(group, score_col):
        market_mean = group[group["source"].isin(MARKET_SOURCES)][score_col].mean()
        non_market_mean = group[~group["source"].isin(MARKET_SOURCES)][score_col].mean()

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
        print(summary_table)
        print()

    get_summary_table("category")
    get_summary_table("source")
    get_summary_table("resolution_date")


if __name__ == "__main__":
    main()
