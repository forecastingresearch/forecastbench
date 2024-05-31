"""Helper functions for resolution code."""

import pandas as pd


def split_dataframe_on_source(df, source):
    """Return tuple of this data source from dataframe and everything else."""
    mask = df["source"] == source
    return df[mask].copy(), df[~mask].copy()


def is_combo(row):
    """Tell whether or not id is a combo question."""
    return True if isinstance(row["id"], tuple) else False


def combo_change_sign(value: bool, sign: int):
    """Change direction of bool value given sign (-1 or 1)."""
    return int(value if sign == 1 else not value)


def get_question(dfq, mid):
    """Get question from dfq."""
    dftmp = dfq[dfq["id"] == mid]
    return None if dftmp.empty else dftmp.iloc[0]


def make_list_hashable(df, col):
    """Turn list into tuple to make it hashable."""
    df[col] = df[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df[col] = df[col].apply(lambda x: tuple() if pd.isna(x) else x)
    return df


def make_columns_hashable(df):
    """Make columns that have array type into tuples."""
    for col in ["id", "direction"]:
        if col in df.columns:
            df = make_list_hashable(df, col)
    return df


def standardize_direction(val):
    """Try to standardize the value in direction which is given by the user."""
    return tuple() if val is None or val == "N/A" else val
