"""Wikipedia resolution functions."""

import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import dates, resolution, wikipedia  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"


def resolve_question(mid, forecast_due_date, resolution_date, dfq, dfr):
    """Resolve the forecast on the question given by mid."""
    question = resolution.get_question(dfq, mid)
    if question is None:
        logger.warn(f"Wikipedia: could NOT find {mid}")
        return np.nan

    return wikipedia.resolve(
        mid=mid,
        dfr=dfr,
        forecast_due_date=forecast_due_date,
        resolution_date=resolution_date,
    )


def resolve(df, dfq, dfr):
    """Resolve Wikipedia questions."""
    logger.info("Resolving Wikipedia questions.")
    wikipedia.populate_hash_mapping()

    # Forward fill dfr to yesterday (only have data until yesterday, the last complete day)
    dfr = dfr.sort_values(by=["id", "date"])
    dfr = dfr.drop_duplicates(subset=["id", "date"])
    yesterday = dates.get_date_today() - timedelta(days=1)
    yesterday = pd.Timestamp(yesterday)
    result_df = pd.DataFrame()
    for unique_id in dfr["id"].unique():
        temp_df = dfr[dfr["id"] == unique_id].set_index("date").resample("D").ffill().reset_index()
        if temp_df["date"].max() < yesterday:
            last_value = temp_df.iloc[-1]["value"]
            additional_days = pd.date_range(
                start=temp_df["date"].max() + timedelta(days=1), end=yesterday
            )
            additional_df = pd.DataFrame(
                {"date": additional_days, "id": unique_id, "value": last_value}
            )
            temp_df = pd.concat([temp_df, additional_df])

        result_df = pd.concat([result_df, temp_df])

    dfr = result_df.sort_values(by=["id", "date"]).reset_index(drop=True)

    # Only pick out data relevant to wikipedia
    mask = (df["source"] == "wikipedia") & (df["resolution_date"] <= yesterday)
    for index, row in df[mask].iterrows():
        forecast_due_date = row["forecast_due_date"].date()
        resolution_date = row["resolution_date"].date()
        if not resolution.is_combo(row):
            value = resolve_question(
                mid=row["id"],
                forecast_due_date=forecast_due_date,
                resolution_date=resolution_date,
                dfq=dfq,
                dfr=dfr,
            )
        else:
            value1 = resolve_question(
                mid=row["id"][0],
                forecast_due_date=forecast_due_date,
                resolution_date=resolution_date,
                dfq=dfq,
                dfr=dfr,
            )
            value2 = resolve_question(
                mid=row["id"][1],
                forecast_due_date=forecast_due_date,
                resolution_date=resolution_date,
                dfq=dfq,
                dfr=dfr,
            )
            value = resolution.combo_change_sign(
                value1, row["direction"][0]
            ) * resolution.combo_change_sign(value2, row["direction"][1])
        df.at[index, "resolved_to"] = float(value)
    df.loc[mask, "resolved"] = True
    return df
