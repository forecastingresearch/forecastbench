"""ACLED resolution functions."""

import logging
import os
import sys

import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import acled, resolution  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "acled"


def make_resolution_df():
    """Prepare ACLED data for resolution."""
    dfr, _, _ = acled.download_dff_and_prepare_dfr()
    return dfr


def resolve_question(mid, forecast_submitted_date, forecast_evaluation_date, dfq, dfr):
    """Resolve an individual ACLED question."""
    question = resolution.get_question(dfq, mid)
    if question is None:
        logger.warn(f"ACLED: could NOT find {mid}")
        return np.nan

    d = acled.id_unhash(mid)

    return acled.resolve(
        **d,
        dfr=dfr,
        forecast_due_date=forecast_submitted_date,
        resolution_date=forecast_evaluation_date,
    )


def resolve(df, dfq, dfr):
    """Resolve ACLED questions."""
    logger.info("Resolving ACLED questions.")
    acled.populate_hash_mapping()
    max_date = dfr["event_date"].max()
    mask = (df["source"] == "acled") & (df["forecast_evaluation_date"] <= max_date)
    for index, row in df[mask].iterrows():
        forecast_submitted_date = row["forecast_submitted_date"].date()
        forecast_evaluation_date = row["forecast_evaluation_date"].date()
        if not resolution.is_combo(row):
            value = resolve_question(
                mid=row["id"],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
        else:
            value1 = resolve_question(
                mid=row["id"][0],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
            value2 = resolve_question(
                mid=row["id"][1],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
            value = resolution.combo_change_sign(
                value1, row["direction"][0]
            ) * resolution.combo_change_sign(value2, row["direction"][1])
        df.at[index, "resolved_to"] = value
    df.loc[mask, "resolved"] = True
    return df
