"""Generate Wikipedia questions."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator, env, wikipedia  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"
filenames = data_utils.generate_filenames(source=source)


def create_resolution_file(dff, page, wid, question_key: pd.Series):
    """Create the resolution file. Overwrite it every time.

    filename is: `{source}/{wid}.jsonl`
    """
    id_field = page["fields"]["id"]
    value_field = page["fields"]["value"]

    mask = pd.Series(True, index=dff.index)
    for field_name in question_key.index:
        mask &= dff[field_name] == question_key[field_name]

    df = dff[mask].copy()
    if df["date"].max().date() < constants.QUESTION_BANK_DATA_STORAGE_START_DATE:
        # Fetching more data than we need for naive forecasts. Don't need to create resolution
        # files for events that are no longer current
        return None

    df.rename(columns={id_field: "id", value_field: "value"}, inplace=True)
    df["id"] = wid

    def fill_missing_with_nan(df, dff):
        """Sometimes values drop out of the table then reappear.

        This could be for valid reasons, e.g. someone had a world record, lost it, then got it
        again.

        This could be for invalid reasons: a name change, e.g. Erigaisi Arjun -> Arjun Erigaisi

        Either way, fill these with nan. Invalid reasons will need to be caught by hand and
        invalidated in `src/helpers/wikipedia.py` IDS_TO_NULLIFY.
        """
        # fill in nan where the item has dropped out of the table
        all_dates = dff["date"].sort_values().unique()
        all_dates = all_dates[all_dates >= constants.QUESTION_BANK_DATA_STORAGE_START_DATETIME]
        next_after_df_max_date = all_dates[all_dates > df["date"].max()]
        max_cutoff = (
            next_after_df_max_date.min() if len(next_after_df_max_date) > 0 else df["date"].max()
        )
        all_dates = all_dates[(all_dates <= max_cutoff) & (all_dates >= df["date"].min())]
        drop_out_dates = []
        for drop_out_date in [date for date in all_dates if date not in df["date"].unique()]:
            drop_out_dates.append(
                {
                    "id": wid,
                    "value": None,
                    "date": drop_out_date,
                }
            )
        df = pd.concat([df, pd.DataFrame(drop_out_dates)], ignore_index=True)
        return df

    df = fill_missing_with_nan(df=df, dff=dff)

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values(by="date", ignore_index=True)

    local_filename = f"/tmp/{wid}.jsonl"
    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_filename,
        destination_folder=source,
    )
    return df


def add_to_dfq(dfq, dfr, page, wid, id_field_value):
    """Add the question to dfq."""

    def fill_template(page, page_key, values):
        fill_values = {field: values[field] for field in page["question"][1]}
        # Always maintain resolution_date and forecast_due_date when formatting the string
        default_values = {
            "resolution_date": "{resolution_date}",
            "forecast_due_date": "{forecast_due_date}",
        }
        combined_fill_values = {**default_values, **fill_values}
        return page[page_key][0].format(**combined_fill_values)

    dfr = dfr.sort_values(by="date")
    value = dfr.iloc[-1]["value"]

    resolved = value is None
    if "is_resolved_func" in page.keys():
        resolved = eval(f"wikipedia.{page['is_resolved_func']}(value)")

    if "value_func" in page.keys():
        value = eval(f"wikipedia.{page['value_func']}(value)")

    values = {
        "id": id_field_value,
        "value": value,
    }
    question = fill_template(page=page, page_key="question", values=values)
    freeze_datetime_value_explanation = fill_template(
        page=page, page_key="freeze_datetime_value_explanation", values=values
    )

    background = fill_template(page=page, page_key="background", values=values)

    row = {
        "id": wid,
        "question": question,
        "background": background,
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "url": f"https://en.wikipedia.org/wiki/{page['page_title']}",
        "market_info_resolution_datetime": "N/A",
        "resolved": resolved,
        "forecast_horizons": [] if resolved else constants.FORECAST_HORIZONS_IN_DAYS,
        "freeze_datetime_value": value,
        "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
    }

    df_question = pd.DataFrame([row])
    if row["id"] not in dfq["id"].values:
        return df_question if dfq.empty else pd.concat([dfq, df_question], ignore_index=True)

    # Update the row where `dfq["id"] == df_question["id"]`
    dfq = dfq.set_index("id")
    df_question = df_question.set_index("id")
    dfq.update(df_question)
    return dfq.reset_index()


def update_page_questions(page, dfq, dff):
    """Update questions and resolutions for the provided Wikipedia page."""
    question_id_root = page.get("id_root")
    logger.info(f"Updating questions for for {question_id_root}.")

    # The `key` field of each page contains the unique entry/entries that make a question.
    # See issue #123.
    id_fields = [page["fields"][key] for key in page["key"]]
    for _, row in dff[id_fields].drop_duplicates().iterrows():
        id_field_value_for_wid = str(row.iloc[0]) if len(row) == 1 else str(sorted(row))
        wid = wikipedia.id_hash(id_root=question_id_root, id_field_value=id_field_value_for_wid)
        try:
            dfr = create_resolution_file(dff=dff, page=page, wid=wid, question_key=row)
            if dfr is not None:
                dfq = add_to_dfq(
                    dfq=dfq,
                    dfr=dfr,
                    page=page,
                    wid=wid,
                    id_field_value=row[page["fields"]["id"]],
                )
        except Exception as e:
            logger.warning(f"Couldn't add {question_id_root} {wid}: {row}")
            logger.warning(f"Exception encountered: {e}")

    return dfq


def resolve_questions_for_dropped_pages(dfq):
    """Resolve questions for pages that have been removed from page.PAGES.

    If we ever remove pages, we want to stop sampling from those questions.
    Simply resolve them.
    """
    id_roots = [d["id_root"] for d in wikipedia.PAGES]
    for index, row in dfq.iterrows():
        d = wikipedia.id_unhash(hash_key=row["id"])
        if d is None or d.get("id_root") not in id_roots:
            dfq.loc[index, "resolved"] = True
    return dfq


def resolve_questions_for_id_transformations(dfq):
    """Resolve questions for keys in `wikipedia.transform_id_mapping`.

    `wikipedia.transform_id_mapping` contains keys of questions that were erroneously made for one
    reason or another. Those keys point to the correct IDs for those questions. When the correct ID
    is resolved, ensure the original question ID is resolved too.
    """
    for key, value in wikipedia.transform_id_mapping.items():
        resolved_series = dfq[dfq["id"] == value]["resolved"]
        if not resolved_series.empty and resolved_series.iloc[0]:
            dfq.loc[dfq["id"] == key, "resolved"] = True
            logger.info(f"Resolving: {key}")
    return dfq


def update_all_forecast_questions(dfq):
    """For each set of pages that is still being updated, download the associated fetch file."""
    for page in wikipedia.PAGES:
        filename = wikipedia.get_fetch_filename(page.get("id_root"))
        local_filename = f"/tmp/{filename}"
        remote_filename = f"{wikipedia.fetch_directory}/{filename}"
        dff = data_utils.download_and_read(
            filename=remote_filename, local_filename=local_filename, df_tmp=pd.DataFrame(), dtype={}
        )
        if not dff.empty:
            dff["date"] = pd.to_datetime(dff["date"])
            if "clean_func" in page.keys():
                dff = eval(f"wikipedia.{page['clean_func']}(dff)")
            dfq = update_page_questions(page=page, dfq=dfq, dff=dff)

    dfq = resolve_questions_for_dropped_pages(dfq=dfq)
    dfq = resolve_questions_for_id_transformations(dfq=dfq)
    return dfq


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    logger.info("Downloading previously-fetched Wikipedia data from Cloud.")

    wikipedia.populate_hash_mapping()

    # We'll overwrite all questions for wikipedia.PAGES that we are still getting
    # Only pull this in to save pages we've stopped fetching for one reason or another.
    dfq = data_utils.get_data_from_cloud_storage(source=source, return_question_data=True)

    # Update the existing questions
    dfq = update_all_forecast_questions(dfq)

    logger.info(f"Found {len(dfq)} questions.")

    # Save
    with open(filenames["local_question"], "w", encoding="utf-8") as f:
        for record in dfq.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload Questions
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_question"],
    )

    # Upload hash
    wikipedia.upload_hash_mapping()

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
