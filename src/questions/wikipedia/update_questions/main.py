"""Generate Wikipedia questions."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator, wikipedia  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikipedia"
filenames = data_utils.generate_filenames(source=source)


def create_resolution_file(dff, page, wid, id_field_value):
    """Create the resolution file. Overwrite it every time.

    filename is: `{source}/{wid}.jsonl`
    """
    max_date = dff["date"].max()

    id_field = page["fields"]["id"]
    value_field = page["fields"]["value"]

    df = dff[dff[id_field] == id_field_value].copy()
    df.rename(columns={id_field: "id", value_field: "value"}, inplace=True)
    df["id"] = wid

    # The case where some item has dropped off the list, mark as resolved
    if max_date > df["date"].max():
        all_dates = dff["date"].sort_values().unique()
        df_dates = df["date"].unique()
        resolved_date = next(date for date in all_dates if date not in df_dates)
        df_new_row = pd.DataFrame(
            [
                {
                    "id": wid,
                    "value": None,
                    "date": resolved_date,
                }
            ]
        )
        df = pd.concat([df, df_new_row], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    local_filename = f"/tmp/{wid}.jsonl"
    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME, local_filename=local_filename, destination_folder=source
    )
    return df


def add_to_dfq(dfq, dfr, page, wid, id_field_value):
    """Add the question to dfq."""

    def fill_template(page, page_key, values):
        fill_values = {field: values[field] for field in page["question"][1]}
        return page[page_key][0].format(**fill_values)

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
    value_at_freeze_datetime_explanation = fill_template(
        page=page, page_key="value_at_freeze_datetime_explanation", values=values
    )

    background = fill_template(page=page, page_key="background", values=values)

    row = {
        "id": wid,
        "question": question,
        "background": background,
        "source_resolution_criteria": "N/A",
        "source_begin_datetime": "N/A",
        "source_close_datetime": "N/A",
        "url": f"https://en.wikipedia.org/wiki/{page['page_title']}",
        "source_resolution_datetime": "N/A",
        "resolved": resolved,
        "continual_resolution": True,
        "forecast_horizons": [] if resolved else constants.FORECAST_HORIZONS_IN_DAYS,
        "value_at_freeze_datetime": value,
        "value_at_freeze_datetime_explanation": value_at_freeze_datetime_explanation,
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

    id_field = page["fields"]["id"]
    for id_field_value in dff[id_field].unique():
        wid = wikipedia.id_hash(id_root=question_id_root, id_field_value=id_field_value)
        try:
            dfr = create_resolution_file(dff=dff, page=page, wid=wid, id_field_value=id_field_value)
            dfq = add_to_dfq(dfq=dfq, dfr=dfr, page=page, wid=wid, id_field_value=id_field_value)
        except Exception as e:
            logger.warning(f"Couldn't add {question_id_root} {id_field_value}: {wid}")
            logger.warning(f"Exception encountered: {e}")

    return dfq


def resolve_questions_for_dropped_pages(dfq):
    """Resovlve questions for pages that have been removed from page.PAGES.

    If we ever remove pages, we want to stop sampling from those questions.
    Simply resolve them.
    """
    id_roots = [d["id_root"] for d in wikipedia.PAGES]
    for index, row in dfq.iterrows():
        d = wikipedia.id_unhash(hash_key=row["id"])
        if d is None or d.get("id_root") not in id_roots:
            dfq.loc[index, "resolved"] = True
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
    return dfq


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    logger.info("Downloading previously-fetched Wikipedia data from Cloud.")

    hash_filename = "hash_mapping.json"
    remote_filename = f"{source}/{hash_filename}"
    local_hash_filename = f"/tmp/{hash_filename}"
    gcp.storage.download_no_error_message_on_404(
        bucket_name=constants.BUCKET_NAME,
        filename=remote_filename,
        local_filename=local_hash_filename,
    )
    if os.path.getsize(local_hash_filename) > 0:
        with open(local_hash_filename, "r") as file:
            wikipedia.hash_mapping = json.load(file)

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
        bucket_name=constants.BUCKET_NAME,
        local_filename=filenames["local_question"],
    )

    # Upload hash
    with open(local_hash_filename, "w") as file:
        json.dump(wikipedia.hash_mapping, file, indent=4)

    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME,
        local_filename=local_hash_filename,
        destination_folder=source,
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
