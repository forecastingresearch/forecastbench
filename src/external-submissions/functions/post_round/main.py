"""
ForecastBench post-round processor.

Run after a round's deadline passes. Does the following:
  1. Copies all team submissions for the round → interstitial bucket
  2. Copies fully valid files → processing (final) bucket
  3. Sends one digest email to FRI with full round summary
  4. Emails each team about their invalid files
  5. Archives all team submissions to the history bucket
  6. Clears team folders (keeps .keep placeholder)

Environment variables:
  UPLOAD_BUCKET        source (default: forecastbench-submissions-dev)
  INTERSTITIAL_BUCKET  staging (default: forecastbench-submissions-interstitial-dev)
  FORECAST_SETS_BUCKET    final for scoring (default: forecastbench-forecast-sets-dev)
  HISTORY_BUCKET       archive (default: forecastbench-submissions-history-dev)
  FRI_EMAIL            digest recipient (default: forecastbench@forecastingresearch.org)
  BUILD_ENV            dev or prod
  MOCK_DATE            YYYY-MM-DD, only used when BUILD_ENV=dev
"""

import json
import os

from google.cloud import storage, firestore
from email_utils import send_round_digest, send_round_processed
from utils import is_past_deadline

UPLOAD_BUCKET       = os.environ.get("UPLOAD_BUCKET",       "forecastbench-submissions-dev")
INTERSTITIAL_BUCKET = os.environ.get("INTERSTITIAL_BUCKET", "forecastbench-submissions-interstitial-dev")
FORECAST_SETS_BUCKET   = os.environ.get("FORECAST_SETS_BUCKET",   "forecastbench-forecast-sets-dev")
HISTORY_BUCKET      = os.environ.get("HISTORY_BUCKET",      "forecastbench-submissions-history-dev")
FRI_EMAIL           = os.environ.get("FRI_EMAIL",           "forecastbench@forecastingresearch.org")
BUILD_ENV           = os.environ.get("BUILD_ENV",           "prod")


def _process_round(db, gcs, round_date):
    src_bucket   = gcs.bucket(UPLOAD_BUCKET)
    inter_bucket = gcs.bucket(INTERSTITIAL_BUCKET)
    final_bucket = gcs.bucket(FORECAST_SETS_BUCKET)
    hist_bucket  = gcs.bucket(HISTORY_BUCKET)

    all_blobs = list(src_bucket.list_blobs())
    round_blobs = [
        b for b in all_blobs
        if b.name.endswith(".json")
        and "/" in b.name
        and not b.name.startswith("rejected/")
        and b.name.split("/")[-1].startswith(round_date)
    ]

    if not round_blobs:
        return {"round_date": round_date, "message": "No submissions found.", "teams": []}

    # Load Firestore submission records keyed by filename
    sub_records = {}
    for sub in db.collection("submissions").where("round_date", "==", round_date).stream():
        d = sub.to_dict()
        sub_records[d["filename"]] = d

    # Group blobs by team
    teams_map = {}
    for blob in round_blobs:
        team_name = blob.name.split("/")[0]
        teams_map.setdefault(team_name, []).append(blob)

    team_summaries = []

    for team_name, blobs in sorted(teams_map.items()):
        team_docs = list(db.collection("teams").where("team_name", "==", team_name).stream())
        team = team_docs[0].to_dict() if team_docs else {}
        org_name = team.get("organization", team_name)
        human_emails = [
            e for e in team.get("emails", [])
            if e not in team.get("service_accounts", [])
        ]

        valid_files = []
        invalid_files = []

        for blob in blobs:
            filename = blob.name.split("/")[-1]
            record = sub_records.get(filename, {})
            is_valid = record.get("valid", False)
            errors = record.get("errors", [])

            # 1. All files → interstitial
            try:
                src_bucket.copy_blob(blob, inter_bucket, new_name=blob.name)
            except Exception as e:
                errors = [f"Failed to copy to interstitial: {e}"] + errors

            # 2. Valid files → processing/final
            if is_valid:
                try:
                    src_bucket.copy_blob(blob, final_bucket, new_name=blob.name)
                    valid_files.append(filename)
                except Exception as e:
                    invalid_files.append({"filename": filename, "errors": [f"Copy to processing failed: {e}"]})
            else:
                invalid_files.append({"filename": filename, "errors": errors})

            # 3. Archive to history bucket under round_date/
            try:
                src_bucket.copy_blob(blob, hist_bucket, new_name=f"{round_date}/{blob.name}")
            except Exception:
                pass

        # 4. Clear team folder — delete round files, keep .keep
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass

        # Email team about their results
        if human_emails:
            send_round_processed(
                human_emails, round_date,
                valid_count=len(valid_files),
                invalid_details=invalid_files,
            )

        # Mark submissions transferred in Firestore
        for sub in (db.collection("submissions")
                      .where("round_date", "==", round_date)
                      .where("team_name", "==", team_name)
                      .stream()):
            sub.reference.update({
                "transferred": True,
                "transferred_at": firestore.SERVER_TIMESTAMP,
            })

        team_summaries.append({
            "team_name": team_name,
            "organization": org_name,
            "valid_files": valid_files,
            "invalid_files": invalid_files,
        })

    # 5. One digest email to FRI
    send_round_digest(FRI_EMAIL, round_date, team_summaries, INTERSTITIAL_BUCKET)

    # Log transfer in Firestore
    db.collection("round_transfers").add({
        "round_date": round_date,
        "teams": team_summaries,
        "timestamp": firestore.SERVER_TIMESTAMP,
    })

    return {"round_date": round_date, "teams": team_summaries}


def post_round(request):
    cors = {"Access-Control-Allow-Origin": "*"}

    try:
        db = firestore.Client()
        gcs = storage.Client()

        data = request.get_json(silent=True) or {}
        round_date = data.get("round_date", "").strip()
        force = data.get("force", False) and BUILD_ENV == "dev"

        if round_date:
            if not force and not is_past_deadline(round_date):
                return (json.dumps({
                    "success": False,
                    "error": f"Round {round_date} deadline has not passed yet.",
                }), 400, cors)
            result = _process_round(db, gcs, round_date)
            return (json.dumps({"success": True, "results": [result]}, indent=2), 200, cors)

        else:
            already_transferred = set(
                doc.to_dict().get("round_date")
                for doc in db.collection("round_transfers").stream()
            )
            pending_rounds = set()
            for sub in db.collection("submissions").stream():
                rd = sub.to_dict().get("round_date")
                if rd and rd not in already_transferred and is_past_deadline(rd):
                    pending_rounds.add(rd)

            if not pending_rounds:
                return (json.dumps({
                    "success": True,
                    "message": "No pending rounds to process.",
                }), 200, cors)

            results = [_process_round(db, gcs, rd) for rd in sorted(pending_rounds)]
            return (json.dumps({"success": True, "results": results}, indent=2), 200, cors)

    except Exception as e:
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)
