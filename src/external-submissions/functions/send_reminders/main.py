"""
ForecastBench round reminder handler.

Sends reminder emails to all active teams for a given round date.
Intended to be called by Cloud Scheduler on forecast due dates.

Accepts JSON POST with:
  - round_date: YYYY-MM-DD (required)
  - dry_run: bool (optional, default false) — log but don't send emails

Environment variables:
  - UPLOAD_BUCKET: GCS bucket (default: forecastbench-submissions-dev)
"""

import json
import os
import traceback
from datetime import datetime, timezone

from google.cloud import firestore
from email_utils import send_reminder

UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "forecastbench-submissions-dev")

db = firestore.Client()


def send_reminders(request):
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    cors = {"Access-Control-Allow-Origin": "*"}

    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            return (json.dumps({"success": False, "error": "Invalid or missing JSON body"}), 400, cors)

        round_date = data.get("round_date", "").strip()
        if not round_date:
            round_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        dry_run = bool(data.get("dry_run", False))

        teams = list(db.collection("teams").where("active", "==", True).stream())

        sent = []
        skipped = []
        for doc in teams:
            team = doc.to_dict()
            team_name = team.get("team_name", "")
            display_org = team.get("organization", team_name)
            emails = team.get("emails", [])

            if not emails:
                skipped.append({"team_name": team_name, "reason": "no emails"})
                continue

            if dry_run:
                print(f"[dry_run] Would send reminder to {team_name} ({display_org}): {emails}")
                sent.append({"team_name": team_name, "emails": emails})
            else:
                try:
                    send_reminder(emails, round_date, UPLOAD_BUCKET, team_name, display_org)
                    sent.append({"team_name": team_name, "emails": emails})
                except Exception as e:
                    print(f"Failed to send reminder to {team_name}: {e}")
                    skipped.append({"team_name": team_name, "reason": str(e)})

        return (json.dumps({
            "success": True,
            "round_date": round_date,
            "dry_run": dry_run,
            "sent": len(sent),
            "skipped": len(skipped),
            "details": {"sent": sent, "skipped": skipped},
        }, indent=2), 200, cors)

    except Exception as e:
        print(traceback.format_exc())
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)
