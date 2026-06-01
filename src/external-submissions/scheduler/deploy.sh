#!/bin/bash
# Sets up Cloud Scheduler jobs for ForecastBench automation.
#
# Jobs:
#   post-round-daily    — fires daily at 00:05 UTC; post_round auto-detects rounds past deadline
#   send-reminders      — fires on round due dates at 08:00 UTC; sends reminder emails to all teams
#
# Run this once after functions are deployed to GCP.
#
# Usage:
#   PROJECT=forecastbench-dev REGION=us-central1 bash deploy.sh
#
# To update the reminder schedule (e.g. change which days rounds fall on):
#   gcloud scheduler jobs update http send-reminders-weekly \
#     --project=forecastbench-dev --location=us-central1 \
#     --schedule="0 8 * * 2"   # e.g. Tuesdays at 08:00 UTC

PROJECT=${PROJECT:-forecastbench-dev}
REGION=${REGION:-us-central1}
POST_ROUND_URL="https://${REGION}-${PROJECT}.cloudfunctions.net/post-round"
REMINDERS_URL="https://${REGION}-${PROJECT}.cloudfunctions.net/send-reminders"

# post-round: daily at 00:05 UTC — processes any rounds past their deadline
gcloud scheduler jobs create http post-round-daily \
  --project="${PROJECT}" \
  --location="${REGION}" \
  --schedule="5 0 * * *" \
  --uri="${POST_ROUND_URL}" \
  --message-body="{}" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --time-zone="UTC" \
  --oidc-service-account-email="submissions@${PROJECT}.iam.gserviceaccount.com" \
  --description="Runs post_round daily at 00:05 UTC. Processes any rounds past their deadline."

# send-reminders: on round due dates at 08:00 UTC
# Default schedule: Tuesdays — update to match actual ForecastBench round calendar
gcloud scheduler jobs create http send-reminders-weekly \
  --project="${PROJECT}" \
  --location="${REGION}" \
  --schedule="0 8 * * 0" \
  --uri="${REMINDERS_URL}" \
  --message-body="{}" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --time-zone="UTC" \
  --oidc-service-account-email="submissions@${PROJECT}.iam.gserviceaccount.com" \
  --description="Sends round reminder emails to all active teams on forecast due dates. Rounds are bi-weekly on Sundays (starting 2025-03-02). PAUSED by default — resume after Houtan reviews."
