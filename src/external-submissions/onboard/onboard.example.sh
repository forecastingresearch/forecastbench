#!/bin/bash
# Copy to onboard.sh (gitignored) and edit the values below, so commands with team details
# don't end up in shell history. Run from this directory in bash.

ORGANIZATION="Example Org"
TEAM_NAME=""              # optional internal label, never public
EMAILS="alice@example.com bob@example.com"
SERVICE_ACCOUNTS=""       # e.g. "uploader@project.iam.gserviceaccount.com"
ANONYMOUS=0               # 1 to register under an "Anonymous N" public name
MODE="TEST"               # TEST (default; no email sent) | PROD
SEND_EMAIL_IN_TEST=0      # 1 to send a rerouted [TEST] email while in TEST mode

set -a
. <(grep -v '^#' ../../../variables.mk | tr -d '\r')
set +a

ARGS=(--organization "$ORGANIZATION" --mode "$MODE")
[ -n "$TEAM_NAME" ] && ARGS+=(--team-name "$TEAM_NAME")
[ -n "$EMAILS" ] && ARGS+=(--emails $EMAILS)
[ -n "$SERVICE_ACCOUNTS" ] && ARGS+=(--service-accounts $SERVICE_ACCOUNTS)
[ "$ANONYMOUS" = "1" ] && ARGS+=(--anonymous)
[ "$SEND_EMAIL_IN_TEST" = "1" ] && ARGS+=(--send-email-in-test)

python main.py register "${ARGS[@]}"
