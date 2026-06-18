#!/bin/bash
# Edge-case driver: copy to edge_cases.sh (gitignored) and run against a dev project only.
# Loops through labeled argument sets in TEST mode (no email is ever attempted), so a
# reviewer can see exactly which cases were exercised. Every case here is also covered in
# the unit test suite (src/tests/test_onboard.py).
#
# Labels starting with "ok" are expected to succeed; "fail" cases must exit non-zero.
#
# NB: GCS IAM only accepts EXISTING Google identities, so the ok-cases use $EMAIL (a real
# account — defaults to SMTP_USER) and $SERVICE_ACCOUNT (the real submissions SA).
# Fictional addresses appear only in cases expected to fail. The non-Google-email warning
# path can't be demoed live for the same reason; it is covered by the unit suite.
#
# NB: the loop uses `eval` so case strings can contain quoted arguments. Keep the CASES
# table admin-authored; never build it from external input.

set -a
. <(grep -v '^#' ../../../variables.mk | tr -d '\r')
set +a

EMAIL="${SMTP_USER}"
SERVICE_ACCOUNT="${SUBMISSIONS_SERVICE_ACCOUNT:-${SUBMISSIONS_SA_EMAIL:-}}"

if [ -z "$EMAIL" ]; then
    echo "FAIL: SMTP_USER not set in variables.mk (needed as the real test identity)."
    exit 1
fi
if [ -z "$SERVICE_ACCOUNT" ]; then
    echo "FAIL: no submissions service account in variables.mk (needed for the SA cases)."
    exit 1
fi

# The anonymous case needs the counter; harmless if it already exists.
python init_counters.py --anon-count 0 || true

CASES=(
  "ok-minimal             |--organization MinimalOrg --emails $EMAIL"
  "ok-anonymous           |--organization 'Secret Labs' --anonymous --emails $EMAIL"
  "ok-service-account-only|--organization 'Bot Org' --service-accounts $SERVICE_ACCOUNT"
  "ok-emails-plus-sa      |--organization 'Mixed Org' --emails $EMAIL --service-accounts $SERVICE_ACCOUNT"
  "ok-team-name           |--organization 'Acme' --team-name acme-alpha --emails $EMAIL"
  "ok-same-org-twice      |--organization 'Acme' --emails $EMAIL"
  "ok-unicode-org         |--organization 'Gréta Łabs' --emails $EMAIL"
  "ok-punctuation-org     |--organization 'cmcc.vc' --emails $EMAIL"
  "ok-long-org            |--organization 'An Extremely Long Organization Name That Exceeds The Slug Limit For Sure' --emails $EMAIL"
  "ok-send-test-email     |--organization 'Mail Org' --emails $EMAIL --send-email-in-test"
  "fail-duplicate-teamname|--organization 'Acme' --team-name acme-alpha --emails $EMAIL"
  "fail-empty-org         |--organization '' --emails $EMAIL"
  "fail-no-emails-no-sa   |--organization 'Nobody Org'"
  "fail-invalid-email     |--organization 'Typo Org' --emails not-an-email"
  "fail-nonexistent-acct  |--organization 'Ghost Org' --emails a@dummy-domain-x92ah8.com"
)

PASS=0
FAIL=0
for case in "${CASES[@]}"; do
    label="$(echo "${case%%|*}" | xargs)"
    args="${case#*|}"
    echo "=== ${label} ==="
    if eval python main.py register $args --mode TEST; then
        outcome="succeeded"
    else
        outcome="failed"
    fi
    if { [[ $label == ok-* ]] && [ "$outcome" = "succeeded" ]; } ||
       { [[ $label == fail-* ]] && [ "$outcome" = "failed" ]; }; then
        echo "--- ${label}: ${outcome} (as expected)"
        PASS=$((PASS + 1))
    else
        echo "--- ${label}: ${outcome} (UNEXPECTED)"
        FAIL=$((FAIL + 1))
    fi
    echo
done

echo "${PASS} as expected, ${FAIL} unexpected."
[ "$FAIL" -eq 0 ]
