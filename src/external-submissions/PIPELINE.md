# ForecastBench Submission Pipeline

**GCP project:** `forecastbench-dev`  
**Region:** `us-central1`  
**Submissions bucket:** `forecastbench-submissions-dev`  
**Interstitial bucket:** `forecastbench-submissions-interstitial-dev`  
**History bucket:** `forecastbench-submissions-history-dev`  
**Service account:** `submissions@forecastbench-dev.iam.gserviceaccount.com`

---

## Overview

1. Admin registers a new team via `onboard-team` → team gets a GCS folder + welcome email
2. Team uploads their forecast file to their GCS folder
3. `on-submission` fires automatically, validates the file, emails the team the result
4. Cloud Scheduler calls `send-reminders` on round due dates at 08:00 UTC to remind all active teams
5. Cloud Scheduler calls `post-round` nightly at 00:05 UTC — processes rounds past their deadline
6. `post-round` copies valid submissions to the processing bucket for scoring and emails each team

**Sandbox** (`forecastbench-johan` project): identical pipeline with separate buckets. Used for testing. Never touch `forecastbench-dev` data from the sandbox.

---

## Prerequisites

You need the GCP CLI (`gcloud`) installed and authenticated as `johan@forecastingresearch.org`:

```powershell
gcloud auth print-identity-token
```

If it errors, run `gcloud auth login johan@forecastingresearch.org`.

---

## Register a new team

```powershell
$token = gcloud auth print-identity-token
$headers = @{ "Authorization" = "Bearer $token" }
$body = '{
  "organization": "DeepMind",
  "model": "Gemini 2.0",
  "model_organization": "Google DeepMind",
  "emails": ["researcher@deepmind.com"],
  "anonymous": false
}'
Invoke-RestMethod -Method POST -Headers $headers -Body $body -ContentType "application/json" `
  -Uri "https://us-central1-forecastbench-dev.cloudfunctions.net/onboard-team"
```

**Fields:**

| Field | Required | Notes |
| --- | --- | --- |
| `organization` | Yes | Real org name. Stored privately if anonymous. |
| `model` | Yes | The model they're submitting. |
| `model_organization` | Yes | Who built the model. |
| `emails` | Yes | List of team member emails. Must be Google accounts for GCS upload access. |
| `service_accounts` | No | GCP service accounts to also grant upload access. |
| `anonymous` | No | Default false. If true, public name becomes "Anonymous N". |

**What happens:**

- Team is assigned a permanent ID (`team1`, `team2`, ...) — also used as their GCS folder name
- Their emails are granted `storage.objectAdmin` on their folder only (IAM condition)
- They receive a welcome email with their folder path and upload instructions

**Response example:**

```json
{
  "success": true,
  "team_name": "team3",
  "organization": "DeepMind",
  "upload_folder": "gs://forecastbench-submissions-dev/team3/"
}
```

**Notes:**

- One org can register multiple teams (each with a different model). No duplicate check.
- If any email is not a Google account, registration succeeds but the response includes a warning — those members won't be able to upload via gsutil.
- Anonymous teams: their GCS folder uses the team ID, not the org name. Real org is always in Firestore under `original_organization`.

---

## Remove a team

Revokes GCS access and marks the team inactive in Firestore.

```powershell
$token = gcloud auth print-identity-token
$headers = @{ "Authorization" = "Bearer $token" }
Invoke-RestMethod -Method DELETE -Headers $headers `
  -Body '{"team_name": "team3"}' -ContentType "application/json" `
  -Uri "https://us-central1-forecastbench-dev.cloudfunctions.net/onboard-team"
```

---

## Send round reminders manually

Send reminder emails to all active teams for a specific round date:

```powershell
$token = gcloud auth print-identity-token
$headers = @{ "Authorization" = "Bearer $token" }
Invoke-RestMethod -Method POST -Headers $headers `
  -Body '{"round_date": "2026-05-10"}' -ContentType "application/json" `
  -Uri "https://us-central1-forecastbench-dev.cloudfunctions.net/send-reminders"
```

Omit `round_date` to default to today. Add `"dry_run": true` to log without sending.

The `send-reminders-weekly` Cloud Scheduler job calls this automatically on round due dates (see Scheduler section).

---

## Validate a file without submitting

```bash
curl -X POST -F "file=@your-forecast.json" \
  https://us-central1-forecastbench-dev.cloudfunctions.net/validate-forecast
```

---

## Team uploads their forecast

### Option A: Browser

1. Go to `console.cloud.google.com/storage/browser/forecastbench-submissions-dev`
2. Sign in with the Google account that was registered
3. Click into their team folder (e.g. `team3/`)
4. Upload the file — filename must match `{round_date}.{organization}.{N}.json`

### Option B: Terminal

```bash
gsutil cp your-forecast.json gs://forecastbench-submissions-dev/team3/2026-05-10.DeepMind.1.json
gcloud storage cp your-forecast.json gs://forecastbench-submissions-dev/team3/2026-05-10.DeepMind.1.json
```

### Filename format

`{round_date}.{organization}.{N}.json`

- `round_date` — the forecast due date (`YYYY-MM-DD`)
- `organization` — their public org name (or "Anonymous N" if anonymous)
- `N` — submission number starting at 1. Max 3 per round (1 per model).

Teams can test their upload permissions anytime by uploading to their `test/` subfolder — the validator ignores this folder.

---

## Automatic validation (on-submission)

As soon as a file lands in the bucket, `on-submission` fires automatically. Within seconds, the team gets an email.

**Checks performed:**

- Filename format is correct
- All required top-level fields: `organization`, `model`, `model_organization`, `question_set`, `forecasts`
- Each forecast has `id`, `source`, `forecast` (0–1), `resolution_date`
- All question IDs exist in the official question set for that round
- Coverage ≥ 95% for market and dataset questions

**Edge cases:**

- **Late submission** (past 23:59:59 UTC on round_date): moved to `rejected/late/`, team notified
- **Over limit** (team already has 3 valid submissions this round): moved to `rejected/over-limit/`
- **Re-upload to fix**: before deadline, uploading the same filename replaces the prior submission; old Firestore entry is deleted and the file is validated fresh

---

## Post-round processing

Cloud Scheduler fires `post-round` nightly at 00:05 UTC. It auto-detects rounds past their deadline not yet transferred and:

1. Copies valid files from submissions bucket to the processing bucket for scoring
2. Moves invalid files to the interstitial bucket for manual FRI review
3. Emails each team that submitted
4. Sends FRI a digest email with the full round summary
5. Records the transfer in Firestore under `round_transfers` (idempotent — won't reprocess)

**Trigger post-round manually (auto mode):**

```powershell
$token = gcloud auth print-identity-token
$headers = @{ "Authorization" = "Bearer $token" }
Invoke-RestMethod -Method POST -Headers $headers -Body '{}' -ContentType "application/json" `
  -Uri "https://us-central1-forecastbench-dev.cloudfunctions.net/post-round"
```

**Process a specific round:**

```powershell
Invoke-RestMethod -Method POST -Headers $headers `
  -Body '{"round_date": "2026-05-10"}' -ContentType "application/json" `
  -Uri "https://us-central1-forecastbench-dev.cloudfunctions.net/post-round"
```

---

## Cloud Scheduler jobs

Two jobs run automatically. Set them up once with `scheduler/deploy.sh`:

```bash
PROJECT=forecastbench-dev REGION=us-central1 bash scheduler/deploy.sh
```

| Job | Schedule | Function | Purpose |
| --- | --- | --- | --- |
| `post-round-daily` | 00:05 UTC daily | `post-round` | Processes rounds past their deadline |
| `send-reminders-weekly` | 08:00 UTC Sundays (bi-weekly) | `send-reminders` | Reminder emails to all active teams — **paused, resume after review** |

**Resume the reminder job** after Houtan reviews:

```bash
gcloud scheduler jobs resume send-reminders-weekly \
  --project=forecastbench-dev --location=us-central1
```

Rounds are bi-weekly on Sundays starting 2026-03-02. The job fires every Sunday at 08:00 UTC. Since not every Sunday is a round date, the send-reminders function can also be called manually with a specific `round_date` (see above).

---

## Deploying / redeploying functions

Only needed when changing code. From `functions/`:

```bash
bash deploy.sh              # redeploy all five functions
bash deploy.sh onboard
bash deploy.sh upload
bash deploy.sh post_round
bash deploy.sh validate
bash deploy.sh send_reminders
```

`common/validation.py`, `common/email.py`, and `common/utils.py` are copied into each function directory before deploying and removed after. Edit the files in `common/` — not the copies.

**Sandbox deploy** (forecastbench-johan project):

```bash
GCP_PROJECT=forecastbench-johan \
UPLOAD_BUCKET=forecastbench-johan-submissions \
INTERSTITIAL_BUCKET=forecastbench-johan-interstitial \
HISTORY_BUCKET=forecastbench-johan-history \
BUILD_ENV=dev MOCK_DATE=2026-05-27 TRIGGER_LOCATION=us \
bash deploy.sh
```

---

## First-time GCP setup

To set up a new GCP project from scratch (run after creating the project and service account):

```bash
SA_EMAIL=submissions@forecastbench-dev.iam.gserviceaccount.com \
PROJECT=forecastbench-dev \
bash functions/setup_permissions.sh
```

This grants the service account all required permissions: bucket access, Firestore, Eventarc, Cloud Run invoker, and Pub/Sub.

Then run the scheduler setup:

```bash
PROJECT=forecastbench-dev bash scheduler/deploy.sh
```

Then deploy all functions:

```bash
bash functions/deploy.sh
```

---

## Checking logs

**Cloud Console:** `console.cloud.google.com/functions?project=forecastbench-dev` → click a function → Logs

**Terminal:**

```bash
gcloud functions logs read on-submission --project=forecastbench-dev --region=us-central1 --limit=50
gcloud functions logs read onboard-team --project=forecastbench-dev --region=us-central1 --limit=50
gcloud functions logs read post-round --project=forecastbench-dev --region=us-central1 --limit=50
gcloud functions logs read send-reminders --project=forecastbench-dev --region=us-central1 --limit=50
```

**Firestore:** `console.cloud.google.com/firestore?project=forecastbench-dev`  
**GCS submissions:** `console.cloud.google.com/storage/browser/forecastbench-submissions-dev`

---

## Firestore collections

### `teams`

| Field | Notes |
| --- | --- |
| `team_name` | `team1`, `team2`, ... — permanent ID, used as GCS folder name |
| `organization` | Public name. "Anonymous N" if anonymous. |
| `original_organization` | Always the real org name. Never shown publicly. |
| `model` | |
| `model_organization` | |
| `emails` | Used for IAM and notifications |
| `service_accounts` | GCP service accounts with upload access |
| `anonymous` | bool |
| `gcs_folder` | `gs://forecastbench-submissions-dev/team1/` |
| `rounds_participated` | Round dates with at least one valid submission |
| `models_used` | |
| `active` | bool — set to false by the remove endpoint |

### `submissions`

One document per upload attempt.

| Field | Notes |
| --- | --- |
| `team_name` | |
| `organization` | Public name at time of submission |
| `filename` | |
| `gcs_path` | Full `gs://` path |
| `round_date` | `YYYY-MM-DD` |
| `valid` | bool |
| `errors` | List of validation errors. Empty if valid. |
| `warnings` | Non-fatal issues. |
| `stats` | Market and dataset coverage percentages. |

### `round_transfers`

One document per round processed by `post-round`.

| Field | Notes |
| --- | --- |
| `round_date` | |
| `files_transferred` | Count of valid files copied to processing |
| `files_failed` | Count of failures |
| `transferred` | List of GCS paths copied |
| `errors` | `[{file, error}]` for any failures |

---

## Updating SMTP credentials

No redeploy needed — update env vars in place:

```bash
for FUNC in onboard-team on-submission post-round send-reminders; do
  gcloud functions deploy "$FUNC" --gen2 \
    --project=forecastbench-dev --region=us-central1 \
    --update-env-vars SMTP_USER=new@email.com,SMTP_PASSWORD=newpassword
done
```
