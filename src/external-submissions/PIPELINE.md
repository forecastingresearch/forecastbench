# ForecastBench External Submission Pipeline

## Firestore — `teams` collection

One document per team.

| Field | Notes |
| --- | --- |
| `team_id` | `team1`, `team2`, ... — permanent internal ID, used as GCS folder name |
| `team_name` | Optional internal label (unique). Used to distinguish multiple teams from the same org (e.g. "GDM A", "GDM B"). Never shown publicly or in emails. |
| `organization` | Public name. `"Anonymous N"` if anonymous. |
| `deanonymized_organization` | Always the real org name. Never shown publicly. |
| `emails` | Used for IAM and email notifications |
| `service_accounts` | GCP service accounts for automated uploads. No emails sent to these. |
| `anonymous` | bool |
| `created_at` | Firestore server timestamp |
| `active` | bool — set to false on removal |

### Counter document

`counters/teams` holds `{team_count: N, anon_count: M}` for atomic ID allocation.
Initialize before first deployment (set N and M to the current team and anon counts):

```python
db.collection("counters").document("teams").set({"team_count": N, "anon_count": M})
```

---

## Register a new team

POST to the `onboard-team` Cloud Function:

```json
{
  "organization": "Acme Corp",
  "team_name": "acme-a",
  "emails": ["alice@acme.com", "bob@acme.com"],
  "service_accounts": ["submissions@acme.iam.gserviceaccount.com"],
  "anonymous": false
}
```

Fields:

- `organization` (required) — real org name
- `team_name` (optional) — internal label, must be unique
- `emails` (required) — list of member addresses; must be Gmail/Google Workspace for GCS access
- `service_accounts` (optional) — GCP SAs; always get GCS access
- `anonymous` (optional, default false) — if true, public name becomes `"Anonymous N"`

The function:

1. Allocates the next `teamN` ID atomically via `counters/teams`
2. Creates a `gs://<bucket>/teamN/.keep` placeholder
3. Grants `roles/storage.objectUser` + `roles/storage.objectViewer` on the `teamN/` prefix
4. Writes the Firestore document
5. Sends a welcome email to `emails`

If any email is not a Google account, registration succeeds but a warning is returned — those members won't be able to upload to GCS directly.

---

## Remove a team

DELETE to the `onboard-team` Cloud Function:

```json
{ "team_id": "team7" }
```

Revokes GCS access and marks the team inactive. IAM removal failure returns a 500 — the team is **not** deactivated if permissions cannot be revoked.

---

## Deploy

From `src/external-submissions/`:

```bash
make deploy-onboard
```

Required variables in `variables.mk` (at repo root):

```makefile
CLOUD_PROJECT=...
SUBMISSIONS_SA_EMAIL=...
SUBMISSIONS_BUCKET=...
SMTP_USER=...
SMTP_PASSWORD=...
NEXT_DUE_DATE=YYYY-MM-DD
```
