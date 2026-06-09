"""ForecastBench team onboarding handler.

Each team gets a unique internal ID (team1, team2, ...) used as their GCS folder name.
One organization can have multiple teams — team_name is an optional internal label to
distinguish them (e.g. "GDM A", "GDM B"). It is never shown in emails.

Accepts JSON POST with:
  - organization: real org name (required, stored privately if anonymous)
  - team_name: optional internal label (must be unique if provided)
  - emails: list of member email addresses (required)
  - service_accounts: list of GCP service accounts (optional, no emails sent)
  - anonymous: bool (optional, default false)

Environment variables:
  - UPLOAD_BUCKET: GCS bucket name
  - NEXT_DUE_DATE: next forecast due date (YYYY-MM-DD), included in welcome email
"""

import json
import os
import traceback

import dns.resolver
from email_utils import send_welcome
from google.cloud import firestore, storage

UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "")
NEXT_DUE_DATE = os.environ.get("NEXT_DUE_DATE", "")

db = firestore.Client()
gcs = storage.Client()


@firestore.transactional
def _allocate_ids(transaction, counter_ref, anonymous):
    """Atomically allocate the next team_id and (if anonymous) anon number."""
    snap = counter_ref.get(transaction=transaction)
    data = snap.to_dict() if snap.exists else {"team_count": 0, "anon_count": 0}
    team_n = data.get("team_count", 0) + 1
    anon_n = data.get("anon_count", 0) + (1 if anonymous else 0)
    transaction.set(counter_ref, {"team_count": team_n, "anon_count": anon_n})
    return f"team{team_n}", anon_n


def _team_name_taken(db, team_name):
    """Return True if any active team already has this internal label."""
    results = list(
        db.collection("teams")
        .where("team_name", "==", team_name)
        .where("active", "==", True)
        .stream()
    )
    return len(results) > 0


GOOGLE_MX_SUFFIXES = ("google.com", "googlemail.com")


def _is_google_account(email):
    """Return True if the email is a Gmail or Google Workspace account."""
    if email.endswith("@gmail.com"):
        return True
    domain = email.split("@")[-1]
    try:
        records = dns.resolver.resolve(domain, "MX")
        return any(
            str(r.exchange).rstrip(".").endswith(suffix)
            for r in records
            for suffix in GOOGLE_MX_SUFFIXES
        )
    except Exception:
        return False


def _warn_non_google_emails(emails):
    """Return a warning string if any emails are not Google accounts, else None."""
    non_google = [e for e in emails if not _is_google_account(e)]
    if non_google:
        return (
            f"{len(non_google)} email(s) do not appear to be Google accounts: {non_google}. "
            f"GCS upload permissions require Gmail or Google Workspace accounts. "
            f"See wiki for handling non-Google accounts."
        )
    return None


def _set_folder_permissions(bucket_name, team_id, emails, service_accounts):
    """Grant objectUser + objectViewer on the team's folder prefix only."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    folder_prefix = f"projects/_/buckets/{bucket_name}/objects/{team_id}/"

    def _principal(email):
        if email.endswith(".iam.gserviceaccount.com") or "gserviceaccount" in email:
            return f"serviceAccount:{email}"
        return f"user:{email}"

    members = set(_principal(e) for e in emails + service_accounts)
    condition = {
        "title": f"{team_id} folder access",
        "expression": f'resource.name.startsWith("{folder_prefix}")',
    }
    for role in ("roles/storage.objectUser", "roles/storage.objectViewer"):
        policy.bindings.append({"role": role, "members": members, "condition": condition})

    bucket.set_iam_policy(policy)


def _remove_folder_permissions(bucket_name, team_id, emails, service_accounts):
    """Remove all IAM bindings for a team's folder from the bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    folder_prefix = f"projects/_/buckets/{bucket_name}/objects/{team_id}/"

    def _principal(email):
        if email.endswith(".iam.gserviceaccount.com") or "gserviceaccount" in email:
            return f"serviceAccount:{email}"
        return f"user:{email}"

    principals = set(_principal(e) for e in emails + service_accounts)
    policy.bindings = [
        b
        for b in policy.bindings
        if not (
            folder_prefix in b.get("condition", {}).get("expression", "")
            and principals & set(b.get("members", []))
        )
    ]
    bucket.set_iam_policy(policy)


def onboard(request):
    """Handle team onboarding (POST) and removal (DELETE)."""
    if request.method == "OPTIONS":
        return (
            "",
            204,
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, DELETE",
                "Access-Control-Allow-Headers": "Content-Type",
            },
        )

    cors = {"Access-Control-Allow-Origin": "*"}

    if request.method == "DELETE":
        return _handle_remove(request, cors)
    return _handle_onboard(request, cors)


def _handle_remove(request, cors):
    """Deactivate a team: revoke GCS access, then mark inactive in Firestore."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        team_id = data.get("team_id", "").strip()
        if not team_id:
            return (json.dumps({"success": False, "error": "'team_id' is required"}), 400, cors)

        team_docs = list(db.collection("teams").where("team_id", "==", team_id).stream())
        if not team_docs:
            return (
                json.dumps({"success": False, "error": f"Team '{team_id}' not found"}),
                404,
                cors,
            )

        team = team_docs[0].to_dict()
        emails = team.get("emails", [])
        service_accounts = team.get("service_accounts", [])

        _remove_folder_permissions(UPLOAD_BUCKET, team_id, emails, service_accounts)
        team_docs[0].reference.update({"active": False})

        return (
            json.dumps(
                {
                    "success": True,
                    "team_id": team_id,
                    "message": "Team deactivated and GCS access revoked.",
                }
            ),
            200,
            cors,
        )

    except Exception as e:
        print(traceback.format_exc())
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)


def _handle_onboard(request, cors):
    """Register a new team."""
    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            return (
                json.dumps({"success": False, "error": "Invalid or missing JSON body"}),
                400,
                cors,
            )

        organization = data.get("organization", "").strip()
        team_name = data.get("team_name", "").strip()
        emails = data.get("emails", [])
        service_accounts = data.get("service_accounts", [])
        anonymous = data.get("anonymous", False)

        errors = []
        if not organization:
            errors.append("'organization' is required")
        if not emails or not isinstance(emails, list):
            errors.append("'emails' must be a non-empty list")
        else:
            emails = [e.strip().lower() for e in emails if isinstance(e, str) and e.strip()]
            if not emails:
                errors.append("'emails' must contain at least one valid address")
        if not isinstance(service_accounts, list):
            errors.append("'service_accounts' must be a list")
        else:
            service_accounts = [
                e.strip().lower() for e in service_accounts if isinstance(e, str) and e.strip()
            ]

        if team_name and _team_name_taken(db, team_name):
            errors.append(f"Team name '{team_name}' is already taken.")

        if errors:
            return (json.dumps({"success": False, "errors": errors}), 400, cors)

        counter_ref = db.collection("counters").document("teams")
        team_id, anon_number = _allocate_ids(db.transaction(), counter_ref, anonymous)
        display_org = f"Anonymous {anon_number}" if anonymous else organization

        bucket = gcs.bucket(UPLOAD_BUCKET)
        bucket.blob(f"{team_id}/.keep").upload_from_string("", content_type="application/x-empty")

        all_principals = emails + service_accounts
        _set_folder_permissions(UPLOAD_BUCKET, team_id, all_principals)

        db.collection("teams").add(
            {
                "team_id": team_id,
                "team_name": team_name or None,
                "organization": display_org,
                "deanonymized_organization": organization,
                "emails": emails,
                "service_accounts": service_accounts,
                "anonymous": anonymous,
                "created_at": firestore.SERVER_TIMESTAMP,
                "active": True,
            }
        )

        email_warning = _warn_non_google_emails(emails)

        try:
            send_welcome(emails, team_id, display_org, UPLOAD_BUCKET, anonymous, NEXT_DUE_DATE)
        except Exception as e:
            print(f"send_welcome failed: {e}")

        response = {
            "success": True,
            "team_id": team_id,
            "team_name": team_name or None,
            "organization": display_org,
            "upload_folder": f"gs://{UPLOAD_BUCKET}/{team_id}/",
            "instructions": (
                f"Upload forecast files to gs://{UPLOAD_BUCKET}/{team_id}/ "
                f"using gsutil or the GCP Console. "
                f"Name files: {{forecast_due_date}}.{{organization}}.{{N}}.json. "
                f"Deadline: 23:59:59 UTC."
            ),
        }

        if anonymous:
            response["note"] = (
                f"Public name is '{display_org}'. Use this as 'organization' in forecast files."
            )
        if email_warning:
            response["warning"] = email_warning

        return (json.dumps(response, indent=2), 201, cors)

    except Exception as e:
        print(traceback.format_exc())
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)
