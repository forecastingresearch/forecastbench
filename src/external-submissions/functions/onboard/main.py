"""
ForecastBench team onboarding handler.

Each team gets a unique team_name (team1, team2, ...) as their permanent
identifier. One organization can have multiple teams (e.g. GDM has team1
and team2). The GCS folder is named after team_name, not the org, so
anonymous teams' real identity isn't revealed by their folder name.

Accepts JSON POST with:
  - organization: real org name (required, stored privately if anonymous)
  - model: model name (required)
  - model_organization: org that built the model (required)
  - emails: list of member email addresses (required)
  - anonymous: bool (optional, default false)

Environment variables:
  - UPLOAD_BUCKET: GCS bucket (default: forecastbench-johan-submissions)
"""

import json
import os
import traceback
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
from google.cloud import firestore
from email_utils import send_welcome
from validation import MAX_SUBMISSIONS_PER_ROUND

UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "forecastbench-submissions-dev")

# Initialize once per instance — reused across warm requests
db  = firestore.Client()
gcs = storage.Client()


def _next_team_name(db):
    """Assigns the next sequential team name: team1, team2, team3..."""
    count = len(list(db.collection("teams").stream()))
    return f"team{count + 1}"


def _next_anon_number(db):
    """Counts existing anonymous teams to assign Anonymous N."""
    count = len(list(db.collection("teams").where("anonymous", "==", True).stream()))
    return count + 1


GOOGLE_MX_SUFFIXES = ("google.com", "googlemail.com")


def _is_google_account(email):
    """
    Returns True if the email is a Gmail or Google Workspace account.
    Checks @gmail.com directly, otherwise does a DNS MX lookup on the domain
    to see if it routes through Google's mail servers.
    """
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
        return False  # Can't resolve — assume non-Google, warn


def _warn_non_google_emails(emails):
    """
    GCS IAM permissions only work with Google accounts (Gmail or Google Workspace).
    Does a DNS MX lookup to detect Google Workspace domains.
    See wiki for handling non-Google accounts.
    """
    non_google = [e for e in emails if not _is_google_account(e)]
    if non_google:
        return (
            f"{len(non_google)} email(s) do not appear to be Google accounts: {non_google}. "
            f"GCS upload permissions require Gmail or Google Workspace accounts. "
            f"See wiki for handling non-Google accounts."
        )
    return None


def _set_folder_permissions(bucket_name, team_name, emails):
    """
    Grants team emails full object access on their folder only.
    objectAdmin (not just objectCreator) so they can also delete files.
    IAM condition restricts access to their folder prefix only.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    folder_prefix = f"projects/_/buckets/{bucket_name}/objects/{team_name}/"

    def _principal(email):
        # Service accounts use serviceAccount: prefix, regular emails use user:
        if email.endswith(".iam.gserviceaccount.com") or "gserviceaccount" in email:
            return f"serviceAccount:{email}"
        return f"user:{email}"

    policy.bindings.append({
        "role": "roles/storage.objectAdmin",
        "members": set(_principal(e) for e in emails),
        "condition": {
            "title": f"{team_name} folder access",
            "expression": f'resource.name.startsWith("{folder_prefix}")',
        },
    })
    bucket.set_iam_policy(policy)


def _remove_folder_permissions(bucket_name, team_name, emails, service_accounts):
    """Removes all IAM bindings for a team's folder from the bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    folder_prefix = f"projects/_/buckets/{bucket_name}/objects/{team_name}/"

    def _principal(email):
        if email.endswith(".iam.gserviceaccount.com") or "gserviceaccount" in email:
            return f"serviceAccount:{email}"
        return f"user:{email}"

    principals = set(_principal(e) for e in emails + service_accounts)
    policy.bindings = [
        b for b in policy.bindings
        if not (
            b.get("condition", {}).get("expression", "").find(folder_prefix) != -1
            and principals & set(b.get("members", []))
        )
    ]
    bucket.set_iam_policy(policy)


def onboard(request):
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, DELETE",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    cors = {"Access-Control-Allow-Origin": "*"}

    if request.method == "DELETE":
        return _handle_remove(request, cors)
    return _handle_onboard(request, cors)


def _handle_remove(request, cors):
    """Deactivates a team: revokes GCS access, marks inactive in Firestore."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        team_name = data.get("team_name", "").strip()
        if not team_name:
            return (json.dumps({"success": False, "error": "'team_name' is required"}), 400, cors)

        team_docs = list(db.collection("teams").where("team_name", "==", team_name).stream())
        if not team_docs:
            return (json.dumps({"success": False, "error": f"Team '{team_name}' not found"}), 404, cors)

        team = team_docs[0].to_dict()
        emails = team.get("emails", [])
        service_accounts = team.get("service_accounts", [])

        try:
            _remove_folder_permissions(UPLOAD_BUCKET, team_name, emails, service_accounts)
        except Exception as e:
            print(f"IAM removal failed: {e}")

        team_docs[0].reference.update({"active": False})

        return (json.dumps({"success": True, "team_name": team_name, "message": "Team deactivated and GCS access revoked."}), 200, cors)

    except Exception as e:
        print(traceback.format_exc())
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)


def _handle_onboard(request, cors):

    cors = {"Access-Control-Allow-Origin": "*"}

    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            return (json.dumps({"success": False, "error": "Invalid or missing JSON body"}), 400, cors)

        organization = data.get("organization", "").strip()
        models = data.get("models", [])
        model_organization = data.get("model_organization", "").strip()
        emails = data.get("emails", [])
        service_accounts = data.get("service_accounts", [])
        anonymous = data.get("anonymous", False)

        errors = []
        if not organization:
            errors.append("'organization' is required")
        if not model_organization:
            errors.append("'model_organization' is required")
        if not models or not isinstance(models, list):
            errors.append("'models' must be a non-empty list")
        else:
            models = [m.strip() for m in models if isinstance(m, str) and m.strip()]
            if not models:
                errors.append("'models' must contain at least one valid model name")
            elif len(models) > MAX_SUBMISSIONS_PER_ROUND:
                errors.append(f"'models' can contain at most {MAX_SUBMISSIONS_PER_ROUND} models")
        if not emails or not isinstance(emails, list):
            errors.append("'emails' must be a non-empty list")
        else:
            emails = [e.strip().lower() for e in emails if isinstance(e, str) and e.strip()]
            if not emails:
                errors.append("'emails' must contain at least one valid address")
        if not isinstance(service_accounts, list):
            errors.append("'service_accounts' must be a list")
        else:
            service_accounts = [e.strip().lower() for e in service_accounts if isinstance(e, str) and e.strip()]

        if errors:
            return (json.dumps({"success": False, "errors": errors}), 400, cors)

        # No org-level duplicate check — multiple teams per org are allowed.

        # Assign unique team_name and display organization name
        team_name = _next_team_name(db)
        display_org = f"Anonymous {_next_anon_number(db)}" if anonymous else organization

        # Create permanent GCS folder named by team_name (not org, to protect anonymity)
        bucket = gcs.bucket(UPLOAD_BUCKET)
        placeholder = bucket.blob(f"{team_name}/.keep")
        placeholder.upload_from_string("", content_type="application/x-empty")

        # Save to Firestore and create folder — fast, block on these
        db.collection("teams").add({
            "team_name": team_name,
            "organization": display_org,
            "original_organization": organization,
            "model_organization": model_organization,
            "emails": emails,
            "service_accounts": service_accounts,
            "anonymous": anonymous,
            "rounds_participated": [],
            "models_used": models,
            "created_at": firestore.SERVER_TIMESTAMP,
            "active": True,
        })

        # IAM is slow — run in background; email runs in parallel but we wait for it
        all_principals = emails + service_accounts

        def _iam():
            try:
                _set_folder_permissions(UPLOAD_BUCKET, team_name, all_principals)
            except Exception as e:
                print(f"IAM setup failed: {e}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(_iam)
            f_email = pool.submit(send_welcome, emails, team_name, display_org, UPLOAD_BUCKET, anonymous, models)
            f_dns   = pool.submit(_warn_non_google_emails, emails)
            try:
                f_email.result(timeout=20)
            except Exception as e:
                print(f"send_welcome failed: {e}")

        email_warning = f_dns.result()

        response = {
            "success": True,
            "team_name": team_name,
            "organization": display_org,
            "models": models,
            "upload_folder": f"gs://{UPLOAD_BUCKET}/{team_name}/",
            "instructions": (
                f"Upload forecast files to gs://{UPLOAD_BUCKET}/{team_name}/ "
                f"using gsutil or the GCP Console. "
                f"Name files: {{round_date}}.{{organization}}.{{N}}.json. "
                f"Max 3 submissions per round (1 per model). Deadline: 23:59:59 UTC."
            ),
        }

        if anonymous:
            response["note"] = (
                f"Public name is '{display_org}'. Use this as both 'organization' "
                f"and 'model_organization' in forecast files."
            )

        if email_warning:
            response["warning"] = email_warning

        return (json.dumps(response, indent=2), 201, cors)

    except Exception as e:
        print(traceback.format_exc())
        return (json.dumps({"success": False, "error": f"Internal error: {str(e)}"}), 500, cors)
