r"""Admin CLI to manage external submission teams.

Run locally by an admin with gcloud application-default credentials; not deployed.

Usage: copy `onboard.example.sh` to `onboard.sh` (gitignored), edit the values at the top,
and run it from this directory in bash. Or invoke directly:

    set -a; . <(grep -v '^#' ../../../variables.mk | tr -d '\r'); set +a
    python main.py register --organization "Acme Corp" --emails alice@acme.com --mode TEST
    python main.py deactivate --team-id acme-corp_a1b2c3

`--mode` defaults to TEST, which attempts no email. One-time per-environment setup
(anonymous-number counter) is done by `init_counters.py`, not by this CLI.

Google Cloud client libraries are imported lazily so this module can be unit tested without
them installed.
"""

import argparse
import json
import logging
import os
import re
import secrets
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from helpers import constants, email, env, question_curation  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TEAMS_COLLECTION = "teams"
TEAM_NAMES_COLLECTION = "team_names"
COUNTERS_COLLECTION = "counters"
COUNTERS_DOCUMENT = "teams"

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
GOOGLE_MX_SUFFIXES = ("google.com", "googlemail.com")
TEAM_ID_HASH_LENGTH = 6
MAX_SLUG_LENGTH = 40

SUBMISSION_WIKI_URL = (
    "https://github.com/forecastingresearch/forecastbench/wiki/How-to-submit-to-ForecastBench"
)


def normalize_name(name: str) -> str:
    """Normalize a team/organization name for case-insensitive matching.

    Lowercases, trims, and collapses whitespace. The legacy CSV form "Anonymous #8" normalizes
    to the canonical wiki form "anonymous 8".

    Args:
        name (str): The name to normalize.
    """
    normalized = re.sub(r"\s+", " ", name.strip().lower())
    return re.sub(r"^anonymous #(\d+)$", r"anonymous \1", normalized)


def slugify_organization(name: str) -> str:
    """Return a filename-safe slug for use in team folder names.

    Lowercases, transliterates accents to ASCII, replaces every other character run with a
    single hyphen, and truncates to `MAX_SLUG_LENGTH`. Falls back to "team" if nothing
    survives (e.g. a fully non-Latin name).

    Args:
        name (str): The organization name to slugify.
    """
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    return slug[:MAX_SLUG_LENGTH].rstrip("-") or "team"


def generate_team_id(db: Any, organization: str) -> str:
    """Return a unique team ID of the form "<org-slug>_<6-char-hash>".

    The team ID doubles as the team's GCS folder name. The slug keeps folders legible; the
    random hash makes IDs unique so one organization can have several teams.

    Args:
        db (Any): Firestore client.
        organization (str): Public organization name (the anonymous one if applicable).
    """
    slug = slugify_organization(organization)
    for _ in range(20):
        team_id = f"{slug}_{secrets.token_hex(TEAM_ID_HASH_LENGTH // 2)}"
        if not db.collection(TEAMS_COLLECTION).document(team_id).get().exists:
            return team_id
    raise RuntimeError(f"Could not generate a unique team ID for slug {slug!r}.")


def make_principal(account: str) -> str:
    """Return the IAM principal string for an email address or service account.

    Args:
        account (str): Email address or service account.
    """
    if account.endswith(".gserviceaccount.com"):
        return f"serviceAccount:{account}"
    return f"user:{account}"


def folder_prefix(bucket_name: str, team_id: str) -> str:
    """Return the IAM `resource.name` prefix for a team's folder.

    Args:
        bucket_name (str): GCS bucket name.
        team_id (str): Team ID, e.g. "acme-corp_a1b2c3".
    """
    return f"projects/_/buckets/{bucket_name}/objects/{team_id}/"


def is_google_account(email_address: str) -> bool:
    """Return True if the email is a Gmail or Google Workspace account.

    Workspace domains are detected by their MX records pointing at Google. Errors (including
    dnspython not being installed) count as "not a Google account": this only produces a
    warning, never a registration failure.

    Args:
        email_address (str): The email address to check.
    """
    domain = email_address.split("@")[-1]
    if domain in ("gmail.com", "googlemail.com"):
        return True
    try:
        import dns.resolver

        records = dns.resolver.resolve(domain, "MX", lifetime=3)
        return any(
            str(record.exchange).rstrip(".").endswith(suffix)
            for record in records
            for suffix in GOOGLE_MX_SUFFIXES
        )
    except Exception:
        return False


def get_clients() -> Tuple[Any, Any]:
    """Return (firestore client, storage client) pinned to `CLOUD_PROJECT`.

    Drops any inherited quota-project override: sending the x-goog-user-project header
    requires `serviceusage.services.use`, which admin accounts may lack. Requests then
    bill to the resource's own project, which is what we want here anyway.
    """
    if not env.PROJECT_ID:
        raise RuntimeError(
            "CLOUD_PROJECT is not set — load variables.mk first. Refusing to fall back to"
            " the gcloud default project."
        )
    os.environ.pop("GOOGLE_CLOUD_QUOTA_PROJECT", None)
    os.environ["GOOGLE_CLOUD_PROJECT"] = env.PROJECT_ID

    from google.cloud import firestore, storage

    return firestore.Client(project=env.PROJECT_ID), storage.Client(project=env.PROJECT_ID)


def allocate_anon_number(db: Any) -> int:
    """Atomically allocate the next anonymous team number.

    Args:
        db (Any): Firestore client.
    """
    from google.cloud import firestore

    counter_ref = db.collection(COUNTERS_COLLECTION).document(COUNTERS_DOCUMENT)

    @firestore.transactional
    def _allocate(transaction: Any) -> int:
        snapshot = counter_ref.get(transaction=transaction)
        if not snapshot.exists or "anon_count" not in snapshot.to_dict():
            raise RuntimeError(
                f"Firestore document {COUNTERS_COLLECTION}/{COUNTERS_DOCUMENT} is missing or has"
                " no 'anon_count'. Run `python init_counters.py` once per environment first."
            )
        anon_n = snapshot.to_dict()["anon_count"] + 1
        transaction.set(counter_ref, {"anon_count": anon_n})
        return anon_n

    return _allocate(db.transaction())


def _bindings_without_folder(policy: Any, prefix: str) -> List[Dict[str, Any]]:
    """Return the policy's bindings minus those scoped to the given folder prefix.

    Args:
        policy (Any): Bucket IAM policy (version 3).
        prefix (str): Folder prefix from `folder_prefix`.
    """
    return [
        b for b in policy.bindings if prefix not in b.get("condition", {}).get("expression", "")
    ]


def set_folder_permissions(gcs: Any, bucket_name: str, team_id: str, principals: Set[str]) -> None:
    """Grant objectViewer + objectUser on the team's folder prefix only.

    Idempotent: existing bindings for this folder are replaced, not duplicated.

    Args:
        gcs (Any): Storage client.
        bucket_name (str): Upload bucket name.
        team_id (str): Team ID, used as the folder name.
        principals (Set[str]): IAM principals (from `make_principal`).
    """
    bucket = gcs.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3

    prefix = folder_prefix(bucket_name, team_id)
    policy.bindings = _bindings_without_folder(policy, prefix)
    condition = {
        "title": f"{team_id} folder access",
        "expression": f'resource.name.startsWith("{prefix}")',
    }
    for role in ("roles/storage.objectViewer", "roles/storage.objectUser"):
        policy.bindings.append({"role": role, "members": set(principals), "condition": condition})
    bucket.set_iam_policy(policy)


def remove_folder_permissions(gcs: Any, bucket_name: str, team_id: str) -> None:
    """Remove all IAM bindings scoped to the team's folder prefix.

    Args:
        gcs (Any): Storage client.
        bucket_name (str): Upload bucket name.
        team_id (str): Team ID, used as the folder name.
    """
    bucket = gcs.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3
    policy.bindings = _bindings_without_folder(policy, folder_prefix(bucket_name, team_id))
    bucket.set_iam_policy(policy)


def build_welcome_email(
    team_id: str, organization: str, anonymous: bool, next_due_date: str
) -> Tuple[str, str]:
    """Return (subject, body) for the welcome email sent on registration.

    Submission instructions live on the wiki only, so they are maintained in one place.

    Args:
        team_id (str): Team ID, used as the folder name.
        organization (str): Public organization name (the anonymous one if applicable).
        anonymous (bool): Whether the team registered anonymously.
        next_due_date (str): Next forecast due date in ISO format.
    """
    subject = "ForecastBench — your team has been registered"
    anonymous_note = (
        f"\nYou are registered anonymously. Your public name is '{organization}': use it as"
        " 'organization' in your forecast files. You may choose whether to also use it for"
        " 'model_organization'.\n"
        if anonymous
        else ""
    )
    body = f"""Hi,

Your team has been registered on ForecastBench.

Team: {organization}
Upload folder: gs://{env.SUBMISSIONS_BUCKET}/{team_id}/
Next forecast due date: {next_due_date} (rounds repeat every two weeks)
{anonymous_note}
Please upload a small test file to your folder now to confirm your access works.

Submission instructions: {SUBMISSION_WIKI_URL}

If you have any questions, just reply to this email.

The ForecastBench team
"""
    return subject, body


def register(
    organization: str,
    emails: Optional[List[str]] = None,
    service_accounts: Optional[List[str]] = None,
    team_name: str = "",
    anonymous: bool = False,
    run_mode: constants.RunMode = constants.RunMode.TEST,
    send_email_in_test: bool = False,
    db: Any = None,
    gcs: Any = None,
) -> Dict[str, Any]:
    """Register a new team: GCS folder + IAM, Firestore documents, welcome email.

    Args:
        organization (str): Real organization name (stored privately if anonymous).
        emails (Optional[List[str]]): Member email addresses; receive IAM access and emails.
        service_accounts (Optional[List[str]]): Service accounts; IAM access, never emailed.
        team_name (str): Optional internal label; must be unique, never public.
        anonymous (bool): Register under an "Anonymous N" public name.
        run_mode (constants.RunMode): TEST (default) skips the welcome email unless
            `send_email_in_test` is set; PROD sends it normally.
        send_email_in_test (bool): In TEST mode, send the welcome email rerouted to
            `SMTP_USER` with a "[TEST]" subject prefix.
        db (Any): Firestore client (injected in tests).
        gcs (Any): Storage client (injected in tests).
    """
    emails = [e.strip().lower() for e in (emails or []) if e.strip()]
    service_accounts = [s.strip().lower() for s in (service_accounts or []) if s.strip()]
    organization = organization.strip()
    team_name = team_name.strip()

    errors = []
    if not organization:
        errors.append("--organization is required.")
    if not emails and not service_accounts:
        errors.append("Provide at least one of --emails or --service-accounts.")
    bad_emails = [e for e in emails + service_accounts if not EMAIL_REGEX.match(e)]
    if bad_emails:
        errors.append(f"Invalid email address(es): {bad_emails}")
    if not env.SUBMISSIONS_BUCKET:
        errors.append("SUBMISSIONS_BUCKET is not set; load variables.mk.")
    if errors:
        raise ValueError(" ".join(errors))

    if db is None or gcs is None:
        db, gcs = get_clients()

    if team_name:
        reservation_ref = db.collection(TEAM_NAMES_COLLECTION).document(normalize_name(team_name))
        if reservation_ref.get().exists:
            raise ValueError(
                f"Team name '{team_name}' is permanently reserved (names are never reissued,"
                " even after a team is deactivated)."
            )

    display_org = f"Anonymous {allocate_anon_number(db)}" if anonymous else organization
    team_id = generate_team_id(db, display_org)

    bucket = gcs.bucket(env.SUBMISSIONS_BUCKET)
    bucket.blob(f"{team_id}/.keep").upload_from_string("", content_type="application/x-empty")

    principals = {make_principal(account) for account in emails + service_accounts}
    try:
        set_folder_permissions(gcs, env.SUBMISSIONS_BUCKET, team_id, principals)
    except Exception as exception:
        if "does not exist" in str(exception):
            raise ValueError(
                f"GCS rejected an account ({exception}). IAM only accepts existing Google"
                " identities — every email must belong to a real Gmail/Google Workspace"
                " account and every service account must exist."
            ) from exception
        raise

    from google.cloud import firestore

    db.collection(TEAMS_COLLECTION).document(team_id).set(
        {
            "team_id": team_id,
            "team_name": team_name or None,
            "organization": display_org,
            "deanonymized_organization": organization,
            "emails": emails,
            "service_accounts": service_accounts,
            "anonymous": anonymous,
            "active": True,
            "created_at": firestore.SERVER_TIMESTAMP,
            "deactivated_at": None,
        }
    )
    if team_name:
        reservation_ref.set(
            {
                "team_id": team_id,
                "team_name": team_name,
                "reserved_at": firestore.SERVER_TIMESTAMP,
            }
        )

    warnings = []
    non_google = [e for e in emails if not is_google_account(e)]
    if non_google:
        warnings.append(
            f"Email(s) {non_google} do not appear to be Google accounts; GCS upload requires"
            " Gmail or Google Workspace. Ask the team for Google-based addresses."
        )

    next_due_date = question_curation.get_next_forecast_due_date()
    email_sent = False
    if emails:
        if run_mode == constants.RunMode.PROD or send_email_in_test:
            subject, body = build_welcome_email(team_id, display_org, anonymous, next_due_date)
            email_sent = email.send_email(
                emails, subject, body, run_mode=run_mode, send_email_in_test=send_email_in_test
            )
            if not email_sent:
                warnings.append("Welcome email was NOT sent; see logs. Notify the team manually.")
        else:
            warnings.append(
                f"Welcome email skipped in TEST mode. Email would have been sent to {emails}."
                " Pass --send-email-in-test to send a rerouted test email."
            )

    return {
        "team_id": team_id,
        "team_name": team_name or None,
        "organization": display_org,
        "upload_folder": f"gs://{env.SUBMISSIONS_BUCKET}/{team_id}/",
        "next_due_date": next_due_date,
        "run_mode": run_mode.value,
        "welcome_email_sent": email_sent,
        "warnings": warnings,
    }


def deactivate(team_id: str, db: Any = None, gcs: Any = None) -> Dict[str, Any]:
    """Deactivate a team: revoke GCS access, mark inactive; folder and names stay reserved.

    Args:
        team_id (str): Team ID, e.g. "acme-corp_a1b2c3".
        db (Any): Firestore client (injected in tests).
        gcs (Any): Storage client (injected in tests).
    """
    if db is None or gcs is None:
        db, gcs = get_clients()

    team_ref = db.collection(TEAMS_COLLECTION).document(team_id)
    snapshot = team_ref.get()
    if not snapshot.exists:
        raise ValueError(f"Team '{team_id}' not found.")
    if not snapshot.to_dict().get("active", False):
        raise ValueError(f"Team '{team_id}' is already inactive.")

    remove_folder_permissions(gcs, env.SUBMISSIONS_BUCKET, team_id)

    from google.cloud import firestore

    team_ref.update({"active": False, "deactivated_at": firestore.SERVER_TIMESTAMP})
    return {
        "team_id": team_id,
        "active": False,
        "note": "GCS access revoked. Folder, team ID, and name reservations are kept forever.",
    }


def main() -> None:
    """Parse arguments and dispatch to the requested command."""
    parser = argparse.ArgumentParser(description="Manage ForecastBench submission teams.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    register_parser = subparsers.add_parser("register", help="Register a new team.")
    register_parser.add_argument("--organization", required=True)
    register_parser.add_argument("--team-name", default="", help="Internal label, never public.")
    register_parser.add_argument("--emails", nargs="*", default=[])
    register_parser.add_argument("--service-accounts", nargs="*", default=[])
    register_parser.add_argument("--anonymous", action="store_true")
    register_parser.add_argument(
        "--mode",
        type=constants.RunMode,
        choices=list(constants.RunMode),
        default=constants.RunMode.TEST,
        metavar="{TEST,PROD}",
        help="TEST (default): no email is sent unless --send-email-in-test. PROD: email sent.",
    )
    register_parser.add_argument(
        "--send-email-in-test",
        action="store_true",
        help="In TEST mode, send the welcome email rerouted to SMTP_USER with a [TEST] prefix.",
    )

    deactivate_parser = subparsers.add_parser("deactivate", help="Deactivate a team.")
    deactivate_parser.add_argument("--team-id", required=True)

    args = parser.parse_args()
    try:
        if args.command == "register":
            result = register(
                organization=args.organization,
                emails=args.emails,
                service_accounts=args.service_accounts,
                team_name=args.team_name,
                anonymous=args.anonymous,
                run_mode=args.mode,
                send_email_in_test=args.send_email_in_test,
            )
        else:
            result = deactivate(team_id=args.team_id)
    except (ValueError, RuntimeError) as exception:
        logger.error(str(exception))
        sys.exit(1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
