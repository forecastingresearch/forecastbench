#!/usr/bin/env python3
"""Set up IAM permissions for the ForecastBench submissions service account.

Run once per environment after creating the service account:

    eval $(cat ../../variables.mk | grep -v '^#' | xargs) python setup_permissions.py

Requirements:
    pip install google-cloud-storage google-cloud-resource-manager
"""

import os
import subprocess
import sys

from google.cloud import resourcemanager_v3, storage
from google.iam.v1 import iam_policy_pb2, policy_pb2


def _require(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        print(f"ERROR: {name} must be set", file=sys.stderr)
        sys.exit(1)
    return val


def _add_bucket_role(gcs: storage.Client, bucket_name: str, member: str, role: str) -> None:
    bucket = gcs.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.version = 3
    existing = next((b for b in policy.bindings if b["role"] == role), None)
    if existing:
        existing["members"].add(member)
    else:
        policy.bindings.append({"role": role, "members": {member}})
    bucket.set_iam_policy(policy)
    print(f"  gs://{bucket_name}: granted {role} to {member}")


def _add_project_role(
    rm: resourcemanager_v3.ProjectsClient, project: str, member: str, role: str
) -> None:
    resource = f"projects/{project}"
    policy = rm.get_iam_policy(request=iam_policy_pb2.GetIamPolicyRequest(resource=resource))
    for binding in policy.bindings:
        if binding.role == role:
            if member not in binding.members:
                binding.members.append(member)
                rm.set_iam_policy(
                    request=iam_policy_pb2.SetIamPolicyRequest(resource=resource, policy=policy)
                )
                print(f"  project/{project}: granted {role} to {member}")
            return
    policy.bindings.append(policy_pb2.Binding(role=role, members=[member]))
    rm.set_iam_policy(request=iam_policy_pb2.SetIamPolicyRequest(resource=resource, policy=policy))
    print(f"  project/{project}: granted {role} to {member}")


def main() -> None:
    """Set up IAM permissions for the submissions service account."""
    project = _require("CLOUD_PROJECT")
    sa_email = _require("SUBMISSIONS_SA_EMAIL")
    upload_bucket = _require("SUBMISSIONS_BUCKET")
    deployer = _require("SUBMISSIONS_DEPLOYER")

    interstitial_bucket = os.environ.get("SUBMISSIONS_INTERSTITIAL_BUCKET", "")
    forecast_sets_bucket = os.environ.get("FORECAST_SETS_BUCKET", "")
    history_bucket = os.environ.get("SUBMISSIONS_HISTORY_BUCKET", "")

    member = f"serviceAccount:{sa_email}"
    gcs = storage.Client()
    rm = resourcemanager_v3.ProjectsClient()

    print(f"Configuring permissions for {sa_email} in project {project}...")

    # Upload bucket: storage.admin — onboarding sets per-team conditional IAM bindings
    _add_bucket_role(gcs, upload_bucket, member, "roles/storage.admin")

    # Interstitial: objectAdmin — post-round may overwrite files on reruns
    if interstitial_bucket:
        _add_bucket_role(gcs, interstitial_bucket, member, "roles/storage.objectAdmin")

    # History + forecast-sets: write-only; paths include round_date, no collision risk
    for bucket in filter(None, [history_bucket, forecast_sets_bucket]):
        _add_bucket_role(gcs, bucket, member, "roles/storage.objectCreator")

    # Derive project number for Eventarc and GCS SAs
    project_obj = rm.get_project(name=f"projects/{project}")
    project_number = project_obj.name.split("/")[-1]

    # Eventarc SA: needs legacyBucketReader to validate the GCS trigger at deploy time
    eventarc_sa = f"serviceAccount:service-{project_number}@gcp-sa-eventarc.iam.gserviceaccount.com"
    _add_bucket_role(gcs, upload_bucket, eventarc_sa, "roles/storage.legacyBucketReader")

    # GCS SA: needs pubsub.publisher to emit object-finalized events via Eventarc
    gcs_sa = f"serviceAccount:service-{project_number}@gs-project-accounts.iam.gserviceaccount.com"
    _add_project_role(rm, project, gcs_sa, "roles/pubsub.publisher")

    # Project-level roles for the submissions SA
    for role in (
        "roles/datastore.user",
        "roles/eventarc.eventReceiver",
        "roles/run.invoker",
    ):
        _add_project_role(rm, project, member, role)

    # Allow the deploying user to deploy functions that run as this SA.
    # Uses gcloud since google-cloud-iam-admin is not a standard dependency.
    subprocess.run(
        [
            "gcloud",
            "iam",
            "service-accounts",
            "add-iam-policy-binding",
            sa_email,
            f"--project={project}",
            f"--member=user:{deployer}",
            "--role=roles/iam.serviceAccountUser",
        ],
        check=True,
    )
    print(f"  SA {sa_email}: granted roles/iam.serviceAccountUser to user:{deployer}")

    print("Done.")


if __name__ == "__main__":
    main()
