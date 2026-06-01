#!/bin/bash
# Sets up IAM permissions for the ForecastBench submissions service account.
# Run once per environment after creating the service account.
#
# Usage:
#   SA_EMAIL=submissions@forecastbench-dev.iam.gserviceaccount.com \
#   PROJECT=forecastbench-dev \
#   bash setup_permissions.sh

PROJECT=${PROJECT:-forecastbench-dev}
SA_EMAIL=${SA_EMAIL:-submissions@forecastbench-dev.iam.gserviceaccount.com}
MEMBER="serviceAccount:$SA_EMAIL"

UPLOAD_BUCKET=${UPLOAD_BUCKET:-forecastbench-submissions-dev}
INTERSTITIAL_BUCKET=${INTERSTITIAL_BUCKET:-forecastbench-submissions-interstitial-dev}
FORECAST_SETS_BUCKET=${FORECAST_SETS_BUCKET:-forecastbench-forecast-sets-dev}
HISTORY_BUCKET=${HISTORY_BUCKET:-forecastbench-submissions-history-dev}

echo "Configuring permissions for $SA_EMAIL in $PROJECT..."

# Upload bucket: full admin required — functions read/write/delete objects and
# onboarding sets per-team folder IAM conditions (needs setIamPolicy on bucket)
gcloud storage buckets add-iam-policy-binding "gs://$UPLOAD_BUCKET" \
    --member="$MEMBER" \
    --role="roles/storage.admin"

# Interstitial: objectAdmin — post-round copies files here; may overwrite on reruns
gcloud storage buckets add-iam-policy-binding "gs://$INTERSTITIAL_BUCKET" \
    --member="$MEMBER" \
    --role="roles/storage.objectAdmin"

# History + processing/forecast-sets: write only — archive and scoring buckets;
# paths include round_date so no collision risk; no delete or overwrite needed
for BUCKET in "$HISTORY_BUCKET" "$FORECAST_SETS_BUCKET"; do
    gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
        --member="$MEMBER" \
        --role="roles/storage.objectCreator"
done

# Eventarc service account: needs to read the upload bucket to validate the
# GCS trigger at deploy time (separate from the function's runtime SA)
EVENTARC_SA="serviceAccount:service-$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')@gcp-sa-eventarc.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding "gs://$UPLOAD_BUCKET" \
    --member="$EVENTARC_SA" \
    --role="roles/storage.legacyBucketReader"

# Firestore: read/write documents (teams, submissions, round_transfers)
gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$MEMBER" \
    --role="roles/datastore.user"

# SA needs to receive Eventarc events (required for GCS upload trigger)
gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$MEMBER" \
    --role="roles/eventarc.eventReceiver"

# GCS service account needs to publish events to Pub/Sub (used by Eventarc)
GCS_SA="serviceAccount:service-$(gcloud projects describe $PROJECT --format='value(projectNumber)')@gs-project-accounts.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$GCS_SA" \
    --role="roles/pubsub.publisher"

# Cloud Scheduler needs to invoke Cloud Run services (gen2 functions run on Cloud Run)
# The SA invokes itself — grant run.invoker so scheduler OIDC tokens are accepted
gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="$MEMBER" \
    --role="roles/run.invoker"

# Allow the deploying user to deploy functions that run as this SA
DEPLOYER=${DEPLOYER:-johan@forecastingresearch.org}
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --project="$PROJECT" \
    --member="user:$DEPLOYER" \
    --role="roles/iam.serviceAccountUser"

echo "Done."
