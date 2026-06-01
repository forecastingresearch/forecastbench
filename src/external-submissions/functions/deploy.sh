#!/bin/bash
# Deploys all Cloud Functions to GCP.
# Copies common/ files into each function directory before deploying.
#
# Usage:
#   ./deploy.sh                  # deploy all functions
#   ./deploy.sh validate         # deploy one function
#
# Env vars (optional):
#   BUILD_ENV    dev or prod (default: prod)
#   MOCK_DATE    YYYY-MM-DD  (only active when BUILD_ENV=dev)
#   SA_EMAIL     service account the functions run as (default: submissions@forecastbench-dev...)

PROJECT=${CLOUD_PROJECT:-forecastbench-dev}
REGION=us-central1
SA_EMAIL=${SA_EMAIL:-submissions@forecastbench-dev.iam.gserviceaccount.com}
TRIGGER_LOCATION=${TRIGGER_LOCATION:-us-central1}

BUILD_ENV=${BUILD_ENV:-prod}
MOCK_DATE=${MOCK_DATE:-}

SHARED_ENV="BUILD_ENV=$BUILD_ENV"
[ -n "$MOCK_DATE" ] && SHARED_ENV="$SHARED_ENV,MOCK_DATE=$MOCK_DATE"

SMTP_USER=${SMTP_USER:-}
SMTP_PASSWORD=${SMTP_PASSWORD:-}
FRI_EMAIL=${FRI_EMAIL:-forecastbench@forecastingresearch.org}
SMTP_ENV="SMTP_USER=$SMTP_USER,SMTP_PASSWORD=$SMTP_PASSWORD,FRI_EMAIL=$FRI_EMAIL"

# Bucket names — override via env vars for sandbox (forecastbench-johan uses different naming)
_UPLOAD_BUCKET=${UPLOAD_BUCKET:-forecastbench-submissions-dev}
_INTERSTITIAL_BUCKET=${INTERSTITIAL_BUCKET:-forecastbench-submissions-interstitial-dev}
_FORECAST_SETS_BUCKET=${FORECAST_SETS_BUCKET:-forecastbench-forecast-sets-dev}
_HISTORY_BUCKET=${HISTORY_BUCKET:-forecastbench-submissions-history-dev}
BUCKET_ENV="UPLOAD_BUCKET=$_UPLOAD_BUCKET,INTERSTITIAL_BUCKET=$_INTERSTITIAL_BUCKET,FORECAST_SETS_BUCKET=$_FORECAST_SETS_BUCKET,HISTORY_BUCKET=$_HISTORY_BUCKET"

deploy_function() {
    local NAME=$1
    local DIR=$2
    local ENTRY=$3
    local TRIGGER=$4
    local TIMEOUT=${5:-60s}

    echo "--- Copying common/ into $DIR ---"
    cp common/validation.py "$DIR/validation.py"
    cp common/email.py "$DIR/email_utils.py"
    cp common/utils.py "$DIR/utils.py"

    echo "--- Deploying $NAME ---"
    if [ "$TRIGGER" = "http" ]; then
        if [ "$NAME" = "validate-forecast" ]; then
            AUTH_FLAG="--no-allow-unauthenticated"
        else
            AUTH_FLAG="--no-allow-unauthenticated"
        fi

        gcloud functions deploy "$NAME" \
            --gen2 \
            --project="$PROJECT" \
            --region="$REGION" \
            --runtime=python311 \
            --source="$DIR" \
            --entry-point="$ENTRY" \
            --trigger-http \
            $AUTH_FLAG \
            --service-account="$SA_EMAIL" \
            --memory=256Mi \
            --timeout="$TIMEOUT" \
            --min-instances=1 \
            --set-env-vars="$SHARED_ENV,$SMTP_ENV,$BUCKET_ENV"

    elif [ "$TRIGGER" = "event" ]; then
        gcloud functions deploy "$NAME" \
            --gen2 \
            --project="$PROJECT" \
            --region="$REGION" \
            --runtime=python311 \
            --source="$DIR" \
            --entry-point="$ENTRY" \
            --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
            --trigger-event-filters="bucket=$_UPLOAD_BUCKET" \
            --trigger-location="$TRIGGER_LOCATION" \
            --service-account="$SA_EMAIL" \
            --memory=256Mi \
            --timeout="$TIMEOUT" \
            --set-env-vars="$SHARED_ENV,$SMTP_ENV,$BUCKET_ENV"
    fi

    echo "--- Cleaning up ---"
    rm "$DIR/validation.py"
    rm "$DIR/email_utils.py"
    rm "$DIR/utils.py"

    echo "--- $NAME deployed ---"
}

FUNC=${1:-all}

cd "$(dirname "$0")"

case "$FUNC" in
    validate)      deploy_function validate-forecast  validate        validate       http  60s  ;;
    onboard)       deploy_function onboard-team        onboard         onboard        http  120s ;;
    upload)        deploy_function on-submission       upload          on_submission  event 60s  ;;
    post_round)    deploy_function post-round          post_round      post_round     http  120s ;;
    send_reminders) deploy_function send-reminders     send_reminders  send_reminders http  60s  ;;
    all)
        deploy_function validate-forecast  validate        validate       http  60s
        deploy_function onboard-team       onboard         onboard        http  120s
        deploy_function on-submission      upload          on_submission  event 60s
        deploy_function post-round         post_round      post_round     http  120s
        deploy_function send-reminders     send_reminders  send_reminders http  60s
        ;;
    *)
        echo "Unknown function: $FUNC"
        echo "Usage: ./deploy.sh [validate|onboard|upload|post_round|all]"
        exit 1
        ;;
esac
