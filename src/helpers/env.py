"""Environment variables."""

import os

PROJECT_ID = os.environ.get("CLOUD_PROJECT")
CLOUD_DEPLOY_REGION = os.environ.get("CLOUD_DEPLOY_REGION")
QUESTION_BANK_BUCKET = os.environ.get("QUESTION_BANK_BUCKET")
QUESTION_SETS_BUCKET = os.environ.get("QUESTION_SETS_BUCKET")
FORECAST_SETS_BUCKET = os.environ.get("FORECAST_SETS_BUCKET")
PROCESSED_FORECAST_SETS_BUCKET = os.environ.get("PROCESSED_FORECAST_SETS_BUCKET")
PUBLIC_RELEASE_BUCKET = os.environ.get("PUBLIC_RELEASE_BUCKET")
WEBSITE_BUCKET = os.environ.get("WEBSITE_BUCKET")
LLM_BASELINE_DOCKER_IMAGE_NAME = os.environ.get("LLM_BASELINE_DOCKER_IMAGE_NAME")
LLM_BASELINE_DOCKER_REPO_NAME = os.environ.get("LLM_BASELINE_DOCKER_REPO_NAME")
LLM_BASELINE_PUB_SUB_TOPIC_NAME = os.environ.get("LLM_BASELINE_PUB_SUB_TOPIC_NAME")
LLM_BASELINE_STAGING_BUCKET = os.environ.get("LLM_BASELINE_STAGING_BUCKET")
LLM_BASELINE_SERVICE_ACCOUNT = os.environ.get("LLM_BASELINE_SERVICE_ACCOUNT")
LLM_BASELINE_STAGING_BUCKET = os.environ.get("LLM_BASELINE_STAGING_BUCKET")
LLM_BASELINE_NEWS_BUCKET = os.environ.get("LLM_BASELINE_NEWS_BUCKET")
