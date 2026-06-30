all:
	$(MAKE) clean
	$(MAKE) deploy

.PHONY: all clean deploy

UPLOAD_DIR = upload
ROOT_DIR ?= $(abspath ../../..)/
include $(ROOT_DIR)orchestration_upload.mk

ORCHESTRATION_EXTRA_PACKAGES = llm_forecaster

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

Dockerfile: $(ROOT_DIR)src/helpers/Dockerfile.template
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/google-22-full/g' \
		-e 's/PYTHON_VERSION/python312/g' \
		$< > Dockerfile

NUM_CPUS = 4

deploy : main.py .gcloudignore requirements.txt Dockerfile
	$(stage-orchestration-upload)
	gcloud run jobs deploy \
		func-llm-forecaster-worker \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--task-timeout 24h \
		--memory 16Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(FORECAST_SETS_BUCKET) \
		--add-volume-mount volume=$(FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(FORECAST_SETS_BUCKET) \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS),FORECAST_DUE_DATE=,TEST_OR_PROD=TEST \
		--source $(UPLOAD_DIR)

clean:
	rm -rf $(UPLOAD_DIR) .gcloudignore Dockerfile
