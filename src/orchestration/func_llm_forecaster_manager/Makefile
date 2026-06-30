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

deploy : main.py .gcloudignore requirements.txt Dockerfile
	$(stage-orchestration-upload)
	gcloud run jobs deploy \
		func-llm-forecaster-manager \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 24h \
		--memory 2Gi \
		--max-retries 0 \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),TEST_OR_PROD=$(if $(filter $(BUILD_ENV),prod),PROD,TEST) \
		--source $(UPLOAD_DIR)

clean:
	rm -rf $(UPLOAD_DIR) .gcloudignore Dockerfile
