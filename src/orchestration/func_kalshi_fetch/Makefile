all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload
ROOT_DIR ?= $(abspath ../../..)/
include $(ROOT_DIR)orchestration_upload.mk

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
		func-data-kalshi-fetch \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 1800s \
		--memory 1Gi \
		--max-retries 0 \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore Dockerfile
