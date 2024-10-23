all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

# additional .gcloudignore for polymayket to ignore llm_prompts.py, which does not work with
# python 3.9
GCLOUDIGNORE_ADDITIONAL = $(ROOT_DIR)src/questions/polymarket/.gcloudignore.add

.gcloudignore: $(GCLOUDIGNORE_ADDITIONAL)
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .
	cat $^ >> .gcloudignore

Procfile:
	cp -r $(ROOT_DIR)src/helpers/Procfile .

export IMAGE_NAME=$(CLOUD_DEPLOY_REGION)-docker.pkg.dev/$(CLOUD_PROJECT)/$(POLYMARKET_ARTIFACT_REPOSITORY)/polymarket-fetch

# Python 3.9 runtime required for `py_clob_client` library
deploy : main.py .gcloudignore requirements.txt Procfile
	@echo "Checking if repository '$(POLYMARKET_ARTIFACT_REPOSITORY)' exists..."
	@if gcloud artifacts repositories describe $(POLYMARKET_ARTIFACT_REPOSITORY) --location $(CLOUD_DEPLOY_REGION) --project $(CLOUD_PROJECT) > /dev/null 2>&1; then \
		echo "Repository '$(POLYMARKET_ARTIFACT_REPOSITORY)' exists."; \
	else \
		gcloud artifacts repositories create $(POLYMARKET_ARTIFACT_REPOSITORY) \
			--project $(CLOUD_PROJECT) \
			--location $(CLOUD_DEPLOY_REGION) \
			--repository-format=docker; \
		echo "Created repository '$(POLYMARKET_ARTIFACT_REPOSITORY)'."; \
	fi
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud builds submit $(UPLOAD_DIR) \
		--pack builder=gcr.io/buildpacks/builder,env=GOOGLE_PYTHON_VERSION="3.9.x",image=$(IMAGE_NAME) \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION)
	gcloud run jobs deploy \
		func-data-polymarket-fetch \
	 	--project $(CLOUD_PROJECT) \
	 	--region $(CLOUD_DEPLOY_REGION) \
	 	--tasks 1 \
	 	--parallelism 1 \
	 	--task-timeout 3000s \
	 	--memory 1Gi \
	 	--max-retries 0 \
	 	--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
	 	--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),GOOGLE_PYTHON_VERSION="3.9.x" \
	 	--image $(IMAGE_NAME)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore Procfile
