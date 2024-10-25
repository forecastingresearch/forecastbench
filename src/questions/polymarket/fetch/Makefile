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

Dockerfile:
	sed 's/REGION/$(CLOUD_DEPLOY_REGION)/g' $(ROOT_DIR)src/helpers/DockerfilePython39 > Dockerfile

# Python 3.9 runtime required for `py_clob_client` library
deploy : main.py .gcloudignore requirements.txt Dockerfile
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
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
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore Dockerfile
