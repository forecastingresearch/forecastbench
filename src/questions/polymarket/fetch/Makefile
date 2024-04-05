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

# Python 3.9 runtime required for `py_clob_client` library
deploy : main.py .gcloudignore requirements.txt
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud functions deploy \
		polymarket_fetch \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--entry-point driver \
		--runtime python39 \
		--memory 2GiB \
		--max-instances 1 \
		--timeout 3000s \
		--service-account $(CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT) \
		--trigger-http \
		--no-allow-unauthenticated \
		--gen2 \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore
