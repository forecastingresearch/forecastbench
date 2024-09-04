all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

deploy : main.py .gcloudignore requirements.txt
	$(eval LATEST_PYTHON_RUNTIME := $(shell gcloud functions runtimes list --format="value(name)" --filter="python" --region $(CLOUD_DEPLOY_REGION) | tail -n 1))
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud functions deploy \
		infer_update_questions \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--entry-point driver \
		--runtime $(LATEST_PYTHON_RUNTIME) \
		--memory 2GiB \
		--max-instances 1 \
		--timeout 540s \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--trigger-http \
		--no-allow-unauthenticated \
		--no-gen2 \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore
