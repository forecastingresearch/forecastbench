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
		metaculus_update_questions \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--entry-point driver \
		--runtime $(LATEST_PYTHON_RUNTIME) \
		--memory 512MB \
		--max-instances 1 \
		--timeout 540s \
		--service-account $(CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT) \
		--trigger-http \
		--no-allow-unauthenticated \
		--no-gen2 \
		--set-env-vars CLOUD_STORAGE_BUCKET=$(CLOUD_STORAGE_BUCKET_QUESTION_BANK) \
		--set-secrets API_KEY_METACULUS=API_KEY_METACULUS:latest \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore
