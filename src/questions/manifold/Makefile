all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

deploy : main.py .gcloudignore requirements.txt
	$(eval LATEST_PYTHON_RUNTIME := $(shell gcloud functions runtimes list --format="value(name)" --filter="python" --region $(CLOUD_DEPLOY_REGION) | tail -n 1))
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)gcp $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud functions deploy \
		question_generation_manifold \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--entry-point driver \
		--runtime $(LATEST_PYTHON_RUNTIME) \
		--memory 512MB \
		--max-instances 1 \
		--timeout 30s \
		--service-account $(CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT) \
		--trigger-topic cron_everyday_12AM_UTC \
		--no-gen2 \
		--set-env-vars CLOUD_STORAGE_BUCKET=$(CLOUD_STORAGE_BUCKET_QUESTION_BANK) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR)
