all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

deploy : main.py .gcloudignore requirements.txt Dockerfile
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_fb_types.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(UPLOAD_DIR)/
	mkdir -p $(UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(UPLOAD_DIR)/orchestration/
	cp $^ $(UPLOAD_DIR)/
	gcloud run jobs deploy \
		nightly-bucket-compression \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--task-timeout 1m \
		--memory 16Gi \
		--max-retries 0 \
		--cpu 8 \
		--add-volume name=$(QUESTION_BANK_BUCKET),type=cloud-storage,bucket=$(QUESTION_BANK_BUCKET),readonly=true \
		--add-volume-mount volume=$(QUESTION_BANK_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(QUESTION_BANK_BUCKET) \
		--add-volume name=$(FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(FORECAST_SETS_BUCKET) \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),BUCKET_TO_COMPRESS= \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore
