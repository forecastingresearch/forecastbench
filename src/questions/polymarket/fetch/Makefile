all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

Procfile:
	cp -r $(ROOT_DIR)src/helpers/Procfile .

deploy : main.py .gcloudignore requirements.txt Procfile
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_types.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(UPLOAD_DIR)/
	mkdir -p $(UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(UPLOAD_DIR)/orchestration/
	cp $^ $(UPLOAD_DIR)/
	gcloud run jobs deploy \
		func-data-polymarket-fetch \
	 	--project $(CLOUD_PROJECT) \
	 	--region $(CLOUD_DEPLOY_REGION) \
	 	--tasks 1 \
	 	--parallelism 1 \
	 	--task-timeout 3h \
	 	--memory 4Gi \
	 	--max-retries 0 \
	 	--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore Procfile
