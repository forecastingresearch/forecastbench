all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

Procfile:
	cp -r $(ROOT_DIR)src/helpers/Procfile .

deploy : main.py requirements.txt  Procfile
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
		nightly-manager \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 12h \
		--memory 1Gi \
		--max-retries 0 \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR)
