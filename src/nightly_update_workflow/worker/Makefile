all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

Procfile:
	cp -r $(ROOT_DIR)src/helpers/Procfile .

deploy : main.py requirements.txt ../nightly_update_workflow_helper.py Procfile
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud run jobs deploy \
		nightly-worker \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--task-timeout 1m \
		--memory 512Mi \
		--max-retries 0 \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),DICT_TO_USE=none_provided  \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR)
