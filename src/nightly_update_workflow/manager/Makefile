all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload
DEPENDENCY_GROUP = nightly-manager

include $(ROOT_DIR)deploy_config.mk

deploy : main.py Dockerfile
	rm -rf $(UPLOAD_DIR)
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_fb_types.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(UPLOAD_DIR)/
	mkdir -p $(UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(UPLOAD_DIR)/orchestration/
	cp $^ $(UPLOAD_DIR)/
	$(call stage-lockfile,$(UPLOAD_DIR))
	gcloud run jobs deploy \
		nightly-manager \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 12h \
		--memory 2Gi \
		--max-retries 0 \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) Dockerfile
