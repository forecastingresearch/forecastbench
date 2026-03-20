all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

Dockerfile: $(ROOT_DIR)src/helpers/Dockerfile.template
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/google-22-full/g' \
		-e 's/PYTHON_VERSION/python312/g' \
		$< > Dockerfile

NUM_CPUS = 2

deploy : .gcloudignore requirements.txt Dockerfile
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/helpers
	cp -r $(ROOT_DIR)src/sources $(UPLOAD_DIR)/sources
	cp -r $(ROOT_DIR)src/resolve $(UPLOAD_DIR)/resolve
	mkdir -p $(UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/_types.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(UPLOAD_DIR)/
	cp main.py $(UPLOAD_DIR)/main.py
	cp requirements.txt $(UPLOAD_DIR)/requirements.txt
	cp Dockerfile $(UPLOAD_DIR)/
	gcloud run jobs deploy \
		func-resolve-forecasts-new \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 50 \
		--task-timeout 2h \
		--memory 8Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(FORECAST_SETS_BUCKET) \
		--add-volume name=$(QUESTION_BANK_BUCKET),type=cloud-storage,bucket=$(QUESTION_BANK_BUCKET),readonly=true \
		--add-volume-mount volume=$(QUESTION_BANK_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(QUESTION_BANK_BUCKET) \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore Dockerfile
