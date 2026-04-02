all :
	$(MAKE) clean
	$(MAKE) deploy
	$(MAKE) deploy-dataset
	$(MAKE) clean

.PHONY : all clean deploy deploy-dataset

LEADERBOARD_UPLOAD_DIR = upload-leaderboard
DATASET_UPLOAD_DIR = upload-dataset-leaderboard

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

Dockerfile: $(ROOT_DIR)src/helpers/Dockerfile.template
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/google-22-full/g' \
		-e 's/PYTHON_VERSION/python312/g' \
		$< > Dockerfile

NUM_CPUS = 8

deploy : main.py requirements.txt Dockerfile model_release_dates.csv
	mkdir -p $(LEADERBOARD_UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(LEADERBOARD_UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(LEADERBOARD_UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(LEADERBOARD_UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_fb_types.py $(LEADERBOARD_UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(LEADERBOARD_UPLOAD_DIR)/
	mkdir -p $(LEADERBOARD_UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(LEADERBOARD_UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(LEADERBOARD_UPLOAD_DIR)/orchestration/
	cp $^ $(LEADERBOARD_UPLOAD_DIR)/
	gcloud run jobs deploy \
		func-leaderboard \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 4h \
		--memory 16Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(WORKSPACE_BUCKET),type=cloud-storage,bucket=$(WORKSPACE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WORKSPACE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WORKSPACE_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS) \
		--source $(LEADERBOARD_UPLOAD_DIR)

deploy-dataset : main.py requirements.txt model_release_dates.csv
	rm -rf $(DATASET_UPLOAD_DIR)
	mkdir -p $(DATASET_UPLOAD_DIR)
	cp -r $(ROOT_DIR)utils $(DATASET_UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/helpers $(DATASET_UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(DATASET_UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_fb_types.py $(DATASET_UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(DATASET_UPLOAD_DIR)/
	mkdir -p $(DATASET_UPLOAD_DIR)/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $(DATASET_UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(DATASET_UPLOAD_DIR)/orchestration/
	cp main.py requirements.txt model_release_dates.csv $(DATASET_UPLOAD_DIR)/
	cp $(ROOT_DIR)src/helpers/.gcloudignore $(DATASET_UPLOAD_DIR)/.gcloudignore
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/google-22-full/g' \
		-e 's/PYTHON_VERSION/python312/g' \
		$(ROOT_DIR)src/helpers/Dockerfile.template > $(DATASET_UPLOAD_DIR)/Dockerfile
	gcloud run jobs deploy \
		func-leaderboard-dataset \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 4h \
		--memory 16Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(WORKSPACE_BUCKET),type=cloud-storage,bucket=$(WORKSPACE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WORKSPACE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WORKSPACE_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS),LEADERBOARD_MODE=dataset \
		--source $(DATASET_UPLOAD_DIR)

clean :
	rm -rf $(LEADERBOARD_UPLOAD_DIR) $(DATASET_UPLOAD_DIR) .gcloudignore Dockerfile
