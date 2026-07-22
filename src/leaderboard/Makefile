all : deploy

deploy :
	$(MAKE) clean
	$(MAKE) -j3 deploy-tournament deploy-baseline deploy-preliminary
	$(MAKE) clean

.PHONY : all clean deploy deploy-tournament deploy-baseline deploy-preliminary

TOURNAMENT_UPLOAD_DIR = upload-leaderboard-tournament
BASELINE_UPLOAD_DIR = upload-leaderboard-baseline
PRELIMINARY_UPLOAD_DIR = upload-leaderboard-preliminary

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

Dockerfile: $(ROOT_DIR)src/helpers/Dockerfile.template
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/$(RUNTIME_STACK)/g' \
		-e 's/PYTHON_VERSION/$(PYTHON_RUNTIME)/g' \
		$< > Dockerfile

NUM_CPUS = 8
LEADERBOARD_DEPENDENCIES = main.py llm_identities.py requirements.txt Dockerfile .gcloudignore

define PREPARE_UPLOAD_DIR
	rm -rf $1
	mkdir -p $1
	cp -r $(ROOT_DIR)src/helpers $1/
	cp -r $(ROOT_DIR)src/llm_forecaster $1/
	cp -r $(ROOT_DIR)src/sources $1/
	cp $(ROOT_DIR)src/_fb_types.py $1/
	cp $(ROOT_DIR)src/_schemas.py $1/
	mkdir -p $1/orchestration
	cp $(ROOT_DIR)src/orchestration/__init__.py $1/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $1/orchestration/
	cp $(LEADERBOARD_DEPENDENCIES) $1/
	cat $(ROOT_DIR)requirements.runtime.txt requirements.txt > $1/requirements.txt
endef

deploy-tournament : $(LEADERBOARD_DEPENDENCIES)
	$(call PREPARE_UPLOAD_DIR,$(TOURNAMENT_UPLOAD_DIR))
	gcloud run jobs deploy \
		func-leaderboard-tournament \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 4h \
		--memory 32Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(WORKSPACE_BUCKET),type=cloud-storage,bucket=$(WORKSPACE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WORKSPACE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WORKSPACE_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS),LEADERBOARD_TYPE=tournament \
		--source $(TOURNAMENT_UPLOAD_DIR)

deploy-baseline : $(LEADERBOARD_DEPENDENCIES)
	$(call PREPARE_UPLOAD_DIR,$(BASELINE_UPLOAD_DIR))
	gcloud run jobs deploy \
		func-leaderboard-baseline \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 4h \
		--memory 32Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(WORKSPACE_BUCKET),type=cloud-storage,bucket=$(WORKSPACE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WORKSPACE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WORKSPACE_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS),LEADERBOARD_TYPE=baseline \
		--source $(BASELINE_UPLOAD_DIR)

deploy-preliminary : $(LEADERBOARD_DEPENDENCIES)
	$(call PREPARE_UPLOAD_DIR,$(PRELIMINARY_UPLOAD_DIR))
	gcloud run jobs deploy \
		func-leaderboard-preliminary \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--parallelism 1 \
		--task-timeout 4h \
		--memory 32Gi \
		--cpu $(NUM_CPUS) \
		--max-retries 0 \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(WORKSPACE_BUCKET),type=cloud-storage,bucket=$(WORKSPACE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WORKSPACE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WORKSPACE_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS),NUM_CPUS=$(NUM_CPUS),LEADERBOARD_TYPE=preliminary \
		--source $(PRELIMINARY_UPLOAD_DIR)

clean :
	rm -rf $(TOURNAMENT_UPLOAD_DIR) $(BASELINE_UPLOAD_DIR) $(PRELIMINARY_UPLOAD_DIR) .gcloudignore Dockerfile
