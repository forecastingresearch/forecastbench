all :
	$(MAKE) clean
	$(MAKE) deploy

.PHONY : all clean deploy

UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

JEKYLL_ENV := $(if $(filter $(BUILD_ENV),prod),production,development)

entrypoint.sh: entrypoint.sh.template
	sed \
		-e 's|BUCKET_MOUNT_POINT|$(BUCKET_MOUNT_POINT)|g' \
		-e 's/PUBLIC_RELEASE_BUCKET/$(PUBLIC_RELEASE_BUCKET)/g' \
		-e 's/PROCESSED_FORECAST_SETS_BUCKET/$(PROCESSED_FORECAST_SETS_BUCKET)/g' \
		-e 's/FORECAST_SETS_BUCKET/$(FORECAST_SETS_BUCKET)/g' \
		-e 's/WEBSITE_STAGING_ASSETS_BUCKET/$(WEBSITE_STAGING_ASSETS_BUCKET)/g' \
		-e 's/WEBSITE_BUCKET/$(WEBSITE_BUCKET)/g' \
		-e 's/BUILD_ENV/$(JEKYLL_ENV)/g' \
		$< > $@

WEBSITE_VOLUME := $(subst .,-,$(WEBSITE_BUCKET))

deploy : entrypoint.sh Dockerfile .gcloudignore
	mkdir -p $(UPLOAD_DIR)
	rsync -a \
		--exclude='.*' \
		--exclude='*~' \
		--exclude='\#*' \
		--exclude='Makefile' \
		--exclude='entrypoint.sh.template' \
		--exclude='_site/' \
		--exclude='$(UPLOAD_DIR)/' \
		--exclude='assets/js/leaderboard_baseline_compact.js' \
		--exclude='assets/js/leaderboard_baseline_full.js' \
		--exclude='assets/js/leaderboard_tournament_compact.js' \
		--exclude='assets/js/leaderboard_tournament_full.js' \
		--exclude='assets/data/question-fixed-effects/placeholder.json' \
		--exclude='assets/data/sota_graph_tournament.csv' \
		--exclude='assets/data/parity_dates.json' \
		./ $(UPLOAD_DIR)/
	gcloud run jobs deploy \
		func-website \
		--project $(CLOUD_PROJECT) \
		--region $(CLOUD_DEPLOY_REGION) \
		--tasks 1 \
		--task-timeout 15m \
		--memory 4Gi \
		--max-retries 0 \
		--cpu 1 \
		--add-volume name=$(FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(FORECAST_SETS_BUCKET) \
		--add-volume name=$(PROCESSED_FORECAST_SETS_BUCKET),type=cloud-storage,bucket=$(PROCESSED_FORECAST_SETS_BUCKET),readonly=true \
		--add-volume-mount volume=$(PROCESSED_FORECAST_SETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PROCESSED_FORECAST_SETS_BUCKET) \
		--add-volume name=$(PUBLIC_RELEASE_BUCKET),type=cloud-storage,bucket=$(PUBLIC_RELEASE_BUCKET),readonly=false \
		--add-volume-mount volume=$(PUBLIC_RELEASE_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(PUBLIC_RELEASE_BUCKET) \
		--add-volume name=$(WEBSITE_VOLUME),type=cloud-storage,bucket=$(WEBSITE_BUCKET),readonly=false \
		--add-volume-mount volume=$(WEBSITE_VOLUME),mount-path=$(BUCKET_MOUNT_POINT)/$(WEBSITE_BUCKET) \
		--add-volume name=$(WEBSITE_STAGING_ASSETS_BUCKET),type=cloud-storage,bucket=$(WEBSITE_STAGING_ASSETS_BUCKET),readonly=false \
		--add-volume-mount volume=$(WEBSITE_STAGING_ASSETS_BUCKET),mount-path=$(BUCKET_MOUNT_POINT)/$(WEBSITE_STAGING_ASSETS_BUCKET) \
		--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT) \
		--set-env-vars $(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--source $(UPLOAD_DIR)

clean :
	rm -rf $(UPLOAD_DIR) .gcloudignore entrypoint.sh _site
