all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main deploy-step1 deploy-fetch-and-update-data

deploy-main : main.yaml
	gcloud workflows deploy \
		main \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$<

deploy-step1 : step1.yaml
	gcloud workflows deploy \
		main-step1 \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$< \

TMP_DEPLOY_FILE := tmp.yaml

deploy-fetch-and-update-data : fetch_and_update_data_source.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE)
	gcloud workflows deploy \
		fetch_and_update_data_source \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE)
	$(MAKE) clean


clean:
	rm -f $(TMP_DEPLOY_FILE)
