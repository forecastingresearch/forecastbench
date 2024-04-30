all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main deploy-step1 deploy-fetch-and-update-data deploy-step2

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


TMP_DEPLOY_FILE_0 := tmp0.yaml
TMP_DEPLOY_FILE_1 := tmp1.yaml

deploy-fetch-and-update-data : fetch_and_update_data_source.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE_0)
	gcloud workflows deploy \
		fetch_and_update_data_source \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE_0)
	rm -f $(TMP_DEPLOY_FILE_0)


deploy-step2 : step2.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE_1)
	gcloud workflows deploy \
		main-step2 \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE_1)
	rm -f $(TMP_DEPLOY_FILE_1)


clean:
	rm -f $(TMP_DEPLOY_FILE_0) $(TMP_DEPLOY_FILE_1)
