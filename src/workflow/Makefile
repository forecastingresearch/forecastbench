all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main deploy-step1 deploy-acled deploy-fetch-and-update-data deploy-step2 deploy-resolve

WORKFLOW_BASE_URI=https://workflowexecutions.googleapis.com/v1/projects/$(CLOUD_PROJECT)/locations/$(CLOUD_DEPLOY_REGION)/workflows

setup-crons :
	gcloud services enable cloudscheduler.googleapis.com
	gcloud projects add-iam-policy-binding $(CLOUD_PROJECT) \
	  --member serviceAccount:$(WORKFLOW_SERVICE_ACCOUNT) \
	  --role roles/workflows.invoker
	- gcloud scheduler jobs create http main-workflow \
		--schedule="0 0 * * * " \
		--uri="$(WORKFLOW_BASE_URI)/main/executions" \
		--location=$(CLOUD_DEPLOY_REGION) \
		--oauth-service-account-email=$(WORKFLOW_SERVICE_ACCOUNT)
	- gcloud scheduler jobs create http acled-workflow \
		--schedule="0 0 * * 2,3" \
		--uri="$(WORKFLOW_BASE_URI)/acled/executions" \
		--location=$(CLOUD_DEPLOY_REGION) \
		--oauth-service-account-email=$(WORKFLOW_SERVICE_ACCOUNT)


deploy-main : main.yaml
	gcloud workflows deploy \
		main \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$<

deploy-step1 : step1.yaml
	gcloud workflows deploy \
		main-step1 \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$< \

# Run ACLED fetch/update separately because we only do it once/week
# See issue #115
deploy-acled : acled.yaml
	gcloud workflows deploy \
		acled \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$< \

TMP_DEPLOY_FILE_0 := tmp0.yaml
TMP_DEPLOY_FILE_1 := tmp1.yaml
TMP_DEPLOY_FILE_2 := tmp2.yaml

deploy-fetch-and-update-data : fetch_and_update_data_source.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE_0)
	gcloud workflows deploy \
		fetch_and_update_data_source \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE_0)
	rm -f $(TMP_DEPLOY_FILE_0)


deploy-step2 : step2.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE_1)
	gcloud workflows deploy \
		main-step2 \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE_1)
	rm -f $(TMP_DEPLOY_FILE_1)


deploy-resolve : resolve.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE_2)
	gcloud workflows deploy \
		main-resolve \
		--project $(CLOUD_PROJECT) \
		--service-account $(WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE_2)
	rm -f $(TMP_DEPLOY_FILE_2)


clean:
	rm -f $(TMP_DEPLOY_FILE_0) $(TMP_DEPLOY_FILE_1) $(TMP_DEPLOY_FILE_2)
