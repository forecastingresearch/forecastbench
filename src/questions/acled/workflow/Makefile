all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main

TMP_DEPLOY_FILE := tmp.yaml

deploy-main : main.yaml
	sed 's/SUBDOMAIN/$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)/g' $< > $(TMP_DEPLOY_FILE)
	gcloud workflows deploy \
		acled-main \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$(TMP_DEPLOY_FILE)
	$(MAKE) clean

clean:
	rm -f $(TMP_DEPLOY_FILE)
