all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main

#	@sed -i '' 's|URL_SUBDOMAIN|$(CLOUD_DEPLOY_REGION)-$(CLOUD_PROJECT)|g' $<
deploy-main : main.yaml
	gcloud workflows deploy \
		manifold-main \
		--project $(CLOUD_PROJECT) \
		--service-account $(CLOUD_WORKFLOW_SERVICE_ACCOUNT) \
		--source=$<

