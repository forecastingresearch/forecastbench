all :
	$(MAKE) deploy

.PHONY : all deploy

deploy : deploy-main deploy-step1

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


