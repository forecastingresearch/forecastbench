all:
	$(MAKE) clean
	$(MAKE) deploy

.PHONY: all clean deploy

UPLOAD_DIR_DOCKER = upload_docker
UPLOAD_DIR = upload

.gcloudignore:
	cp -r $(ROOT_DIR)src/helpers/.gcloudignore .

# Deploy Docker image and Cloud Function
deploy: deploy-image deploy-function

# Build and push Docker image with Cloud Build
deploy-image: Dockerfile run_notebook.py notebook.ipynb .gcloudignore requirements.txt cloudbuild.yaml
	mkdir -p $(UPLOAD_DIR_DOCKER)
	cp -r $(ROOT_DIR)utils $(UPLOAD_DIR_DOCKER)/
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR_DOCKER)/
	cp $^ $(UPLOAD_DIR_DOCKER)/

	gcloud builds submit $(UPLOAD_DIR_DOCKER) --config=$(UPLOAD_DIR_DOCKER)/cloudbuild.yaml \
        --substitutions=_FORECAST_SETS_BUCKET=$(FORECAST_SETS_BUCKET),_QUESTION_SETS_BUCKET=$(QUESTION_SETS_BUCKET),_CLOUD_PROJECT=$(CLOUD_PROJECT),_CLOUD_DEPLOY_REGION=$(CLOUD_DEPLOY_REGION),_REPO_NAME=$(LLM_BASELINE_DOCKER_REPO_NAME),_IMAGE_NAME=$(LLM_BASELINE_DOCKER_IMAGE_NAME)

# Deploy the Cloud Function
deploy-function: main.py run_notebook.py notebook.ipynb .gcloudignore requirements.txt
	$(eval LATEST_PYTHON_RUNTIME := $(shell gcloud functions runtimes list --format="value(name)" --filter="python" --region $(CLOUD_DEPLOY_REGION) | tail -n 1))
	mkdir -p $(UPLOAD_DIR)
	cp -r $(ROOT_DIR)/src/helpers $(UPLOAD_DIR)/
	cp $^ $(UPLOAD_DIR)/
	gcloud functions deploy trigger_vertex_ai \
		--project=$(CLOUD_PROJECT) \
		--region=$(CLOUD_DEPLOY_REGION) \
		--runtime=$(LATEST_PYTHON_RUNTIME) \
		--trigger-topic=$(LLM_BASELINE_PUB_SUB_TOPIC_NAME) \
		--entry-point=trigger_vertex_ai \
		--source=$(UPLOAD_DIR) \
		--timeout=540s \
		--service-account=$(LLM_BASELINE_SERVICE_ACCOUNT) \
		--no-allow-unauthenticated \
		--no-gen2 \
		--set-env-vars=$(DEFAULT_CLOUD_FUNCTION_ENV_VARS) \
		--max-instances 1 \
		--memory=4GiB

clean:
	rm -rf $(UPLOAD_DIR_DOCKER) $(UPLOAD_DIR) .gcloudignore
