export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET
export GCP_PROJECT_ID
export MANIFOLD_QUESTIONS_PUBSUB_TOPIC_ID
export CLOUD_DEPLOY_REGION := us-central1
export SERVICE_ACCOUNT_JSON_PATH := $(ROOT_DIR)$(SERVICE_ACCOUNT_JSON)

lint:
	isort .
	black .
	flake8 .
	pydocstyle .

clean:
	find . -type f -name "*~" -exec rm -f {} +
	cd src/gpt && rm -rf plotly_charts && rm -f table_of_contents.html

deploy: infrastructure manifold

infrastructure: router set_min_instances

router:
	make -C src/gcp/functions/router

set_min_instances:
	make -C src/gcp/functions/set_min_instances

manifold: manifold-question-generation manifold-forecast

manifold-question-generation:
	make -C src/gcp/functions/manifold/question_generation

manifold-forecast:
	make -C src/gcp/functions/manifold/forecast
