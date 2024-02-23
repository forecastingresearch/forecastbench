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

deploy: set-min-instances leaderboard manifold

set-min-instances:
	make -C src/gcp/functions/set_min_instances

leaderboard:
	make -C src/gcp/functions/leaderboard

manifold: manifold-question-generation manifold-forecast manifold-workflow

manifold-question-generation:
	make -C src/gcp/functions/manifold/question_generation

manifold-forecast:
	make -C src/gcp/functions/manifold/forecast

manifold-workflow:
	make -C src/gcp/functions/manifold/workflow
