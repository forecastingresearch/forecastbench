export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export SERVICE_ACCOUNT
export CLOUD_DEPLOY_REGION := us-central1

lint:
	isort .
	black .
	flake8 .
	pydocstyle .

clean:
	find . -type f -name "*~" -exec rm -f {} +
	cd src/gpt && rm -rf plotly_charts && rm -f table_of_contents.html


deploy: set_min_instances manifold-question-generation

set_min_instances:
	make -C src/gcp/functions/set_min_instances

manifold-question-generation:
	make -C src/gcp_functions/question_generation/manifold
