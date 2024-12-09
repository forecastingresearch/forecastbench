export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export CLOUD_PROJECT
export QUESTION_BANK_BUCKET
export QUESTION_BANK_BUCKET_SERVICE_ACCOUNT
export QUESTION_SETS_BUCKET
export WORKFLOW_SERVICE_ACCOUNT
export LLM_BASELINE_SERVICE_ACCOUNT
export FORECAST_SETS_BUCKET
export PROCESSED_FORECAST_SETS_BUCKET
export PUBLIC_RELEASE_BUCKET
export WEBSITE_BUCKET
export LLM_BASELINE_STAGING_BUCKET
export LLM_BASELINE_NEWS_BUCKET
export BUILD_ENV

export CLOUD_DEPLOY_REGION := us-central1

export LLM_BASELINE_DOCKER_IMAGE_NAME := llm-baselines
export LLM_BASELINE_DOCKER_REPO_NAME := llm-baselines
export LLM_BASELINE_PUB_SUB_TOPIC_NAME := run-llm-baselines

export DEFAULT_CLOUD_FUNCTION_ENV_VARS=CLOUD_PROJECT=$(CLOUD_PROJECT),QUESTION_BANK_BUCKET=$(QUESTION_BANK_BUCKET),QUESTION_SETS_BUCKET=$(QUESTION_SETS_BUCKET),FORECAST_SETS_BUCKET=$(FORECAST_SETS_BUCKET),PROCESSED_FORECAST_SETS_BUCKET=$(PROCESSED_FORECAST_SETS_BUCKET),PUBLIC_RELEASE_BUCKET=$(PUBLIC_RELEASE_BUCKET),WEBSITE_BUCKET=$(WEBSITE_BUCKET),CLOUD_DEPLOY_REGION=$(CLOUD_DEPLOY_REGION),LLM_BASELINE_DOCKER_IMAGE_NAME=$(LLM_BASELINE_DOCKER_IMAGE_NAME),LLM_BASELINE_DOCKER_REPO_NAME=$(LLM_BASELINE_DOCKER_REPO_NAME),LLM_BASELINE_STAGING_BUCKET=$(LLM_BASELINE_STAGING_BUCKET),LLM_BASELINE_SERVICE_ACCOUNT=$(LLM_BASELINE_SERVICE_ACCOUNT),LLM_BASELINE_PUB_SUB_TOPIC_NAME=$(LLM_BASELINE_PUB_SUB_TOPIC_NAME)

.PHONY: all clean lint deploy

MAKE_LINT_ERROR_OUT ?= 0
ISORT_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),--check-only,)
BLACK_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),--check,)

lint:
	isort $(ISORT_FLAGS) .
	black $(BLACK_FLAGS) .
	flake8 .
	pydocstyle .

clean:
	@find src -type d -mindepth 1 | while read dir; do \
		if [ -f "$$dir/Makefile" ]; then \
			echo "Running make clean in $$dir"; \
			make -C $$dir clean; \
			echo ""; \
		fi ; \
	done

.venv:
	python3 -m venv .venv

install-requirements:
	@. ${ROOT_DIR}.venv/bin/activate && ${ROOT_DIR}/.venv/bin/python3 -m pip install -r requirements.txt
	@find src -type d | while read dir; do \
		if [ -f "$$dir/Makefile" ] && [ -f "$$dir/requirements.txt" ]; then \
			echo "\nInstalling requirements in $$dir"; \
			(cd $$dir && . ${ROOT_DIR}/.venv/bin/activate && python3 -m pip install -r requirements.txt); \
			echo "Installation complete in $$dir\n"; \
		fi; \
	done

setup-python-env: .venv install-requirements
	@:

all: deploy

deploy: orchestration questions metadata resolve leaderboard curate-questions website baselines

questions: manifold metaculus acled infer yfinance polymarket wikipedia fred dbnomics

orchestration: nightly-worker-job nightly-manager-job

metadata: tag-questions validate-questions

resolve: resolve-forecasts

curate-questions: create-question-set publish-question-set

website:
	make -C src/www.forecastbench.org

create-question-set:
	make -C src/curate_questions/create_question_set

publish-question-set:
	make -C src/curate_questions/publish_question_set

baselines: llm-baselines naive-and-dummy-forecasters

manifold: manifold-fetch manifold-update-questions

manifold-fetch:
	make -C src/questions/manifold/fetch

manifold-update-questions:
	make -C src/questions/manifold/update_questions

metaculus: metaculus-fetch metaculus-update-questions

metaculus-fetch:
	make -C src/questions/metaculus/fetch

metaculus-update-questions:
	make -C src/questions/metaculus/update_questions

infer: infer-fetch infer-update-questions

infer-fetch:
	make -C src/questions/infer/fetch

infer-update-questions:
	make -C src/questions/infer/update_questions

acled: acled-fetch acled-update-questions

acled-fetch:
	make -C src/questions/acled/fetch

acled-update-questions:
	make -C src/questions/acled/update_questions

yfinance: yfinance-fetch yfinance-update-questions

yfinance-fetch:
	make -C src/questions/yfinance/fetch

yfinance-update-questions:
	make -C src/questions/yfinance/update_questions


polymarket: polymarket-fetch polymarket-update-questions

polymarket-fetch:
	make -C src/questions/polymarket/fetch

polymarket-update-questions:
	make -C src/questions/polymarket/update_questions

wikipedia: wikipedia-fetch wikipedia-update-questions

wikipedia-fetch:
	make -C src/questions/wikipedia/fetch

wikipedia-update-questions:
	make -C src/questions/wikipedia/update_questions

fred: fred-fetch fred-update-questions

fred-fetch:
	make -C src/questions/fred/fetch

fred-update-questions:
	make -C src/questions/fred/update_questions

dbnomics: dbnomics-fetch dbnomics-update-questions

dbnomics-fetch:
	make -C src/questions/dbnomics/fetch

dbnomics-update-questions:
	make -C src/questions/dbnomics/update_questions

tag-questions:
	make -C src/metadata/tag_questions

validate-questions:
	make -C src/metadata/validate_questions

resolve-forecasts:
	make -C src/resolve_forecasts

naive-and-dummy-forecasters:
	make -C src/base_eval/naive_and_dummy_forecasters

leaderboard:
	make -C src/leaderboard

nightly-worker-job:
	make -C src/nightly_update_workflow/worker

nightly-manager-job:
	make -C src/nightly_update_workflow/manager

llm-baselines: llm-baseline-manager llm-baseline-worker

llm-baseline-manager:
	make -C src/base_eval/llm_baselines/manager

llm-baseline-worker:
	make -C src/base_eval/llm_baselines/worker
