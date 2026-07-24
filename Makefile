export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

MAKE_FAILURE_LOG ?= .make_failures.log
export MAKE_FAILURE_LOG

ifeq ($(MAKELEVEL),0)
ifeq (,$(filter check-failures,$(MAKECMDGOALS)))
  $(shell : > $(MAKE_FAILURE_LOG))
endif
endif

include *.mk
export CLOUD_PROJECT
export QUESTION_BANK_BUCKET
export QUESTION_BANK_BUCKET_SERVICE_ACCOUNT
export QUESTION_SETS_BUCKET
export WORKFLOW_SERVICE_ACCOUNT
export LLM_BASELINE_SERVICE_ACCOUNT
export FORECAST_SETS_BUCKET
export FORECAST_SETS_TRANSCRIPTS_BUCKET
export PROCESSED_FORECAST_SETS_BUCKET
export PUBLIC_RELEASE_BUCKET
export WEBSITE_BUCKET
export WEBSITE_STAGING_ASSETS_BUCKET
export LLM_BASELINE_STAGING_BUCKET
export LLM_BASELINE_NEWS_BUCKET
export BUILD_ENV
export WORKSPACE_BUCKET

export CLOUD_DEPLOY_REGION := us-central1

export LLM_BASELINE_DOCKER_IMAGE_NAME := llm-baselines
export LLM_BASELINE_DOCKER_REPO_NAME := llm-baselines
export LLM_BASELINE_PUB_SUB_TOPIC_NAME := run-llm-baselines

export BUCKET_MOUNT_POINT := /mnt

export DEFAULT_CLOUD_FUNCTION_ENV_VARS=CLOUD_PROJECT=$(CLOUD_PROJECT),GOOGLE_CLOUD_PROJECT=$(CLOUD_PROJECT),QUESTION_BANK_BUCKET=$(QUESTION_BANK_BUCKET),QUESTION_SETS_BUCKET=$(QUESTION_SETS_BUCKET),FORECAST_SETS_BUCKET=$(FORECAST_SETS_BUCKET),FORECAST_SETS_TRANSCRIPTS_BUCKET=$(FORECAST_SETS_TRANSCRIPTS_BUCKET),PROCESSED_FORECAST_SETS_BUCKET=$(PROCESSED_FORECAST_SETS_BUCKET),PUBLIC_RELEASE_BUCKET=$(PUBLIC_RELEASE_BUCKET),WEBSITE_BUCKET=$(WEBSITE_BUCKET),WEBSITE_STAGING_ASSETS_BUCKET=$(WEBSITE_STAGING_ASSETS_BUCKET),CLOUD_DEPLOY_REGION=$(CLOUD_DEPLOY_REGION),LLM_BASELINE_DOCKER_IMAGE_NAME=$(LLM_BASELINE_DOCKER_IMAGE_NAME),LLM_BASELINE_DOCKER_REPO_NAME=$(LLM_BASELINE_DOCKER_REPO_NAME),LLM_BASELINE_STAGING_BUCKET=$(LLM_BASELINE_STAGING_BUCKET),LLM_BASELINE_SERVICE_ACCOUNT=$(LLM_BASELINE_SERVICE_ACCOUNT),LLM_BASELINE_PUB_SUB_TOPIC_NAME=$(LLM_BASELINE_PUB_SUB_TOPIC_NAME),RUNNING_LOCALLY=0,BUCKET_MOUNT_POINT=$(BUCKET_MOUNT_POINT),WORKSPACE_BUCKET=$(WORKSPACE_BUCKET)

.PHONY: all clean lint test deploy install-requirements setup-python-env

MAKE_LINT_ERROR_OUT ?= 0
ISORT_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),--check-only,)
BLACK_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),--check,)
VENV_PYTHON := .venv/bin/python
ROOT_REQUIREMENTS_STAMP := .venv/.root-requirements-installed
ALL_REQUIREMENTS_STAMP := .venv/.all-requirements-installed
ROOT_REQUIREMENTS_NO_RUNTIME := .venv/.root-requirements-no-runtime.txt
SRC_REQUIREMENTS := $(shell find src -type f -name requirements.txt ! -path '*/upload/*')

lint:
	isort $(ISORT_FLAGS) .
	black $(BLACK_FLAGS) .
	flake8 .
	pydocstyle .

test: setup-python-env
	@. ${ROOT_DIR}.venv/bin/activate && python -m pytest src/tests/ $(ARGS)

clean:
	@find src -type d -mindepth 1 | while read dir; do \
		if [ -f "$$dir/Makefile" ]; then \
			echo "Running make clean in $$dir"; \
			$(MAKE) -C $$dir clean; \
			echo ""; \
		fi ; \
	done

$(VENV_PYTHON):
	rm -rf .venv
	python3 -m venv .venv

.venv: $(VENV_PYTHON)

$(ROOT_REQUIREMENTS_STAMP): Makefile requirements.txt requirements.runtime.txt | $(VENV_PYTHON)
	@grep -v -x -- "-r requirements.runtime.txt" requirements.txt > $(ROOT_REQUIREMENTS_NO_RUNTIME)
	@. ${ROOT_DIR}.venv/bin/activate && python -m pip install -r requirements.runtime.txt
	@. ${ROOT_DIR}.venv/bin/activate && python -m pip install -r $(ROOT_REQUIREMENTS_NO_RUNTIME)
	@touch $@

$(ALL_REQUIREMENTS_STAMP): Makefile $(ROOT_REQUIREMENTS_STAMP) $(SRC_REQUIREMENTS)
	@find src -type d | while read dir; do \
		if [ -f "$$dir/Makefile" ] && [ -f "$$dir/requirements.txt" ]; then \
			echo "\nInstalling requirements in $$dir"; \
			(cd $$dir && . ${ROOT_DIR}/.venv/bin/activate && python -m pip install -r requirements.txt) || exit $$?; \
			echo "Installation complete in $$dir\n"; \
		fi; \
	done
	@touch $@

install-requirements: $(ALL_REQUIREMENTS_STAMP)

setup-python-env: install-requirements
	@:

check-failures:
	@echo "Checking for failures..."
	@if [ -f $(MAKE_FAILURE_LOG) ] && [ -s $(MAKE_FAILURE_LOG) ]; then \
		echo ""; \
		echo "=========================================="; \
		echo "FAILED RULES:"; \
		echo "=========================================="; \
		cat $(MAKE_FAILURE_LOG); \
		echo "=========================================="; \
		echo ""; \
		exit 1; \
	else \
		echo ""; \
		echo "=========================================="; \
		echo "All rules completed successfully :)"; \
		echo "=========================================="; \
		echo ""; \
	fi

all: deploy
	@$(MAKE) check-failures

deploy: orchestration questions metadata resolve leaderboards curate-questions website baselines

questions: manifold metaculus acled infer kalshi yfinance polymarket wikipedia fred dbnomics

orchestration: nightly-worker-job nightly-manager-job compress_buckets

metadata: tag-questions validate-questions

resolve: resolve-forecasts

leaderboards: leaderboard-tournament leaderboard-baseline leaderboard-preliminary

curate-questions: create-question-set publish-question-set

website:
	$(MAKE) -C src/www.forecastbench.org || echo "* $@" >> $(MAKE_FAILURE_LOG)

create-question-set:
	$(MAKE) -C src/curate_questions/create_question_set || echo "* $@" >> $(MAKE_FAILURE_LOG)

publish-question-set:
	$(MAKE) -C src/curate_questions/publish_question_set || echo "* $@" >> $(MAKE_FAILURE_LOG)

baselines: llm-forecaster naive-and-dummy-forecasters

manifold: manifold-fetch manifold-update-questions

manifold-fetch:
	$(MAKE) -C src/orchestration/func_manifold_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

manifold-update-questions:
	$(MAKE) -C src/orchestration/func_manifold_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

metaculus: metaculus-fetch metaculus-update-questions

metaculus-fetch:
	$(MAKE) -C src/orchestration/func_metaculus_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

metaculus-update-questions:
	$(MAKE) -C src/orchestration/func_metaculus_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

infer: infer-fetch infer-update-questions

infer-fetch:
	$(MAKE) -C src/orchestration/func_infer_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

infer-update-questions:
	$(MAKE) -C src/orchestration/func_infer_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

acled: acled-fetch acled-update-questions

acled-fetch:
	$(MAKE) -C src/questions/acled/fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

acled-update-questions:
	$(MAKE) -C src/questions/acled/update_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

kalshi: kalshi-fetch kalshi-update-questions

kalshi-fetch:
	$(MAKE) -C src/orchestration/func_kalshi_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

kalshi-update-questions:
	$(MAKE) -C src/orchestration/func_kalshi_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

yfinance: yfinance-fetch yfinance-update-questions

yfinance-fetch:
	$(MAKE) -C src/orchestration/func_yfinance_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

yfinance-update-questions:
	$(MAKE) -C src/orchestration/func_yfinance_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

polymarket: polymarket-fetch polymarket-update-questions

polymarket-fetch:
	$(MAKE) -C src/orchestration/func_polymarket_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

polymarket-update-questions:
	$(MAKE) -C src/orchestration/func_polymarket_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

wikipedia: wikipedia-fetch wikipedia-update-questions

wikipedia-fetch:
	$(MAKE) -C src/questions/wikipedia/fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

wikipedia-update-questions:
	$(MAKE) -C src/questions/wikipedia/update_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

fred: fred-fetch fred-update-questions

fred-fetch:
	$(MAKE) -C src/questions/fred/fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

fred-update-questions:
	$(MAKE) -C src/questions/fred/update_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

dbnomics: dbnomics-fetch dbnomics-update-questions

dbnomics-fetch:
	$(MAKE) -C src/questions/dbnomics/fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

dbnomics-update-questions:
	$(MAKE) -C src/questions/dbnomics/update_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

tag-questions:
	$(MAKE) -C src/metadata/tag_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

validate-questions:
	$(MAKE) -C src/metadata/validate_questions || echo "* $@" >> $(MAKE_FAILURE_LOG)

resolve-forecasts:
	$(MAKE) -C src/orchestration/func_resolve || echo "* $@" >> $(MAKE_FAILURE_LOG)

naive-and-dummy-forecasters:
	$(MAKE) -C src/base_eval/naive_and_dummy_forecasters || echo "* $@" >> $(MAKE_FAILURE_LOG)

leaderboard-prereqs:
	$(MAKE) -C src/leaderboard Dockerfile .gcloudignore

leaderboard-tournament: leaderboard-prereqs
	$(MAKE) -C src/leaderboard deploy-tournament || echo "* $@" >> $(MAKE_FAILURE_LOG)

leaderboard-baseline: leaderboard-prereqs
	$(MAKE) -C src/leaderboard deploy-baseline || echo "* $@" >> $(MAKE_FAILURE_LOG)

leaderboard-preliminary: leaderboard-prereqs
	$(MAKE) -C src/leaderboard deploy-preliminary || echo "* $@" >> $(MAKE_FAILURE_LOG)

nightly-worker-job:
	$(MAKE) -C src/nightly_update_workflow/worker || echo "* $@" >> $(MAKE_FAILURE_LOG)

nightly-manager-job:
	$(MAKE) -C src/nightly_update_workflow/manager || echo "* $@" >> $(MAKE_FAILURE_LOG)

llm-forecaster: llm-forecaster-manager llm-forecaster-worker

llm-forecaster-manager:
	$(MAKE) -C src/orchestration/func_llm_forecaster_manager || echo "* $@" >> $(MAKE_FAILURE_LOG)

llm-forecaster-worker:
	$(MAKE) -C src/orchestration/func_llm_forecaster_worker || echo "* $@" >> $(MAKE_FAILURE_LOG)

compress_buckets:
	$(MAKE) -C src/nightly_update_workflow/compress_buckets || echo "* $@" >> $(MAKE_FAILURE_LOG)
