export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

MAKE_FAILURE_LOG ?= .make_failures.log
export MAKE_FAILURE_LOG

ifeq ($(MAKELEVEL),0)
ifeq (,$(filter check-failures,$(MAKECMDGOALS)))
  $(shell : > $(MAKE_FAILURE_LOG))
endif
endif

.DEFAULT_GOAL := lint

-include variables.mk
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

export RUNTIME_STACK := google-24-full
export PYTHON_RUNTIME := python$(subst .,,$(strip $(shell cat $(ROOT_DIR).python-version)))

export LLM_BASELINE_DOCKER_IMAGE_NAME := llm-baselines
export LLM_BASELINE_DOCKER_REPO_NAME := llm-baselines
export LLM_BASELINE_PUB_SUB_TOPIC_NAME := run-llm-baselines

export BUCKET_MOUNT_POINT := /mnt

export DEFAULT_CLOUD_FUNCTION_ENV_VARS=CLOUD_PROJECT=$(CLOUD_PROJECT),GOOGLE_CLOUD_PROJECT=$(CLOUD_PROJECT),QUESTION_BANK_BUCKET=$(QUESTION_BANK_BUCKET),QUESTION_SETS_BUCKET=$(QUESTION_SETS_BUCKET),FORECAST_SETS_BUCKET=$(FORECAST_SETS_BUCKET),FORECAST_SETS_TRANSCRIPTS_BUCKET=$(FORECAST_SETS_TRANSCRIPTS_BUCKET),PROCESSED_FORECAST_SETS_BUCKET=$(PROCESSED_FORECAST_SETS_BUCKET),PUBLIC_RELEASE_BUCKET=$(PUBLIC_RELEASE_BUCKET),WEBSITE_BUCKET=$(WEBSITE_BUCKET),WEBSITE_STAGING_ASSETS_BUCKET=$(WEBSITE_STAGING_ASSETS_BUCKET),CLOUD_DEPLOY_REGION=$(CLOUD_DEPLOY_REGION),LLM_BASELINE_DOCKER_IMAGE_NAME=$(LLM_BASELINE_DOCKER_IMAGE_NAME),LLM_BASELINE_DOCKER_REPO_NAME=$(LLM_BASELINE_DOCKER_REPO_NAME),LLM_BASELINE_STAGING_BUCKET=$(LLM_BASELINE_STAGING_BUCKET),LLM_BASELINE_SERVICE_ACCOUNT=$(LLM_BASELINE_SERVICE_ACCOUNT),LLM_BASELINE_PUB_SUB_TOPIC_NAME=$(LLM_BASELINE_PUB_SUB_TOPIC_NAME),RUNNING_LOCALLY=0,BUCKET_MOUNT_POINT=$(BUCKET_MOUNT_POINT),WORKSPACE_BUCKET=$(WORKSPACE_BUCKET)

.PHONY: all clean lint test deploy setup-python-env

MAKE_LINT_ERROR_OUT ?= 0
RUFF_CHECK_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),,--fix)
RUFF_FORMAT_FLAGS := $(if $(filter 1,$(MAKE_LINT_ERROR_OUT)),--check,)
UV ?= uv
# Extra flags for `uv sync`, e.g. `UV_SYNC_FLAGS=--locked` to fail on a stale `uv.lock`.
UV_SYNC_FLAGS ?=

lint: setup-python-env
	$(UV) run --no-sync ruff check $(RUFF_CHECK_FLAGS) .
	$(UV) run --no-sync ruff format $(RUFF_FORMAT_FLAGS) .

test: setup-python-env
	$(UV) run --no-sync python -m pytest src/tests/ $(ARGS)

clean:
	@find src -type d -mindepth 1 | while read dir; do \
		if [ -f "$$dir/Makefile" ]; then \
			echo "Running make clean in $$dir"; \
			$(MAKE) -C $$dir clean; \
			echo ""; \
		fi ; \
	done

# Create the project environment (`.venv`, or `$UV_PROJECT_ENVIRONMENT` when set) on the Python
# version in `.python-version` and install every dependency group from `uv.lock`.
setup-python-env:
	@command -v $(UV) >/dev/null 2>&1 || { \
		echo "uv is required: https://docs.astral.sh/uv/getting-started/installation/"; \
		exit 1; \
	}
	$(UV) sync $(UV_SYNC_FLAGS)

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

questions: manifold metaculus acled infer kalshi yfinance polymarket wikipedia fred dbnomics serpapi

orchestration: nightly-worker-job nightly-manager-job compress_buckets push-datasets-to-git

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

manifold: manifold-update-questions

manifold-update-questions:
	$(MAKE) -C src/orchestration/func_manifold_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

metaculus: metaculus-fetch metaculus-update-questions

metaculus-fetch:
	$(MAKE) -C src/orchestration/func_metaculus_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

metaculus-update-questions:
	$(MAKE) -C src/orchestration/func_metaculus_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

infer: infer-update-questions

infer-update-questions:
	$(MAKE) -C src/orchestration/func_infer_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

acled: acled-fetch acled-update-questions

acled-fetch:
	$(MAKE) -C src/orchestration/func_acled_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

acled-update-questions:
	$(MAKE) -C src/orchestration/func_acled_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

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
	$(MAKE) -C src/orchestration/func_wikipedia_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

wikipedia-update-questions:
	$(MAKE) -C src/orchestration/func_wikipedia_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

fred: fred-fetch fred-update-questions

fred-fetch:
	$(MAKE) -C src/orchestration/func_fred_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

fred-update-questions:
	$(MAKE) -C src/orchestration/func_fred_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

dbnomics: dbnomics-fetch dbnomics-update-questions

dbnomics-fetch:
	$(MAKE) -C src/orchestration/func_dbnomics_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

dbnomics-update-questions:
	$(MAKE) -C src/orchestration/func_dbnomics_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

serpapi: serpapi-fetch serpapi-update-questions

serpapi-fetch:
	$(MAKE) -C src/orchestration/func_serpapi_fetch || echo "* $@" >> $(MAKE_FAILURE_LOG)

serpapi-update-questions:
	$(MAKE) -C src/orchestration/func_serpapi_update || echo "* $@" >> $(MAKE_FAILURE_LOG)

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

push-datasets-to-git:
	$(MAKE) -C src/orchestration/func_push_datasets_to_git || echo "* $@" >> $(MAKE_FAILURE_LOG)

llm-forecaster: llm-forecaster-manager llm-forecaster-worker

llm-forecaster-manager:
	$(MAKE) -C src/orchestration/func_llm_forecaster_manager || echo "* $@" >> $(MAKE_FAILURE_LOG)

llm-forecaster-worker:
	$(MAKE) -C src/orchestration/func_llm_forecaster_worker || echo "* $@" >> $(MAKE_FAILURE_LOG)

compress_buckets:
	$(MAKE) -C src/nightly_update_workflow/compress_buckets || echo "* $@" >> $(MAKE_FAILURE_LOG)
