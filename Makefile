export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export CLOUD_PROJECT
export CLOUD_STORAGE_BUCKET_QUESTION_BANK
export CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET_QUESTIONS
export CLOUD_WORKFLOW_SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET_PUBLIC_LEADERBOARD

export CLOUD_DEPLOY_REGION := us-central1

export DEFAULT_CLOUD_FUNCTION_ENV_VARS=CLOUD_STORAGE_BUCKET=$(CLOUD_STORAGE_BUCKET_QUESTION_BANK),CLOUD_STORAGE_BUCKET_QUESTIONS=$(CLOUD_STORAGE_BUCKET_QUESTIONS),CLOUD_PROJECT=$(CLOUD_PROJECT),CLOUD_STORAGE_BUCKET_PROCESSED_FORECASTS=$(CLOUD_STORAGE_BUCKET_PROCESSED_FORECASTS),CLOUD_STORAGE_BUCKET_FORECASTS=$(CLOUD_STORAGE_BUCKET_FORECASTS),CLOUD_STORAGE_BUCKET_PUBLIC_LEADERBOARD=$(CLOUD_STORAGE_BUCKET_PUBLIC_LEADERBOARD)

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

deploy: questions workflows metadata curate-questions resolve naive-forecaster leaderboard

questions: manifold metaculus acled infer yfinance polymarket wikipedia fred

workflows: main-workflow

metadata: tag-questions validate-questions

resolve: resolve-forecasts

curate-questions:
	make -C src/curate_questions

main-workflow:
	make -C src/workflow

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

tag-questions:
	make -C src/metadata/tag_questions

validate-questions:
	make -C src/metadata/validate_questions

resolve-forecasts:
	make -C src/resolve_forecasts

naive-forecaster:
	make -C src/base_eval/naive_forecaster

leaderboard:
	make -C src/leaderboard
