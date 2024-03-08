export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET
export GCP_PROJECT_ID
export MANIFOLD_QUESTIONS_PUBSUB_TOPIC_ID
export CLOUD_DEPLOY_REGION := us-central1
export SERVICE_ACCOUNT_JSON_PATH := $(ROOT_DIR)$(SERVICE_ACCOUNT_JSON)

.PHONY: all clean lint deploy

lint:
	isort .
	black .
	flake8 .
	pydocstyle .

clean:
	find . -type f -name "*~" -exec rm -f {} +

all: deploy

deploy: main-workflow set-min-instances leaderboard charts news manifold wikidata

workflows: main-workflow news-workflow manifold-workflow wikidata-workflow

main-workflow:
	make -C src/workflow

set-min-instances:
	make -C src/functions/set_min_instances

leaderboard:
	make -C src/functions/leaderboard

charts:
	make -C src/functions/charts

news: news-fetch news-workflow

news-workflow:
	make -C src/functions/news/workflow

news-fetch:
	make -C src/functions/news/fetch

manifold: manifold-question-generation manifold-forecast manifold-workflow

manifold-question-generation:
	make -C src/functions/manifold/question_generation

manifold-forecast:
	make -C src/functions/manifold/forecast

manifold-workflow:
	make -C src/functions/manifold/workflow

wikidata: wikidata-question-generation wikidata-forecast wikidata-workflow

wikidata-question-generation:
	make -C src/functions/wikidata/question_generation

wikidata-forecast:
	make -C src/functions/wikidata/forecast

wikidata-workflow:
	make -C src/functions/wikidata/workflow
