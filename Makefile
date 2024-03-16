export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export CLOUD_PROJECT
export CLOUD_STORAGE_BUCKET_QUESTION_BANK
export CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET_QUESTIONS

export CLOUD_DEPLOY_REGION := us-central1

.PHONY: all clean lint deploy

lint:
	isort .
	black .
	flake8 .
	pydocstyle .

clean:
	find . -type f -name "*~" -exec rm -f {} +

all: deploy

deploy: questions leaderboard

questions: manifold wikidata_heads_of_state_gov

manifold:
	make -C src/questions/manifold

wikidata-heads-of_state-gov:
	make -C src/questions/wikidata_heads_of_state_gov

leaderboard:
	make -C src/leaderboard
