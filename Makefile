export ROOT_DIR := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(ROOT_DIR))

include *.mk
export CLOUD_PROJECT
export CLOUD_STORAGE_BUCKET_QUESTION_BANK
export CLOUD_STORAGE_BUCKET_QUESTION_BANK_SERVICE_ACCOUNT
export CLOUD_STORAGE_BUCKET_QUESTIONS
export CLOUD_WORKFLOW_SERVICE_ACCOUNT

export CLOUD_DEPLOY_REGION := us-central1

.PHONY: all clean lint deploy

lint:
	isort .
	black .
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

all: deploy

deploy: questions leaderboard

questions: manifold metaculus wikidata infer yfinance

workflows: main-workflow manifold-workflow infer-workflow metaculus-workflow yfinance-workflow

main-workflow:
	make -C src/workflow

manifold: manifold-workflow manifold-fetch manifold-update-questions

manifold-workflow:
	make -C src/questions/manifold/workflow

manifold-fetch:
	make -C src/questions/manifold/fetch

manifold-update-questions:
	make -C src/questions/manifold/update_questions

metaculus: metaculus-workflow metaculus-fetch metaculus-update-questions

metaculus-workflow:
	make -C src/questions/metaculus/workflow

metaculus-fetch:
	make -C src/questions/metaculus/fetch

metaculus-update-questions:
	make -C src/questions/metaculus/update_questions

wikidata: wikidata-workflow wikidata-fetch wikidata-update-questions

wikidata-workflow:
	make -C src/questions/wikidata/workflow

wikidata-fetch:
	make -C src/questions/wikidata/fetch

wikidata-update-questions:
	make -C src/questions/wikidata/update_questions

infer: infer-workflow infer-fetch infer-update-questions

infer-workflow:
	make -C src/questions/infer/workflow

infer-fetch:
	make -C src/questions/infer/fetch

infer-update-questions:
	make -C src/questions/infer/update_questions

yfinance: yfinance-workflow yfinance-fetch yfinance-update-questions

yfinance-workflow:
	make -C src/questions/yfinance/workflow

yfinance-fetch:
	make -C src/questions/yfinance/fetch

yfinance-update-questions:
	make -C src/questions/yfinance/update_questions

leaderboard:
	make -C src/leaderboard
