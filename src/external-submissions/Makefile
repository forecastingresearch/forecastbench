-include ../../variables.mk

.PHONY: deploy-onboard setup-permissions clean

REGION          := us-central1
SA_EMAIL        ?= $(SUBMISSIONS_SA_EMAIL)
ROOT_DIR        ?= ../../

FUNC_DIR = functions

ONBOARD_ENV_VARS = \
	RUN_MODE=$(RUN_MODE),\
	SMTP_USER=$(SMTP_USER),\
	SMTP_PASSWORD=$(SMTP_PASSWORD),\
	UPLOAD_BUCKET=$(SUBMISSIONS_BUCKET),\
	NEXT_DUE_DATE=$(NEXT_DUE_DATE)

deploy-onboard:
	cp $(ROOT_DIR)src/helpers/email.py $(FUNC_DIR)/onboard/email_utils.py
	gcloud functions deploy onboard-team \
		--gen2 \
		--project=$(CLOUD_PROJECT) \
		--region=$(REGION) \
		--runtime=python312 \
		--source=$(FUNC_DIR)/onboard \
		--entry-point=onboard \
		--trigger-http \
		--no-allow-unauthenticated \
		--service-account=$(SA_EMAIL) \
		--memory=256Mi \
		--timeout=120s \
		--set-env-vars=$(ONBOARD_ENV_VARS)
	rm -f $(FUNC_DIR)/onboard/email_utils.py

setup-permissions:
	eval $$(cat ../../variables.mk | grep -v '^#' | xargs) \
		python $(FUNC_DIR)/setup_permissions.py

clean:
	rm -f $(FUNC_DIR)/onboard/email_utils.py
