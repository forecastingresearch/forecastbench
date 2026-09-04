# Shared build rules for the deployment Makefiles under `src/`.
#
# Every directory deployed to Cloud Run stages a `.gcloudignore` and a
# `Dockerfile` rendered from a template. Import these rules from a deployment
# Makefile with:
#
#   include $(ROOT_DIR)deploy_config.mk

DOCKERFILE_TEMPLATE ?= $(ROOT_DIR)src/helpers/Dockerfile.template

.gcloudignore: $(ROOT_DIR)src/helpers/.gcloudignore
	cp $< $@

Dockerfile: $(DOCKERFILE_TEMPLATE) $(ROOT_DIR)Makefile $(ROOT_DIR).python-version
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/$(RUNTIME_STACK)/g' \
		-e 's/PYTHON_VERSION/$(PYTHON_RUNTIME)/g' \
		$< > $@
