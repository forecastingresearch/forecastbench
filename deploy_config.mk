# Shared build rules for the deployment Makefiles under `src/`.
#
# Every directory deployed to Cloud Run stages a `.gcloudignore` and a
# `Dockerfile` rendered from a template. Import these rules from a deployment
# Makefile with:
#
#   include $(ROOT_DIR)deploy_config.mk

DOCKERFILE_TEMPLATE ?= $(ROOT_DIR)src/helpers/Dockerfile.template
UV ?= uv

.gcloudignore: $(ROOT_DIR)src/helpers/.gcloudignore
	cp $< $@

Dockerfile: $(DOCKERFILE_TEMPLATE) $(ROOT_DIR)Makefile $(ROOT_DIR).python-version
	sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/$(RUNTIME_STACK)/g' \
		-e 's/PYTHON_VERSION/$(PYTHON_RUNTIME)/g' \
		$< > $@

# Export a job's locked dependencies as the flat `requirements.txt` that the Dockerfile installs.
# `DEPENDENCY_GROUP` is the job's dependency group in root `pyproject.toml`; `$(1)` is the upload
# directory.
define export-requirements
	$(if $(DEPENDENCY_GROUP),,$(error DEPENDENCY_GROUP must name a dependency group in pyproject.toml))$(UV) export --project $(ROOT_DIR) --frozen --no-hashes --no-emit-project --quiet --only-group $(DEPENDENCY_GROUP) --output-file $(abspath $(1))/requirements.txt
endef
