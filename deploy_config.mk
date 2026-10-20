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

# `DEPENDENCY_GROUP` is the job's dependency group in root `pyproject.toml`; the rendered
# Dockerfile installs only that group from `uv.lock`.
Dockerfile: $(DOCKERFILE_TEMPLATE) Makefile $(ROOT_DIR)Makefile $(ROOT_DIR).python-version
	$(if $(DEPENDENCY_GROUP),,$(error DEPENDENCY_GROUP must name a dependency group in pyproject.toml))sed \
		-e 's/REGION/$(CLOUD_DEPLOY_REGION)/g' \
		-e 's/STACK/$(RUNTIME_STACK)/g' \
		-e 's/PYTHON_VERSION/$(PYTHON_RUNTIME)/g' \
		-e 's/UV_VERSION/$(UV_VERSION)/g' \
		-e 's/DEPENDENCY_GROUP/$(DEPENDENCY_GROUP)/g' \
		$< > $@

# Stage the project manifest and lockfile next to the job's code so the Dockerfile can run
# `uv sync` against them. `$(1)` is the upload directory.
define stage-lockfile
	cp $(ROOT_DIR)pyproject.toml $(ROOT_DIR)uv.lock $(1)/
endef
