ORCHESTRATION_EXTRA_PACKAGES ?=

define stage-orchestration-upload
	rm -rf $(UPLOAD_DIR)
	mkdir -p $(UPLOAD_DIR)/orchestration
	cp -r $(ROOT_DIR)src/helpers $(UPLOAD_DIR)/
	cp -r $(ROOT_DIR)src/sources $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_fb_types.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/_schemas.py $(UPLOAD_DIR)/
	cp $(ROOT_DIR)src/orchestration/__init__.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_io.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_llm_forecaster_io.py $(UPLOAD_DIR)/orchestration/
	cp $(ROOT_DIR)src/orchestration/_source_io.py $(UPLOAD_DIR)/orchestration/
$(foreach package,$(ORCHESTRATION_EXTRA_PACKAGES),	cp -r $(ROOT_DIR)src/$(package) $(UPLOAD_DIR)/$(package)
)
	cp $^ $(UPLOAD_DIR)/
	cat $(ROOT_DIR)requirements.runtime.txt requirements.txt > $(UPLOAD_DIR)/requirements.txt
endef
