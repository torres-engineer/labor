DATA_DIR = data
RAW_DIR = $(DATA_DIR)/raw
CLEAN_DIR = $(DATA_DIR)/cleaned
INTEGRATED_DIR = $(DATA_DIR)/integrated

EXTRACT_SCRIPTS := $(wildcard bin/extract_*)
EXTRACT_NAMES := $(patsubst extract_%,%,$(basename $(notdir $(EXTRACT_SCRIPTS))))
EXTRACT_FLAGS := $(patsubst %, $(RAW_DIR)/%/.extracted, $(EXTRACT_NAMES))

extract: $(EXTRACT_FLAGS)

$(RAW_DIR)/%/.extracted:
	@echo "Extracting $*"
	./bin/extract_$*.*
	@mkdir -p $(dir $@)
	@touch $@

TRANSFORM_SCRIPTS := $(wildcard bin/transform_*)
TRANSFORM_NAMES := $(patsubst transform_%,%,$(basename $(notdir $(TRANSFORM_SCRIPTS))))
TRANSFORM_FLAGS := $(patsubst %, $(CLEAN_DIR)/%/.transformed, $(TRANSFORM_NAMES))

transform: $(TRANSFORM_FLAGS)

$(CLEAN_DIR)/%/.transformed: $(RAW_DIR)/%/.extracted
	@echo "Transforming $*"
	./bin/transform_$*.*
	@mkdir -p $(dir $@)
	@touch $@

INTEGRATE_OUTPUT = $(INTEGRATED_DIR)/global_labor_inequality.csv

integrate: $(INTEGRATE_OUTPUT)

$(INTEGRATE_OUTPUT): $(TRANSFORM_FLAGS) bin/integrate.R
	@echo "Integrating data..."
	./bin/integrate.R $(CLEAN_DIR) $(INTEGRATE_OUTPUT)

LOAD_OUTPUT = $(DATA_DIR)/data.duckdb

load: $(LOAD_OUTPUT)

$(LOAD_OUTPUT): $(INTEGRATE_OUTPUT) bin/load.sh
	@echo "Loading integrated data..."
	./bin/load.sh $(INTEGRATE_OUTPUT) $(LOAD_OUTPUT)

ARCHIVE_DIR = archives
ARCHIVE_NAME = data_backup.tar.xz
ARCHIVE_FILE = $(ARCHIVE_DIR)/$(ARCHIVE_NAME)

COMPRESSOR ?= xz

ifeq ($(COMPRESSOR),xz)
	TAR_FLAGS = -caf
	EXT = xz
else ifeq ($(COMPRESSOR),zstd)
	TAR_FLAGS = --zstd -cf
	EXT = zst
else ifeq ($(COMPRESSOR),gzip)
	TAR_FLAGS = -czf
	EXT = gz
else
	TAR_FLAGS = -cf
	EXT =
endif

ARCHIVE_FILE = $(ARCHIVE_DIR)/data_backup.tar.$(EXT)

ARCHIVE_DEPENDS := $(shell find $(DATA_DIR) -type f)

archive: $(ARCHIVE_FILE)

$(ARCHIVE_FILE): $(ARCHIVE_DEPENDS) | $(ARCHIVE_DIR)
	@echo "Creating archive $@ with compressor $(COMPRESSOR)"
	tar $(TAR_FLAGS) $@ -C $(DATA_DIR) .
	@echo "Archive created at $@"

$(ARCHIVE_DIR):
	mkdir -p $(ARCHIVE_DIR)

.PHONY: all extract transform integrate load clean archive

all: extract transform archive integrate load

clean:
	rm -rf $(RAW_DIR) $(CLEAN_DIR) $(INTEGRATED_DIR) $(ARCHIVE_DIR)

