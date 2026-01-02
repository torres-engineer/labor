DATA_DIR = data
RAW_DIR = $(DATA_DIR)/raw
CLEAN_DIR = $(DATA_DIR)/cleaned
BIN_DIR = bin

EXTRACT_SCRIPTS := $(wildcard $(BIN_DIR)/extract_*)
EXTRACT_NAMES := $(patsubst extract_%,%,$(basename $(notdir $(EXTRACT_SCRIPTS))))
EXTRACT_FLAGS := $(patsubst %,$(RAW_DIR)/%/.extracted,$(EXTRACT_NAMES))

extract: $(EXTRACT_FLAGS)

$(RAW_DIR)/%/.extracted:
	@echo "Extracting $*"
	@mkdir -p $(dir $@)
	$(BIN_DIR)/extract_$*.*
	@touch $@

TRANSFORM_SCRIPTS := $(wildcard $(BIN_DIR)/transform_*)
TRANSFORM_NAMES := $(patsubst transform_%,%,$(basename $(notdir $(TRANSFORM_SCRIPTS))))
TRANSFORM_FLAGS := $(patsubst %,$(CLEAN_DIR)/%/.transformed,$(TRANSFORM_NAMES))

transform: $(TRANSFORM_FLAGS)

$(CLEAN_DIR)/%/.transformed: $(RAW_DIR)/%/.extracted
	@echo "Transforming $*"
	@mkdir -p $(dir $@)
	$(BIN_DIR)/transform_$*.*
	@touch $@

LOAD_OUTPUT = $(DATA_DIR)/data.duckdb

load: $(LOAD_OUTPUT)

$(LOAD_OUTPUT): transform $(BIN_DIR)/load.sh
	@echo "Loading..."
	$(BIN_DIR)/load.sh $(INTEGRATE_OUTPUT) $(LOAD_OUTPUT)

ARCHIVE_DIR = archives
COMPRESSOR ?= xz

TAR_FLAGS_xz   = -caf
TAR_FLAGS_zstd = --zstd -cf
TAR_FLAGS_gzip = -czf
EXT_xz   = xz
EXT_zstd = zst
EXT_gzip = gz

TAR_FLAGS = $(TAR_FLAGS_$(COMPRESSOR))
EXT       = $(EXT_$(COMPRESSOR))
ARCHIVE_FILE = $(ARCHIVE_DIR)/data_backup.$(EXT)

$(ARCHIVE_DIR)/.data_ready: load
	@touch $@

archive: $(ARCHIVE_FILE)

$(ARCHIVE_FILE): $(ARCHIVE_DIR)/.data_ready | $(ARCHIVE_DIR)
	@echo "Creating archive $@"
	tar $(TAR_FLAGS) $@ -C $(DATA_DIR) .

$(ARCHIVE_DIR):
	mkdir -p $@

all: load archive

clean:
	rm -rf $(RAW_DIR) $(CLEAN_DIR) $(INTEGRATED_DIR)

distclean: clean
	rm -f $(ARCHIVE_FILE)

.PHONY: all extract transform load archive clean distclean
