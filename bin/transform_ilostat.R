#!/usr/bin/env Rscript
library(readr)
library(dplyr)
library(stringr)
library(tidyr)

raw_dir <- "data/raw/ilostat"
clean_dir <- "data/cleaned/ilostat"
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

clean_ilostat <- function(file) {
    dat <- read_rds(file)

    dat <- dat %>%
        rename_with(tolower) %>%
        rename(
            country_code = ref_area,
            indicator_code = indicator,
            year = time,
            value = obs_value
        ) %>%
        select(country_code, indicator_code, year, value, everything())

    dat
}

for (file in list.files(raw_dir, pattern = "\\.rds$", full.names = TRUE)) {
    message("Processing ", basename(file))
    dat_clean <- clean_ilostat(file)

    # optionally pivot or summarize if multiple indicators per file
    # e.g. in employment_by_sector, keep only total
    if (any(grepl("employment_by_sector", file))) {
        dat_clean <- dat_clean %>%
            filter(classif1 == "ECO_SECTOR_TOTAL" | is.na(classif1))
    }

    name <- str_replace(basename(file), "\\.rds$", "")
    write_rds(dat_clean, file.path(clean_dir, paste0(name, ".rds")))
    write_csv(dat_clean, file.path(clean_dir, paste0(name, ".csv")))
}
