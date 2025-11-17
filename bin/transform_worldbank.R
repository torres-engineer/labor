#!/usr/bin/env Rscript
library(readr)
library(dplyr)
library(tidyr)
library(stringr)

raw_dir <- "data/raw/worldbank"
clean_dir <- "data/cleaned/worldbank"
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

# helper: derive friendly output name
indicator_alias <- function(ind_code) {
    case_when(
        str_detect(ind_code, "NY\\.GDP\\.MKTP\\.CD") ~ "gdp",
        str_detect(ind_code, "SI\\.POV\\.GINI") ~ "gini",
        str_detect(ind_code, "BX\\.KLT\\.DINV\\.CD\\.WD") ~ "fdi",
        TRUE ~ str_replace_all(ind_code, "\\.", "_")
    )
}

# read country metadata once (same for all indicators)
meta_country_path <- list.files(
    raw_dir,
    pattern = "^Metadata_Country_API_.*\\.csv$",
    full.names = TRUE
)[1]
meta_country <- read_csv(
    meta_country_path,
    skip = 0,
    show_col_types = FALSE
) %>%
    rename(
        country_code = `Country Code`,
        region = Region,
        income_group = IncomeGroup
    ) %>%
    select(country_code, region, income_group)

# iterate over all API_*.csv files
for (file in list.files(
    raw_dir,
    pattern = "^API_.*\\.csv$",
    full.names = TRUE
)) {
    message("Processing ", basename(file))
    dat_raw <- read_csv(file, skip = 4, show_col_types = FALSE)

    # reshape
    dat_long <- dat_raw %>%
        pivot_longer(
            cols = matches("^[0-9]{4}$"),
            names_to = "year",
            values_to = "value",
            values_drop_na = TRUE
        ) %>%
        rename(
            country = `Country Name`,
            country_code = `Country Code`,
            indicator_name = `Indicator Name`,
            indicator_code = `Indicator Code`
        ) %>%
        left_join(meta_country, by = "country_code") %>%
        select(
            country,
            country_code,
            region,
            income_group,
            indicator_name,
            indicator_code,
            year,
            value
        )

    # derive clean file name
    ind_code <- unique(dat_long$indicator_code)
    short <- indicator_alias(ind_code[1])
    out_path <- file.path(clean_dir, paste0(short, ".csv"))

    write_csv(dat_long, out_path)
    message(" → wrote ", out_path)
}
