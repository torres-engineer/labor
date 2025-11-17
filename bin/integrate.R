#!/usr/bin/env Rscript
library(readr)
library(dplyr)
library(tidyr)

wb_dir <- "data/cleaned/worldbank"
ilo_dir <- "data/cleaned/ilostat"
out_dir <- "data/integrated"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# read World Bank indicators
gdp <- read_csv(file.path(wb_dir, "gdp.csv"), show_col_types = FALSE) %>%
    select(country_code, year, gdp = value) %>%
    mutate(year = as.character(year))
gini <- read_csv(file.path(wb_dir, "gini.csv"), show_col_types = FALSE) %>%
    select(country_code, year, gini = value) %>%
    mutate(year = as.character(year))
fdi <- read_csv(file.path(wb_dir, "fdi.csv"), show_col_types = FALSE) %>%
    select(country_code, year, fdi = value) %>%
    mutate(year = as.character(year))

# read ILOSTAT indicators
labour <- read_rds(file.path(ilo_dir, "labour_share.rds")) %>%
    select(country_code, year, labour_share = value)
earnings <- read_rds(file.path(ilo_dir, "hourly_earnings.rds")) %>%
    pivot_wider(
        names_from = classif2,
        values_from = value,
        names_prefix = "hourly_earnings_"
    ) %>%
    select(
        country_code,
        year,
        # hourly_earnings_lcu = hourly_earnings_CUR_TYPE_LCU,
        hourly_earnings_ppp = hourly_earnings_CUR_TYPE_PPP,
        # hourly_earnings_usd = hourly_earnings_CUR_TYPE_USD
    )

# combine all
merged <- gdp %>%
    full_join(gini, by = c("country_code", "year")) %>%
    full_join(fdi, by = c("country_code", "year")) %>%
    full_join(labour, by = c("country_code", "year")) %>%
    full_join(earnings, by = c("country_code", "year"))

merged <- merged %>%
    mutate(
        year = as.integer(year),
        across(c(gdp, gini, fdi, labour_share, hourly_earnings_ppp), as.numeric)
    )


write_csv(merged, file.path(out_dir, "global_labor_inequality.csv"))
write_rds(merged, file.path(out_dir, "global_labor_inequality.rds"))
