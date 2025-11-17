#!/usr/bin/env Rscript
library(Rilostat)
library(readr)

raw_dir <- "data/raw/ilostat"
clean_dir <- "data/cleaned/ilostat"
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

# SDG indicator 10.4.1 - Labour income share as a percent of GDP (%)
out_path <- file.path(raw_dir, paste0("labour_share", ".rds"))
writeLines(as.character(Sys.time()), out_path)
dat <- get_ilostat(
    id = "SDG_1041_NOC_RT_A",
    segment = "indicator",
    quiet = TRUE
)
write_rds(dat, out_path)
# Employment by sex, economic activity and public/private sector
# Informal employment by sex, economic activity and public/private sector
out_path <- file.path(raw_dir, paste0("employment_by_sector", ".rds"))
writeLines(as.character(Sys.time()), out_path)
dat <- get_ilostat(
    id = c("EMP_TEMP_SEX_ECO_INS_NB_A", "EMP_NIFL_SEX_ECO_INS_NB_A"),
    segment = "indicator",
    quiet = TRUE
)
write_rds(dat, out_path)
# Average hourly earnings of employees by sex and economic activity
out_path <- file.path(raw_dir, paste0("hourly_earnings", ".rds"))
writeLines(as.character(Sys.time()), out_path)
dat <- get_ilostat(
    id = "EAR_4HRL_SEX_ECO_CUR_NB_A",
    segment = "indicator",
    quiet = TRUE
)
write_rds(dat, out_path)
