#!/usr/bin/env Rscript
library(Rilostat)
library(readr)
library(dplyr)

fetch_and_store_ilostat <- function(
  id,
  out_name,
  raw_dir,
  segment = "indicator",
  quiet = TRUE,
  label_code = "all"
) {
  out_path <- file.path(raw_dir, out_name)

  dat <- get_ilostat(id = id, segment = segment, quiet = quiet) %>%
    label_ilostat(code = label_code)

  if (nrow(dat) == 0) {
    stop("No data returned for ", out_name)
  }

  write_rds(dat, paste0(out_path, ".rds"))
  write_csv(dat, paste0(out_path, ".csv"))

  invisible(dat)
}

datasets <- list(
  labour_share = list(
    id = "SDG_1041_NOC_RT_A",
    description = paste(
      "SDG indicator 10.4.1",
      "Labour income share as a percent of GDP (%)",
      sep = " — "
    )
  ),
  employment_by_sector = list(
    id = c("EMP_TEMP_SEX_ECO_INS_NB_M", "EMP_NIFL_SEX_ECO_INS_NB_M"),
    description = paste0(
      "Formal and informal employment",
      "by sex, economic activity and public/private sector"
    )
  ),
  hourly_earnings = list(
    id = "EAR_4HRL_SEX_ECO_CUR_NB_A",
    description = paste0(
      "Average hourly earnings of employees",
      "by sex and economic activity"
    )
  )
)

raw_dir <- "data/raw/ilostat"
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

for (name in names(datasets)) {
  message("Fetching: ", name)
  fetch_and_store_ilostat(
    id = datasets[[name]]$id,
    out_name = name,
    raw_dir = raw_dir
  )
}
