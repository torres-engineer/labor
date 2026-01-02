#!/usr/bin/env Rscript

library(tidyverse)
library(lubridate)
library(readr)
library(janitor)

raw_dir <- "data/raw/ilostat"
clean_dir <- "data/cleaned/ilostat"
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

employment <- read_rds(file.path(raw_dir, "employment_by_sector.rds")) %>%
  clean_names()
wages <- read_rds(file.path(raw_dir, "hourly_earnings.rds")) %>%
  clean_names()
macro <- read_rds(file.path(raw_dir, "labour_share.rds")) %>%
  clean_names()

dim_geo <- bind_rows(
  employment %>% select(ref_area, ref_area_label),
  wages %>% select(ref_area, ref_area_label),
  macro %>% select(ref_area, ref_area_label)
) %>%
  distinct() %>%
  mutate(
    code_ilo = ref_area,
    name = ref_area_label,
    geo_type = case_when(
      str_starts(ref_area, "X") ~ "aggregate",
      TRUE ~ "country"
    )
  ) %>%
  select(code_ilo, name, geo_type)

write_rds(dim_geo, file.path(clean_dir, "dim_geo.rds"))
write_csv(dim_geo, file.path(clean_dir, "dim_geo.csv"))

parse_time <- function(x) {
  tibble(
    time_code = x,
    year = as.integer(str_sub(x, 1, 4)),
    month = if_else(
      str_detect(x, "M"),
      as.integer(str_sub(x, 6, 7)),
      NA_integer_
    ),
    is_monthly = str_detect(x, "M"),
    is_yearly = !str_detect(x, "M")
  )
}

dim_date <- bind_rows(
  employment %>% select(time),
  wages %>% select(time),
  macro %>% select(time)
) %>%
  distinct() %>%
  bind_cols(parse_time(.$time)) %>%
  select(time_code, year, month, is_yearly, is_monthly)

write_rds(dim_date, file.path(clean_dir, "dim_date.rds"))
write_csv(dim_date, file.path(clean_dir, "dim_date.csv"))

dim_source <- bind_rows(
  employment %>% select(source, source_label),
  wages %>% select(source, source_label),
  macro %>% select(source, source_label)
) %>%
  distinct() %>%
  mutate(id = row_number()) %>%
  select(id, code = source, name = source_label)

write_rds(dim_source, file.path(clean_dir, "dim_source.rds"))
write_csv(dim_source, file.path(clean_dir, "dim_source.csv"))

dim_indicator <- bind_rows(
  employment %>% select(indicator, indicator_label, source),
  wages %>% select(indicator, indicator_label, source),
  macro %>% select(indicator, indicator_label, source)
) %>%
  distinct() %>%
  left_join(
    dim_source %>% mutate(source_label = name),
    by = c("source" = "code")
  ) %>%
  mutate(
    domain = case_when(
      str_detect(indicator, "SDG") ~ "labour share",
      str_detect(indicator, "EAR") ~ "wages",
      str_detect(indicator, "EMP") ~ "employment",
    ),
    unit = case_when(
      str_detect(indicator, "SDG") ~ "percentage of GDP (%)",
      str_detect(indicator, "EAR") ~ "LCU",
      str_detect(indicator, "EMP") ~ "thousands",
    ),
    measure_type = case_when(
      str_detect(indicator, "SDG") ~ "non-additive",
      str_detect(indicator, "EAR") ~ "semi-additive",
      str_detect(indicator, "EMP") ~ "additive",
    ),
    source_id = id,
    source_code = source,
    source_name = source_label
  ) %>%
  mutate(id = row_number()) %>%
  select(
    id,
    code = indicator,
    name = indicator_label,
    domain,
    unit,
    measure_type,
    source_code,
    source_name
  )

write_rds(dim_indicator, file.path(clean_dir, "dim_indicator.rds"))
write_csv(dim_indicator, file.path(clean_dir, "dim_indicator.csv"))

dim_sex <- employment %>%
  select(sex, sex_label) %>%
  bind_rows(
    wages %>% select(sex, sex_label)
  ) %>%
  distinct() %>%
  select(code = sex, name = sex_label)

write_rds(dim_sex, file.path(clean_dir, "dim_sex.rds"))
write_csv(dim_sex, file.path(clean_dir, "dim_sex.csv"))

dim_econ_activity <- employment %>%
  select(classif1, classif1_label) %>%
  bind_rows(
    wages %>% select(classif1, classif1_label)
  ) %>%
  distinct() %>%
  mutate(
    scheme = str_extract(classif1_label, "(?<=\\().*?(?=\\):)"),
    name = str_extract(classif1_label, "(?<=: ).*$")
  ) %>%
  select(code = classif1, scheme, name)

write_rds(dim_econ_activity, file.path(clean_dir, "dim_econ_activity.rds"))
write_csv(dim_econ_activity, file.path(clean_dir, "dim_econ_activity.csv"))

dim_sector <- employment %>%
  select(classif2, classif2_label) %>%
  distinct() %>%
  select(code = classif2, name = classif2_label)

write_rds(dim_sector, file.path(clean_dir, "dim_sector.rds"))
write_csv(dim_sector, file.path(clean_dir, "dim_sector.csv"))

dim_currency <- wages %>%
  select(classif2, classif2_label) %>%
  distinct() %>%
  select(code = classif2, label = classif2_label)

write_rds(dim_currency, file.path(clean_dir, "dim_currency.rds"))
write_csv(dim_currency, file.path(clean_dir, "dim_currency.csv"))

# write_rds(dim_obs_status, file.path(clean_dir, "dim_obs_status.rds"))
# write_csv(dim_obs_status, file.path(clean_dir, "dim_obs_status.csv"))

fact_macroecon <- macro %>%
  transmute(
    geo = ref_area,
    date = time,
    indicator = paste(indicator, source, sep = " | "),
    value = obs_value
  )

write_rds(fact_macroecon, file.path(clean_dir, "fact_macroecon.rds"))
write_csv(fact_macroecon, file.path(clean_dir, "fact_macroecon.csv"))

fact_wages <- wages %>%
  transmute(
    geo = ref_area,
    date = time,
    indicator = paste(indicator, source, sep = " | "),
    sex,
    econ_activity = classif1,
    currency = classif2,
    value = obs_value
  )

write_rds(fact_wages, file.path(clean_dir, "fact_wages.rds"))
write_csv(fact_wages, file.path(clean_dir, "fact_wages.csv"))

fact_employment <- employment %>%
  transmute(
    geo = ref_area,
    date = time,
    indicator = paste(indicator, source, sep = " | "),
    sex,
    econ_activity = classif1,
    sector = classif2,
    value = obs_value
  )

write_rds(fact_employment, file.path(clean_dir, "fact_employment.rds"))
write_csv(fact_employment, file.path(clean_dir, "fact_employment.csv"))
