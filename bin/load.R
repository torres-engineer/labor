#!/usr/bin/env Rscript

library(DBI)
library(duckdb)
library(conflicted)
library(duckplyr)
library(readr)
library(WikidataQueryServiceR)
library(stringr)
library(tidyr)

clean_dir <- "data/cleaned"
db_path <- "data/data.duckdb"

con <- dbConnect(duckdb(), dbdir = db_path)

df_date <- list.files(
  clean_dir,
  pattern = "dim_date.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  mutate(
    id = row_number(),
    year = as.integer(year),
    month = as.integer(month %||% NA_integer_),
    quarter = NA_integer_,
    is_yearly = as.logical(is_yearly),
    is_monthly = as.logical(is_monthly)
  ) %>%
  select(id, year, quarter, month, is_yearly, is_monthly)

dbWriteTable(con, "dim_date", df_date, overwrite = TRUE)

wikidata_query <- '
SELECT ?country ?countryLabel ?iso2 ?iso3 WHERE {
  ?country wdt:P297 ?iso2 .
  OPTIONAL { ?country wdt:P298 ?iso3 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,mul". }
}
'

wikidata_geo <- query_wikidata(wikidata_query) %>%
  transmute(
    wikidata_qid = str_extract(country, "Q\\d+"),
    name = countryLabel,
    code_iso2 = iso2,
    code_iso3 = iso3
  ) %>%
  distinct(code_iso3, .keep_all = TRUE)

df_geo <- list.files(
  clean_dir,
  pattern = "dim_geo.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  mutate(
    code_iso2 = NA_character_,
    code_iso3 = NA_character_,
    wikidata_qid = NA_character_,
  ) %>%
  left_join(
    wikidata_geo %>%
      transmute(
        iso = code_iso3,
        wd_code_iso2 = code_iso2,
        wd_code_iso3 = code_iso3,
        wd_qid = wikidata_qid,
        wd_name = name
      ),
    by = c("code_wb" = "iso"),
  ) %>%
  mutate(
    name = if_else(
      !is.na(code_wb),
      coalesce(wd_name, name),
      name
    ),
    code_iso2 = if_else(
      !is.na(code_wb),
      coalesce(wd_code_iso2, code_iso2),
      code_iso2
    ),
    code_iso3 = if_else(
      !is.na(code_wb),
      coalesce(wd_code_iso3, code_iso3),
      code_iso3
    ),
    wikidata_qid = if_else(
      !is.na(code_wb),
      coalesce(wd_qid, wikidata_qid),
      wikidata_qid
    )
  ) %>%
  select(-starts_with("wd_")) %>%
  left_join(
    wikidata_geo %>%
      transmute(
        iso = code_iso3,
        wd_code_iso2 = code_iso2,
        wd_code_iso3 = code_iso3,
        wd_qid = wikidata_qid,
        wd_name = name
      ),
    by = c("code_ilo" = "iso"),
  ) %>%
  mutate(
    name = if_else(
      !is.na(code_ilo),
      coalesce(wd_name, name),
      name
    ),
    code_iso2 = if_else(
      !is.na(code_ilo),
      coalesce(wd_code_iso2, code_iso2),
      code_iso2
    ),
    code_iso3 = if_else(
      !is.na(code_ilo),
      coalesce(wd_code_iso3, code_iso3),
      code_iso3
    ),
    wikidata_qid = if_else(
      !is.na(code_ilo),
      coalesce(wd_qid, wikidata_qid),
      wikidata_qid
    )
  ) %>%
  select(-starts_with("wd_"))

has_iso <- df_geo %>%
  dplyr::filter(!is.na(code_iso3)) %>%
  group_by(code_iso3) %>%
  summarise(
    code_iso3 = first(na.omit(code_iso3)),
    code_iso2 = first(na.omit(code_iso2)),
    wikidata_qid = first(na.omit(wikidata_qid)),
    code_wb = first(na.omit(code_wb)),
    code_ilo = first(na.omit(code_ilo)),
    name = first(na.omit(name)),
    geo_type = first(na.omit(geo_type)),
    region = first(na.omit(region)),
    income_group = first(na.omit(income_group)),
    special_notes = first(na.omit(special_notes)),
    .groups = "drop"
  )

no_iso <- df_geo %>%
  dplyr::filter(is.na(code_iso3))

df_geo <- bind_rows(has_iso, no_iso) %>%
  arrange(is.na(code_iso3), code_iso3) %>%
  mutate(id = row_number()) %>%
  select(
    id,
    code_iso2,
    code_iso3,
    wikidata_qid,
    code_wb,
    code_ilo,
    name,
    geo_type,
    region,
    income_group,
    special_notes
  )

dbWriteTable(con, "dim_geo", df_geo, overwrite = TRUE)

df_source <- list.files(
  clean_dir,
  pattern = "dim_source.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  mutate(
    id = row_number(),
    code = as.character(code %||% NA_character_),
    name = as.character(name),
  ) %>%
  select(id, code, name)

dbWriteTable(con, "dim_source", df_source, overwrite = TRUE)

df_indicator <- list.files(
  clean_dir,
  pattern = "dim_indicator.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  left_join(
    df_source %>% select(source = id, code, name),
    by = c("source_code" = "code", "source_name" = "name")
  ) %>%
  transmute(
    id = row_number(),
    code = as.character(code %||% NA_character_),
    name = as.character(name),
    domain = as.character(domain),
    unit = as.character(unit),
    measure_type = as.character(measure_type),
    source,
  )

dbWriteTable(con, "dim_indicator", df_indicator, overwrite = TRUE)

df_obs <- list.files(
  clean_dir,
  pattern = "dim_obs_status.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  dplyr::filter(!is.na(code)) %>%
  transmute(
    id = row_number(),
    code = as.character(code),
    label = as.character(label),
  )

dbWriteTable(con, "dim_obs_status", df_obs, overwrite = TRUE)

df_sex <- list.files(
  clean_dir,
  pattern = "dim_sex.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  transmute(
    id = row_number(),
    code = as.character(code),
    name = as.character(name),
  )

dbWriteTable(con, "dim_sex", df_sex, overwrite = TRUE)

df_econ <- list.files(
  clean_dir,
  pattern = "dim_econ_activity.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  transmute(
    id = row_number(),
    code = as.character(code),
    scheme = as.character(scheme),
    name = as.character(name),
  )

dbWriteTable(con, "dim_econ_activity", df_econ, overwrite = TRUE)

df_sector <- list.files(
  clean_dir,
  pattern = "dim_sector.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  transmute(
    id = row_number(),
    code = as.character(code),
    name = as.character(name),
  )

dbWriteTable(con, "dim_sector", df_sector, overwrite = TRUE)

df_currency <- list.files(
  clean_dir,
  pattern = "dim_currency.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(read_rds) %>%
  bind_rows() %>%
  distinct() %>%
  transmute(
    id = row_number(),
    code = as.character(code),
    label = as.character(label),
  )

dbWriteTable(con, "dim_currency", df_currency, overwrite = TRUE)

df <- list.files(
  clean_dir,
  pattern = "fact_macroecon.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(function(file) {
    dat <- read_rds(file)
    dat$date <- as.integer(dat$date)
    dat
  }) %>%
  bind_rows() %>%
  distinct() %>%
  left_join(
    df_date %>% select(date_id = id, year),
    by = c("date" = "year")
  ) %>%
  mutate(date = date_id) %>%
  separate(
    indicator,
    into = c("indicator_code", "source_code"),
    sep = " \\| ",
    fill = "right",
    extra = "drop"
  ) %>%
  mutate(source_code = coalesce(source_code, NA_character_)) %>%
  left_join(
    df_source %>% select(source_id = id, code),
    by = c("source_code" = "code")
  ) %>%
  left_join(
    df_indicator %>% select(indicator = id, code, source),
    by = c("indicator_code" = "code", "source_id" = "source")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_wb = id, code_wb),
    by = c("geo" = "code_wb")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_ilo = id, code_ilo),
    by = c("geo" = "code_ilo"),
  ) %>%
  mutate(geo = coalesce(geo_id_wb, geo_id_ilo)) %>%
  left_join(
    df_obs %>% select(obs_id = id, code),
    by = c("obs" = "code"),
  ) %>%
  mutate(obs = obs_id) %>%
  select(geo, date, indicator, value, obs)

dbWriteTable(con, "fact_macroecon", df, overwrite = TRUE)

df <- list.files(
  clean_dir,
  pattern = "fact_employment.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(function(file) {
    dat <- read_rds(file)
    dat$date <- as.integer(dat$date)
    dat
  }) %>%
  bind_rows() %>%
  distinct() %>%
  left_join(
    df_date %>% select(date_id = id, year),
    by = c("date" = "year")
  ) %>%
  mutate(date = date_id) %>%
  separate(
    indicator,
    into = c("indicator_code", "source_code"),
    sep = " \\| ",
    fill = "right",
    extra = "drop"
  ) %>%
  mutate(source_code = coalesce(source_code, NA_character_)) %>%
  left_join(
    df_source %>% select(source_id = id, code),
    by = c("source_code" = "code")
  ) %>%
  left_join(
    df_indicator %>% select(indicator = id, code, source),
    by = c("indicator_code" = "code", "source_id" = "source")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_wb = id, code_wb),
    by = c("geo" = "code_wb")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_ilo = id, code_ilo),
    by = c("geo" = "code_ilo"),
  ) %>%
  mutate(geo = coalesce(geo_id_wb, geo_id_ilo)) %>%
  left_join(
    df_obs %>% select(obs_id = id, code),
    by = c("obs" = "code"),
  ) %>%
  mutate(obs = obs_id) %>%
  left_join(
    df_sex %>% select(sex_id = id, code),
    by = c("sex" = "code"),
  ) %>%
  mutate(sex = sex_id) %>%
  left_join(
    df_econ %>% select(econ_id = id, code),
    by = c("econ_activity" = "code"),
  ) %>%
  mutate(econ_activity = econ_id) %>%
  left_join(
    df_sector %>% select(sector_id = id, code),
    by = c("sector" = "code"),
  ) %>%
  mutate(sector = sector_id) %>%
  select(geo, date, indicator, sex, econ_activity, sector, value, obs)

dbWriteTable(con, "fact_employment", df, overwrite = TRUE)

df <- list.files(
  clean_dir,
  pattern = "fact_wages.rds$",
  full.names = TRUE,
  recursive = TRUE,
) %>%
  lapply(function(file) {
    dat <- read_rds(file)
    dat$date <- as.integer(dat$date)
    dat
  }) %>%
  bind_rows() %>%
  distinct() %>%
  left_join(
    df_date %>% select(date_id = id, year),
    by = c("date" = "year")
  ) %>%
  mutate(date = date_id) %>%
  separate(
    indicator,
    into = c("indicator_code", "source_code"),
    sep = " \\| ",
    fill = "right",
    extra = "drop"
  ) %>%
  mutate(source_code = coalesce(source_code, NA_character_)) %>%
  left_join(
    df_source %>% select(source_id = id, code),
    by = c("source_code" = "code")
  ) %>%
  left_join(
    df_indicator %>% select(indicator = id, code, source),
    by = c("indicator_code" = "code", "source_id" = "source")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_wb = id, code_wb),
    by = c("geo" = "code_wb")
  ) %>%
  left_join(
    df_geo %>% select(geo_id_ilo = id, code_ilo),
    by = c("geo" = "code_ilo"),
  ) %>%
  mutate(geo = coalesce(geo_id_wb, geo_id_ilo)) %>%
  left_join(
    df_obs %>% select(obs_id = id, code),
    by = c("obs" = "code"),
  ) %>%
  mutate(obs = obs_id) %>%
  left_join(
    df_sex %>% select(sex_id = id, code),
    by = c("sex" = "code"),
  ) %>%
  mutate(sex = sex_id) %>%
  left_join(
    df_econ %>% select(econ_id = id, code),
    by = c("econ_activity" = "code"),
  ) %>%
  mutate(econ_activity = econ_id) %>%
  left_join(
    df_currency %>% select(currency_id = id, code),
    by = c("currency" = "code"),
  ) %>%
  mutate(currency = currency_id) %>%
  select(geo, date, indicator, sex, econ_activity, currency, value, obs)

dbWriteTable(con, "fact_wages", df, overwrite = TRUE)

dbDisconnect(con, shutdown = TRUE)
