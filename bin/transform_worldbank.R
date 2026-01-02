#!/usr/bin/env Rscript

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(WikidataQueryServiceR)

raw_dir <- "data/raw/worldbank"
clean_dir <- "data/cleaned/worldbank"
dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

country_meta_file <- list.files(
  raw_dir,
  pattern = "^Metadata_Country_.*csv$",
  full.names = TRUE
)[1]

# a row is `geo_type` "region" only if there's at least one row which has its `Region` as the start of the region's `TableName`.
#
# example:
# "LAC","","","","Latin America & Caribbean (excluding high income)",
# "LCN","","","","Latin America & Caribbean",
# "MEX","Latin America & Caribbean","Upper middle income","","Mexico",
#
# the first two rows are regions because the third row is the start of the regions' `TableName`
#
# on the other cases where a row hasn't a `Region`, that row `geo_type` is "aggregate"
#
# example:
# "MIC","","","Middle income group aggregate. Middle-income economies are those in which 2024 Atlas GNI per capita was between $1,136 and $13,935.","Middle income",
# "WLD","","","","World",
#
# the rest of the rows countries

dim_geo_raw <- read_csv(country_meta_file, show_col_types = FALSE) %>%
  rename(
    code_wb = `Country Code`,
    region = Region,
    income_group = IncomeGroup,
    special_notes = SpecialNotes,
    name = TableName
  ) %>%
  mutate(region = na_if(region, ""))

all_regions <- unique(na.omit(dim_geo_raw$name[
  dim_geo_raw$region %in% dim_geo_raw$name
]))

dim_geo <- dim_geo_raw %>%
  mutate(
    geo_type = case_when(
      name %in% all_regions ~ "region",
      is.na(region) & !(name %in% all_regions) ~ "aggregate",
      TRUE ~ "country"
    ),
    region = if_else(is.na(region), "", region)
  ) %>%
  select(
    code_wb,
    name,
    geo_type,
    region,
    income_group,
    special_notes
  )

write_rds(dim_geo, file.path(clean_dir, "dim_geo.rds"))
write_csv(dim_geo, file.path(clean_dir, "dim_geo.csv"))

indicator_files <- list.files(
  raw_dir,
  pattern = "^Metadata_Indicator_.*csv$",
  full.names = TRUE
)

dim_indicators_raw <- map_dfr(
  indicator_files,
  ~ {
    read_csv(.x, show_col_types = FALSE)
  }
)

dim_source <- dim_indicators_raw %>%
  rename(name = SOURCE_ORGANIZATION) %>%
  distinct(name, .keep_all = TRUE) %>%
  mutate(id = row_number()) %>%
  select(id, name)

write_rds(dim_source, file.path(clean_dir, "dim_source.rds"))
write_csv(dim_source, file.path(clean_dir, "dim_source.csv"))

indicator_domains <- tibble(
  code = c(
    "NY.GDP.MKTP.PP.KD",
    "NY.GDP.MKTP.KD",
    "NY.GDP.MKTP.KN",
    "BM.KLT.DINV.CD.WD",
    "BX.KLT.DINV.CD.WD",
    "SI.POV.GINI"
  ),
  domain = c("GDP", "GDP", "GDP", "FDI", "FDI", "Gini"),
  unit = c(
    "constant 2021 international $",
    "constant 2015 US$",
    "constant LCU",
    "BoP, current US$",
    "BoP, current US$",
    "index"
  ),
  measure_type = c(
    "additive",
    "additive",
    "additive",
    "additive",
    "additive",
    "non-additive"
  )
)

dim_indicator <- dim_indicators_raw %>%
  rename(
    code = INDICATOR_CODE,
    name = INDICATOR_NAME,
    source_name = SOURCE_ORGANIZATION,
  ) %>%
  left_join(indicator_domains, by = "code") %>%
  left_join(
    dim_source %>% select(name, id),
    by = c("source_name" = "name")
  ) %>%
  rename(source = id) %>%
  select(code, name, domain, unit, measure_type, source)

write_rds(dim_indicator, file.path(clean_dir, "dim_indicator.rds"))
write_csv(dim_indicator, file.path(clean_dir, "dim_indicator.csv"))

years <- 1960:2024

dim_date <- tibble(
  year = years,
  is_yearly = TRUE,
  is_monthly = FALSE
)

write_rds(dim_date, file.path(clean_dir, "dim_date.rds"))
write_csv(dim_date, file.path(clean_dir, "dim_date.csv"))

data_files <- list.files(raw_dir, pattern = "^API_.*csv$", full.names = TRUE)

fact_macroecon <- map_dfr(
  data_files,
  ~ {
    read_csv(.x, skip = 4, show_col_types = FALSE) %>%
      pivot_longer(
        cols = matches("^[0-9]{4}$"),
        names_to = "year",
        values_to = "value"
      ) %>%
      transmute(
        geo = `Country Code`,
        indicator = `Indicator Code`,
        date = as.integer(year),
        value = as.numeric(value),
      )
  }
)

write_rds(fact_macroecon, file.path(clean_dir, "fact_macroecon.rds"))
write_csv(fact_macroecon, file.path(clean_dir, "fact_macroecon.csv"))

wikidata_query <- '
SELECT ?country ?countryLabel ?iso2 ?iso3 WHERE {
  ?country wdt:P297 ?iso2 .
  OPTIONAL { ?country wdt:P298 ?iso3 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,mul". }
}
'

geo_wikidata_map <- query_wikidata(wikidata_query)

write_rds(geo_wikidata_map, file.path(clean_dir, "geo_wikidata_map.rds"))
write_csv(geo_wikidata_map, file.path(clean_dir, "geo_wikidata_map.csv"))
