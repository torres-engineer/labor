# Selection

import duckdb

with open('../../sql/query.sql', 'r') as file:
    query = file.read().strip()

with duckdb.connect(database = "../../data/data.duckdb", read_only = True) as con:
    df = con.sql(query).df()

# Pre-processing

import pandas as pd
import numpy as np

def interpolate_country_series(df, value_col, max_gap=2):
    df = df.sort_values("year").copy()

    is_na = df[value_col].isna()
    group = (is_na != is_na.shift()).cumsum()
    gap_sizes = is_na.groupby(group).transform("sum")

    interpolated = df[value_col].interpolate(
        method="linear",
        limit=max_gap,
        limit_area="inside"
    )

    df[f"{value_col}_interp"] = interpolated.where(
        ~(is_na & (gap_sizes > max_gap)),
        pd.NA
    )

    return df

cols = [
    "labor_share",
    "productivity",
    "gini",
    "fdi_net_gdp",
    "avg_hourly_wage"
]

df = (
    df
    .groupby("country", group_keys=False)
    .apply(
        lambda x: x.assign(**{
            f"{col}_interp": interpolate_country_series(x, col, max_gap=2)[f"{col}_interp"]
            for col in cols
        }),
        include_groups=False
    )
    .merge(df[['country']], left_index=True, right_index=True, how='left')
)

for col in cols:
    df[f"{col}_missing"] = df[col].isna()
    df[f"{col}_was_interp"] = df[col].isna() & df[f"{col}_interp"].notna()

import matplotlib.pyplot as plt

def plot_raw_vs_interp(df, col, bins=40):
    raw = df[col].dropna()
    interp_only = df.loc[df[f"{col}_was_interp"], f"{col}_interp"]

    plt.figure()

    if len(raw) > 0:
        plt.hist(raw, bins=bins, density=True, alpha=0.5, label="raw")

    if len(interp_only) > 0:
        plt.hist(interp_only, bins=bins, density=True, alpha=0.5, label="interpolated only")

    if len(raw) == 0 and len(interp_only) == 0:
        plt.close()  # nothing to show for this column
        return

    plt.legend()
    plt.title(col)
    plt.show()

def plot_country_series(df, country, col):
    sub = df[df["country"] == country].sort_values("year")

    plt.figure()
    plt.plot(sub["year"], sub[col], "o-", label="raw")
    plt.plot(sub["year"], sub[f"{col}_interp"], "x--", label="interp")
    plt.title(f"{country} – {col}")
    plt.legend()
    plt.show()

for col in cols:
    #plot_raw_vs_interp(df, col)
    #plot_country_series(df, "Ukraine", col)
    pass

interp_rates = (
    df
    .groupby("country")
    .agg({
        f"{col}_was_interp": "mean" for col in cols
    })
    .reset_index()
)

for col in cols:
    # rates = interp_rates[interp_rates[f"{col}_was_interp"].notna() & (interp_rates[f"{col}_was_interp"] > 0)].sort_values(f"{col}_was_interp")

    # if len(rates) == 0:
    #     continue

    # plt.figure(figsize=(6, 8))
    # plt.barh(rates["country"], rates[f"{col}_was_interp"])
    # plt.title(f"Share of interpolated observations – {col}")
    # plt.xlabel("Proportion interpolated")
    # plt.show()
    pass

# Transform

for col in cols:
    df[col + "_final"] = df[col + "_interp"].combine_first(df[col])

from sklearn.preprocessing import StandardScaler

features = [f"{col}_final" for col in cols]

scaler = StandardScaler()

df_scaled = df.copy()
df_scaled[features] = scaler.fit_transform(df_scaled[features])

lag_vars = ["labor_share_final", "productivity_final", "fdi_net_gdp_final"]

for var in lag_vars:
    df[f"{var}_lag1"] = (
        df
        .groupby("country")[var]
        .shift(1)
    )

df["labor_share_q"] = pd.qcut(
    df["labor_share_final"],
    q=4,
    labels=["low", "mid-low", "mid-high", "high"]
)

df_final = df.sort_values(["country", "year"]).reset_index(drop=True)

dir = "../../data/selection"

import os

os.makedirs(dir, exist_ok=True)

df_final.to_parquet(f"{dir}/country_year_transformed.parquet", index=False)
df_final.to_csv(f"{dir}/country_year_transformed.csv", index=False)
