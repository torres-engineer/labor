WITH macro_base AS (
    SELECT
        d.year,
        g.id            AS geo_id,
        g.name          AS country,
        g.region,
        g.income_group,
        i.code          AS indicator_code,
        f.value
    FROM fact_macroecon f
    JOIN dim_date d
        ON f.date = d.id
       AND d.is_yearly = TRUE
    JOIN dim_geo g
        ON f.geo = g.id
       AND g.geo_type = 'country'
    JOIN dim_indicator i
        ON f.indicator = i.id
    WHERE i.code IN (
        'SDG_1041_NOC_RT',     -- labor share
        'SL.GDP.PCAP.EM.KD',   -- productivity
        'SI.POV.GINI',         -- gini
        'BX.KLT.DINV.CD.WD',   -- FDI inflows
        'BM.KLT.DINV.CD.WD',   -- FDI outflows
        'NY.GDP.MKTP.KD'       -- GDP (constant USD)
    )
),

macro_pivot AS (
    SELECT
        year,
        geo_id,
        country,
        region,
        income_group,

        MAX(CASE WHEN indicator_code = 'SDG_1041_NOC_RT'
                 THEN value END) AS labor_share,

        MAX(CASE WHEN indicator_code = 'SL.GDP.PCAP.EM.KD'
                 THEN value END) AS productivity,

        MAX(CASE WHEN indicator_code = 'SI.POV.GINI'
                 THEN value END) AS gini,

        MAX(CASE WHEN indicator_code = 'BX.KLT.DINV.CD.WD'
                 THEN value END) AS fdi_in,

        MAX(CASE WHEN indicator_code = 'BM.KLT.DINV.CD.WD'
                 THEN value END) AS fdi_out,

        MAX(CASE WHEN indicator_code = 'NY.GDP.MKTP.KD'
                 THEN value END) AS gdp
    FROM macro_base
    GROUP BY
        year,
        geo_id,
        country,
        region,
        income_group
),

macro_derived AS (
    SELECT
        *,
        (fdi_in - fdi_out) / gdp AS fdi_net_gdp
    FROM macro_pivot
),

wages AS (
    SELECT
        d.year,
        g.id AS geo_id,
        AVG(f.value) AS avg_hourly_wage
    FROM fact_wages f
    JOIN dim_date d
        ON f.date = d.id
       AND d.is_yearly = TRUE
    JOIN dim_geo g
        ON f.geo = g.id
       AND g.geo_type = 'country'
    JOIN dim_indicator i
        ON f.indicator = i.id
    WHERE i.code = 'EAR_4HRL_SEX_ECO_CUR_NB'
    GROUP BY
        d.year,
        g.id
)

    SELECT
    m.year,
    m.geo_id,
    m.country,
    m.region,
    m.income_group,

    m.labor_share,
    m.productivity,
    m.gini,
    m.fdi_net_gdp,

    w.avg_hourly_wage
FROM macro_derived m
LEFT JOIN wages w
    ON m.geo_id = w.geo_id
   AND m.year = w.year
WHERE m.year >= 1970
ORDER BY m.country, m.year;

