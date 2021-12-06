-- Original query OHM Market - https://dune.xyz/queries/277250/523062

-- Data used:
-- dune_user_generated.fluidsonic_ohm_supply_daily
-- dune_user_generated.fluidsonic_ohm_constants
-- dune_user_generated.fluidsonic_ohm_treasury_reserves_daily
-- dune_user_generated.fluidsonic_ohm_supply_daily
-- dune_user_generated.fluidsonic_ohm_prices_dai_daily
-- dune_user_generated.fluidsonic_ohm_prices_eth_daily


WITH
constants AS NOT MATERIALIZED (
    SELECT * FROM dune_user_generated.fluidsonic_ohm_constants
),
backings AS MATERIALIZED (
    SELECT day, SUM(total_backing_usd) AS total_backing_usd, SUM(total_risk_free) AS total_backing_risk_free
    FROM dune_user_generated.fluidsonic_ohm_treasury_reserves_daily
    GROUP BY day
),
supplies AS MATERIALIZED (
    SELECT
        day,
        circulating AS supply_circulating,
        staked AS supply_staked,
        total AS supply_total
    FROM dune_user_generated.fluidsonic_ohm_supply_daily
),
prices AS MATERIALIZED (
    SELECT *
    FROM dune_user_generated.fluidsonic_ohm_prices_dai_daily
    LEFT JOIN dune_user_generated.fluidsonic_ohm_prices_eth_daily USING (day)
),
treasury AS MATERIALIZED (
    SELECT day, SUM(total_usd) AS total_usd
    FROM dune_user_generated.fluidsonic_ohm_treasury_reserves_daily
    GROUP BY day
),
values AS MATERIALIZED (
    SELECT
        last_price_dai AS price_dai,
        last_indexed_price_dai AS indexed_price_dai,
        last_price_dai * supply_circulating AS market_cap_dai,
        CASE WHEN day >= '2021-05-01'::timestamp AT TIME ZONE 'UTC' THEN last_price_eth END AS price_eth,
        CASE WHEN day >= '2021-05-01'::timestamp AT TIME ZONE 'UTC' THEN last_indexed_price_eth END AS indexed_price_eth,
        CONCAT('Ξ', TO_CHAR(last_price_eth, 'FM999,999,990.0000')) AS price_eth_formatted,
        CONCAT('Ξ', TO_CHAR(last_indexed_price_eth, 'FM999,999,990.00')) AS indexed_price_eth_formatted,
        last_price_eth * supply_circulating AS market_cap_eth,
        total_backing_risk_free / supply_circulating AS backing_risk_free,
        total_backing_usd / supply_circulating AS backing_usd,
        CASE WHEN day >= '2021-05-01'::timestamp AT TIME ZONE 'UTC' THEN (last_price_dai / total_backing_usd * supply_circulating) - 1 END AS premium_dai,
        CASE WHEN day >= '2021-05-01'::timestamp AT TIME ZONE 'UTC' THEN (last_price_dai / total_backing_risk_free * supply_circulating) - 1 END AS risk_free_premium_dai,
        supply_staked / supply_circulating AS staked_fraction,
        1 - (supply_staked / supply_circulating) AS not_staked_fraction,
        supply_circulating,
        CONCAT(TO_CHAR(supply_circulating, 'FM999,999,999,999,990'), ' / ', TO_CHAR(supply_total, 'FM999,999,999,999,990')) AS supply_formatted,
        supply_staked,
        supply_total,
        total_backing_risk_free,
        total_backing_usd,
        total_usd,
        day::timestamp AT TIME ZONE 'UTC' AS timestamp
    FROM supplies
    JOIN backings USING (day)
    JOIN prices USING (day)
    JOIN treasury USING (day)
)
SELECT *, staked_fraction * 100 AS staked_percent FROM values
ORDER BY timestamp ASC