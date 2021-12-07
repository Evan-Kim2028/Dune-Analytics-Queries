-- Original Query - https://dune.xyz/queries/260421/489786

-- data used:
-- erc20."view_token_balances_daily"
-- dex.trades

WITH 
supply AS (
    SELECT 
        sum(amount_raw), 
        sum(amount_raw) / 1e18 AS "total circulating", 
        day 
    FROM erc20."view_token_balances_daily"
    WHERE token_address = '\x42bBFa2e77757C645eeaAd1655E0911a7553Efbc'
    AND amount_raw > 0
    AND wallet_address <> '\x98d586664fe72119e870b6071f396f03955166c0'
    GROUP BY day
    ORDER BY day DESC
    LIMIT 1
),
daily_price AS (
    SELECT price, block_time AS day FROM (
        SELECT usd_amount / token_a_amount_raw * 1e18 AS price, block_time FROM dex.trades
        WHERE "token_a_address" = '\x42bBFa2e77757C645eeaAd1655E0911a7553Efbc'
        UNION
        SELECT usd_amount / token_b_amount_raw * 1e18 AS price, block_time FROM dex.trades
        WHERE "token_b_address" = '\x42bBFa2e77757C645eeaAd1655E0911a7553Efbc'
    ) pr
    WHERE price > 0
    ORDER BY day DESC
    LIMIT 1
)

SELECT 
    s.day, 
    "total circulating", 
    price AS "lASt price", 
    price * "total circulating" AS "market cap", 
    500000000000000000000000000 * price / 1e18 AS "fdv"
FROM supply s
JOIN daily_price dp
ON 1 = 1
ORDER BY s.day ASC