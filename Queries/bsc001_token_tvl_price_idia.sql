-- original query IDIA TVL & Price - https://dune.xyz/queries/252186/473256

-- Data used:
-- IDIA token swaps on pancakeswap
-- staking data from impossible finance decoded tables
-- price tables 


WITH buy_data AS (
    SELECT 
        date_trunc('minute',"call_block_time") AS minute,
        avg((tx."value"/1e18)/("amountOut"/1e18)) AS implied_bnb_per_idia
    FROM pancakeswap_v2."PancakeRouter_call_swapETHForExactTokens" s -- buying IDIA
    LEFT JOIN bsc."transactions" tx ON s."call_tx_hash" = tx."hash"
    -- "path" column values returns an array of addresses that represents the path the trade was routed through. 
    -- We want all trades that end with the IDIA contract. To get the last value of the "path" array we use `array_length("path",1)`.
    WHERE "path"[array_length("path",1)] = '\x0b15ddf19d47e6a86a56148fb4afffc6929bcb89' -- IDIA contract
    GROUP BY 1
    ORDER BY 1 DESC 
    ),

price_feed_gapped AS (
    SELECT l."minute",
        lead(l."minute", 1, NOW()) OVER (ORDER BY l."minute") AS next_time,
        r."implied_bnb_per_idia" * l."price" AS "IDIA Price"
    FROM buy_data r
    LEFT JOIN (
        SELECT "minute","price" 
        FROM prices."layer1_usd" 
        WHERE "symbol" = 'BNB' 
        ORDER BY "minute" DESC
        ) l
    ON l."minute" = r."minute"
),

minute_series AS (
    SELECT generate_series('2021-08-18', NOW(), '1 minute'::interval) AS minutes 
),

price_feed AS (
    SELECT m.minutes,
            SUM("IDIA Price") AS "IDIA Price"
    FROM price_feed_gapped pfg
    INNER JOIN minute_series m ON pfg.minute <= m.minutes 
    AND m.minutes < pfg.next_time
    GROUP BY 1
    ORDER BY 1
),

staked_idia AS (
    SELECT 
        u.*,
        l."to" 
    FROM impossible_finance."IFAllocationMaster_evt_Stake" u
    left JOIN bsc."traces" l
    ON u.evt_tx_hash = l.tx_hash
    WHERE l."to" = '\x0b15ddf19d47e6a86a56148fb4afffc6929bcb89' -- IDIA contract
),

unstaked_idia AS (
    SELECT 
        u.*,
        l."to" 
    FROM impossible_finance."IFAllocationMaster_evt_Unstake" u
    LEFT JOIN bsc."traces" l
    ON u.evt_tx_hash = l.tx_hash
    WHERE l."to" = '\x0b15ddf19d47e6a86a56148fb4afffc6929bcb89' -- IDIA contract
),

staked_vol AS (
    SELECT 
        day,
        SUM(amount) AS staked_volume
    FROM (
        SELECT 
            date_trunc('day', evt_block_time) AS day,
            "amount"/1e18 AS amount
        FROM staked_idia
        UNION ALL
        SELECT 
            date_trunc('day', evt_block_time) AS day,
            -"amount"/1e18 AS amount
        FROM unstaked_idia
    ) t 
    GROUP BY 1
),

total_staked_1 AS (
    SELECT 
        day, 
        SUM(staked_volume) OVER (ORDER BY day) AS total_staked
    FROM staked_vol 
),

staked_idia_2 AS (
    SELECT 
        CONCAT('\x', RIGHT("topic3"::text,40))::bytea AS address, 
        bytea2numeric("data")/1e18 AS value,
        "tx_hash",
        "block_time"
    FROM bsc."logs"
    WHERE contract_address = '\x1d37f1e6f0cce814f367d2765ebad5448e59b91b' -- IDIA staking contract
    AND "topic1" = '\xc7de557a4862000809abc99c50fd0b30c35beb37a010dae4335462b336827fd3'
),

unstaked_idia_2 AS (
    SELECT 
        CONCAT('\x', RIGHT("topic3"::text,40))::bytea AS address, 
        bytea2numeric("data")/1e18 AS value,
        "tx_hash",
        "block_time"
    FROM bsc."logs"
    WHERE contract_address = '\x1d37f1e6f0cce814f367d2765ebad5448e59b91b'
    AND "topic1" = '\xbd1cd43f77539b6f0bc66ed76923ac8eb8cb34f9d6ff0c16b6b8b1d43cf94f08'
),

total_staked_vol AS (
    SELECT 
        day,
        SUM(amount) AS staked_volume
    FROM (
        SELECT 
            date_trunc('day', "block_time") AS day,
            "value" AS amount
        FROM staked_idia_2
        UNION ALL
        SELECT 
            date_trunc('day', "block_time") AS day,
            -"value" AS amount
        FROM unstaked_idia_2
    ) t 
    GROUP BY 1
),

total_staked_2 AS (
    SELECT day, 
        SUM(staked_volume) OVER (ORDER BY day) AS total_staked_2
    FROM total_staked_vol
),

total_staked_final AS (
    SELECT t1.day,
            total_staked + COALESCE(total_staked_2,0) AS "total_staked" 
    FROM total_staked_1 t1
    LEFT JOIN total_staked_2 t2 ON t1.day = t2.day
)

SELECT  
    day,
    total_staked * "IDIA Price" AS "TVL",
    total_staked,
    "IDIA Price"        
FROM total_staked_final s
INNER JOIN price_feed p ON date_trunc('minute', s.day) = p.minutes
ORDER BY day