-- original query IDIA Holder Breakdown - https://dune.xyz/queries/248422/473332

-- Data used:
-- BEP20 evt transfers - bep20."BEP20_evt_Transfer"
-- track checkpoints for token holder amount - impossible_finance."IFAllocationMaster_evt_AddTrackCheckpoint"

WITH transfers AS (
    SELECT  
        day,
        address, 
        token_address,
        sum(amount) AS amount
    FROM (
        SELECT  
            date_trunc('day', evt_block_time) AS day,
            "to" AS address,
            tr.contract_address AS token_address,
            value AS amount
        FROM bep20."BEP20_evt_Transfer" tr
        WHERE contract_address = '\x0b15ddf19d47e6a86a56148fb4afffc6929bcb89' -- IDIA contract
        UNION ALL
        SELECT  
            date_trunc('day', evt_block_time) AS day,
            "from" AS address,
            tr.contract_address AS token_address,
            -value AS amount
        FROM bep20."BEP20_evt_Transfer" tr
        WHERE contract_address = '\x0b15ddf19d47e6a86a56148fb4afffc6929bcb89' -- IDIA contract
    ) t
  GROUP BY 1, 2, 3
  ),
  
tracks AS (
SELECT 
    track,
    start_date,
    end_date
FROM (
    SELECT 
        DISTINCT("trackId") AS track,
        min(date_trunc('day',evt_block_time)) AS start_date,
        max(date_trunc('day',evt_block_time)) AS end_date
    FROM impossible_finance."IFAllocationMaster_evt_AddTrackCheckpoint"
    GROUP BY 1
    ) t
    WHERE start_date != end_date
)

SELECT 
    holder_size, 
    COUNT(*) AS number_of_holders, 
    AVG(balance) AS average_holding 
FROM (
    SELECT 
        address, 
        balance,
        -- holder distribution uses exponential distribution with buckets between 10^1, 10^2, 10^3, and 10^4
        (CASE 
            WHEN balance <= 10 THEN '1. tiny_holder'
            WHEN balance > 10 AND balance <= 100 THEN '2. small_holder'
            WHEN balance > 100 AND balance <= 1000 THEN '3. medium_holder'
            WHEN balance > 1000 AND balance <=10000 THEN '4. large_holder'
            ELSE '5. whale' END
        ) AS holder_size
    FROM (SELECT address, SUM(amount)/1e18 AS balance FROM transfers GROUP BY 1) tbl
    WHERE balance > 0 
    AND address != '\x782cb1bc68c949a88f153e2efc120cc7754e402b' -- these are all team wallets
    AND address != '\xc86217a218996359680d89d242a4eac93fc607a9'
    AND address != '\x22b6eb86dc704e34b4c729cfeab6caa4f57efee7'
    AND address != '\x1d37f1e6f0cce814f367d2765ebad5448e59b91b'
    AND address != '\xcb1ab5837893854c57ba5018fab20561c9aa727f'
    AND address != '\x662d9f2783ebcc666bf8d13270795ed32bd2c080'
) t
GROUP BY 1
ORDER BY 1

