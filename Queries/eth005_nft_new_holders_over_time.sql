-- original query Chain Runners new Holders over Time - https://dune.xyz/queries/239502/448273

-- Data used:
-- erc721."ERC721_evt_Transfer"

WITH 
addresses AS ( 
    SELECT 
        "to" AS adr
    FROM erc721."ERC721_evt_Transfer" tr
    WHERE contract_address = '/x0000000000'),
transfers AS ( 
    SELECT 
        DAY,
        address,
        token_address,
        1 AS amount -- Net inflow or outflow per day
    FROM ( 
        SELECT 
            date_trunc('day', evt_block_time) AS DAY,
            "to" AS address,
            tr.contract_address AS token_address
        FROM erc721."ERC721_evt_Transfer" tr --INNER JOIN addresses ad ON tr."to" = ad.adr
        WHERE contract_address = CONCAT('\x', substring('0x97597002980134bea46250aa0510c9b90d87a587' FROM 3))::bytea --RUN address
        UNION ALL 
        SELECT 
            date_trunc('day', evt_block_time) AS DAY,
            "from" AS address,
            tr.contract_address AS token_address
        FROM erc721."ERC721_evt_Transfer" tr
        WHERE contract_address = CONCAT('\x', substring('0x97597002980134bea46250aa0510c9b90d87a587' FROM 3))::bytea --RUN address
        ) t
    GROUP BY 1,2,3
),

balances_with_gap_days AS ( 
    SELECT t.day,
            address,
            SUM(1) OVER (PARTITION BY address ORDER BY t.day) AS balance,
            lead(DAY, 1, now()) OVER (PARTITION BY address ORDER BY t.day) AS next_day
    FROM transfers t),

time_series AS ( 
    SELECT 
        generate_series('2016-01-20'::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
),

balance_all_days AS ( 
    SELECT 
        d.day,
        address,
        SUM(balance/10^0) AS balance
    FROM balances_with_gap_days b
    INNER JOIN time_series d ON b.day <= d.day
    AND d.day < b.next_day -- Yields an observation for every day after the first transfer until the next day with transfer
    GROUP BY 1,2
    ORDER BY 1,2
)

SELECT 
    b.day AS "Date",
    COUNT(address) - lag(COUNT(address)) OVER (ORDER BY b.day) AS "# New Holders"
FROM balance_all_days b
WHERE balance > 0
GROUP BY 1
ORDER BY 1
