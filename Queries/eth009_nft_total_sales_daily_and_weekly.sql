-- original query Ninja Squad Sales - https://dune.xyz/queries/254675/477269

-- data used:
-- opensea."WyvernExchange_evt_OrdersMatched"
-- opensea."WyvernExchange_call_atomicMatch_"

SELECT
    MIN(price/1e18),
    PERCENTILE_DISC(0.25) WITHIN group (ORDER BY price/1e18) AS "25th",
    PERCENTILE_DISC(0.50) WITHIN group (ORDER BY price/1e18 ) AS "Median",
    AVG(price/1e18),
    PERCENTILE_DISC(0.75) WITHIN group (ORDER BY price/1e18 ) AS "75th",
    MAX(price/1e18),
    SUM(price/1e18) AS ETH,
    COUNT(evt_tx_hash) AS sales,
    SUM(price/1e18) FILTER (WHERE (NOW() - evt_block_time) <= interval '24 hours') AS vol_24,
    COUNT(evt_tx_hash) FILTER (WHERE (NOW() - evt_block_time) <= interval '24 hours') AS sales_24,
    SUM(price/1e18) FILTER (WHERE (NOW() - evt_block_time) <= interval '1 week') AS vol_7d,
    COUNT(evt_tx_hash) FILTER (WHERE (NOW() - evt_block_time) <= interval '1 week') AS sales_7d
FROM opensea."WyvernExchange_evt_OrdersMatched" om
INNER JOIN opensea."WyvernExchange_call_atomicMatch_" a 
ON a.call_tx_hash = om.evt_tx_hash
INNER JOIN erc20.tokens erc ON a.addrs[7] = erc.contract_address
AND a.addrs[5] = ('\x8c186802b1992f7650ac865d4ca94d55ff3c0d17')
AND price > 0
