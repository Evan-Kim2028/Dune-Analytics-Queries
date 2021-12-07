-- Original query Ninja Squad Sales - https://dune.xyz/queries/254925/478191

-- data used:
-- opensea."WyvernExchange_evt_OrdersMatched"
-- opensea."WyvernExchange_call_atomicMatch_"

SELECT 
    date_trunc('day', evt_block_time) AS Day,
    SUM(price/1e18) AS eth_volume,
    COUNT(evt_tx_hASh) AS transactions,
    AVG(price/1e18) AS avg_price,
    - COUNT(DISTINCT maker) AS unique_sellers,
    COUNT(DISTINCT taker) AS unique_buyers
FROM opensea."WyvernExchange_evt_OrdersMatched" om
INNER JOIN opensea."WyvernExchange_call_atomicMatch_" a 
ON a.call_tx_hash = om.evt_tx_hash
AND a.addrs[5] = '\x8c186802b1992f7650ac865d4ca94d55ff3c0d17'
GROUP BY 1
