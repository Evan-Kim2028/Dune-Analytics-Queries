-- original query Chain Runners Hourly Floor and Avg Price - https://dune.xyz/queries/239538/448345

-- Data used:
-- opensea."WyvernExchange_call_atomicMatch_"
-- opensea."WyvernExchange_evt_OrdersMatched"


WITH 
token AS (
    SELECT 
        call_tx_hash AS tx_hash,
        CASE
            WHEN addrs[7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs[7]
        END AS token_address
  FROM opensea."WyvernExchange_call_atomicMatch_" a
  WHERE addrs[5] = '\x97597002980134bea46250aa0510c9b90d87a587' -- RUN address
)
  
SELECT 
    *,
    AVG(usd_floor) OVER (ORDER BY HOUR ROWS BETWEEN 48 PRECEDING AND CURRENT ROW) AS average_floor,
    AVG(usd_avg) OVER (ORDER BY HOUR ROWS BETWEEN 48 PRECEDING AND CURRENT ROW) AS average_price
FROM (
    SELECT 
        date_trunc('hour', evt_block_time) AS HOUR,
        MIN((om.price / 10^erc.decimals) * p.price)  AS usd_floor,
        AVG((om.price / 10^erc.decimals) * p.price) AS usd_avg,
        SUM((om.price / 10^erc.decimals) * p.price) AS usd_volume,
        count (*) AS transactions
    FROM opensea."WyvernExchange_evt_OrdersMatched" om
    INNER JOIN token ON token.tx_hash = om.evt_tx_hash
    INNER JOIN erc20.tokens erc ON token.token_address = erc.contract_address
    INNER JOIN prices.usd p ON p.minute = date_trunc('minute', evt_block_time)
    AND token.token_address = p.contract_address
    AND date_trunc('month', p.minute) >= '2021-04-01'
    GROUP BY 1
    ORDER BY 1 
) t