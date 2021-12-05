-- original query Chain Runners- OpenSea Sales - https://dune.xyz/queries/239494/448261

-- Data used:
-- opensea."WyvernExchange_call_atomicMatch_"
-- opensea."WyvernExchange_evt_OrdersMatched"
-- erc721."ERC721_evt_Transfer"
-- erc20."tokens"
-- prices."usd"
-- nft."tokens"

WITH token AS (
    SELECT 
        call_tx_hash AS tx_hash, 
          CASE
              WHEN addrs[7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH address
              ELSE addrs[7]
          END AS token_address
   FROM opensea."WyvernExchange_call_atomicMatch_")
   ,

transactions AS (
SELECT 
    opensea.evt_block_time AS date,
    "tokenId",
    erc.contract_address AS contract_address,
    opensea.price / 1e18 AS amount_eth,
    opensea.price / 10^erc.decimals * p.price AS amount_usd,
    concat('https://opensea.io/assets/0x97597002980134bea46250aa0510c9b90d87a587/', "tokenId") AS url,
    maker AS seller,
    taker AS buyer
FROM opensea."WyvernExchange_evt_OrdersMatched" opensea
INNER JOIN erc721."ERC721_evt_Transfer" mee ON mee.evt_tx_hash = opensea.evt_tx_hash
INNER JOIN token ON token.tx_hash = opensea.evt_tx_hash
INNER JOIN erc20."tokens" erc ON erc.contract_address = token.token_address
INNER JOIN prices."usd" p ON p.minute = date_trunc('minute', opensea.evt_block_time)
AND mee.contract_address = '\x97597002980134bea46250aa0510c9b90d87a587' -- RUN address
AND token.token_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH address
AND token.token_address = p.contract_address
AND mee.evt_block_time >= current_date - interval '7 days'
AND p.minute >= current_date - interval '7 days'
ORDER BY 3 DESC, 4 DESC
)

SELECT  
    date,
    "tokenId",
    sum(transactions.amount_eth) AS amount_eth
FROM transactions 
LEFT JOIN nft."tokens" nft_tokens ON nft_tokens.contract_address = transactions.contract_address
GROUP BY 1,2
ORDER BY 2 DESC