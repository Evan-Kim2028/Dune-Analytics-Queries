-- Original query Ninja Squad Minting Progress - https://dune.xyz/queries/254982/478398

-- data used:
-- ethereum.transactions
-- erc721."ERC721_evt_Transfer"

WITH 
base AS (
    SELECT block_time, "from", value/1e18 eth_raised, hash
    FROM ethereum.transactions et
    WHERE et.to = '\x8c186802b1992f7650ac865d4ca94d55ff3c0d17'::BYTEA -- Raise contract receiving ETH
    AND success
    ORDER BY block_number ASC ),
base_evt AS (
    SELECT 
        "to", 
        evt_tx_hash, 
        COUNT(*) AS tot, 
        MAX("tokenId") AS max_id, 
        MIN("tokenId") AS min_id
    FROM erc721."ERC721_evt_Transfer" t
    WHERE t.contract_address = '\x8c186802b1992f7650ac865d4ca94d55ff3c0d17'  and 
    "from" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1,2
), 
first_seen AS (
    SELECT "from", min(block_time) first_seen
    FROM base
    GROUP BY 1
), 
by_blocks AS (
    SELECT  
    block_time,
    SUM(tot) AS minted_in_block,
    SUM(CASE WHEN first_seen.first_seen IS NOT NULL THEN 1 END)  AS first_time_buyers_in_block
    FROM base_evt be
    JOIN base b ON be.evt_tx_hash = b.hash 
    LEFT JOIN first_seen ON first_seen."from" = b."from" AND first_seen.first_seen = b.block_time
    GROUP BY 1
    ORDER BY 1 ASC
)
SELECT 
    block_time,
    SUM(minted_in_block) OVER (ORDER BY block_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total_minted,
    SUM(first_time_buyers_in_block) OVER (ORDER BY block_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) distinct_minter
FROM by_blocks
ORDER BY 1