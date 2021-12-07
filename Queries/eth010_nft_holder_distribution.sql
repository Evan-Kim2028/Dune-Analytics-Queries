-- original query Ninja Squad, by Wallet - https://dune.xyz/queries/255136/478724

-- data used:
-- erc721."ERC721_evt_Transfer"

WITH 
hodler AS (
    SELECT 
        current_owner AS num_wallets,
        COUNT(*) AS num_lasc
    FROM (
        SELECT "to" AS current_owner,
            t."tokenId",
            t.evt_block_time
        FROM erc721."ERC721_evt_Transfer" t
        JOIN (
            SELECT "tokenId", MAX(evt_block_number) AS evt_block_number
            FROM erc721."ERC721_evt_Transfer" t
            WHERE t.contract_address = '\x8c186802b1992f7650ac865d4ca94d55ff3c0d17'
            GROUP BY 1 
        ) t2 ON t2."tokenId" = t."tokenId" AND t2.evt_block_number = t.evt_block_number
    ) t3
    GROUP BY 1
)

SELECT 
    COUNT(num_wallets) AS num_wallets,
    CASE
           WHEN num_lasc = 1 THEN '1'
           WHEN num_lasc = 2 THEN '2'
           WHEN 3 <= num_lasc AND num_lasc <= 5 THEN '3-5'
           WHEN 6 <= num_lasc AND num_lasc <= 10 THEN '6-10'
           WHEN 11 <= num_lasc AND num_lasc <= 15 THEN '11-15'
           WHEN 16 <= num_lasc AND num_lasc <= 20 THEN '16-20'
           WHEN 21 <= num_lasc AND num_lasc <= 30 THEN '21-30'
           WHEN 31 <= num_lasc AND num_lasc <= 50 THEN '31-50'
           WHEN 51 <= num_lasc AND num_lasc <= 100 THEN '51-100'
           WHEN 101 <= num_lasc AND num_lasc <= 150 THEN '101-150'
           ELSE '150+'
       END AS num_lasc_bucket,
       CASE
           WHEN num_lasc = 1 THEN 0
           WHEN num_lasc = 2 THEN 1
           WHEN 3 <= num_lasc AND num_lasc <= 5 THEN 2
           WHEN 6 <= num_lasc AND num_lasc <= 10 THEN 3
           WHEN 11 <= num_lasc AND num_lasc <= 15 THEN 4
           WHEN 16 <= num_lasc AND num_lasc <= 20 THEN 5
           WHEN 21 <= num_lasc AND num_lasc <= 30 THEN 6
           WHEN 31 <= num_lasc AND num_lasc <= 50 THEN 7
           WHEN 51 <= num_lasc AND num_lasc <= 100 THEN 8
           WHEN 101 <= num_lasc AND num_lasc <= 150 THEN 9
           ELSE 10
       END AS num_lasc_order
FROM hodler
GROUP BY 2,3
ORDER BY 3 ASC