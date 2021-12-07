WITH 
price_data AS (
SELECT date_trunc('week', minute) AS week, symbol, price from prices."usd"
WHERE symbol in ('UMA', 'WETH', 'MATIC', 'USDT','USDC')
AND minute > NOW() - interval '90 day'
),
avg_price_data AS (
SELECT 
    week, 
    symbol, 
    AVG(price) OVER (PARTITION BY symbol ORDER BY week) AS three_month_avg_price FROM price_data
),

avg_price_data_final AS (
SELECT * FROM avg_price_data
WHERE week > now() - interval '90 day'
GROUP BY 1,2,3
ORDER BY 1 DESC
)

SELECT * from avg_price_data_final
LIMIT 5