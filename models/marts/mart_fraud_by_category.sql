{{
    config(
        materialized='table'
    )
}}

WITH category_agg AS (
    SELECT
        category_id,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fraud_rate,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount
    FROM {{ ref('stg_transactions') }}
    GROUP BY category_id
)

SELECT * FROM category_agg
ORDER BY fraud_rate DESC