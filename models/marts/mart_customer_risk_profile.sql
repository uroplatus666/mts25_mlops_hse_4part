{{
    config(
        materialized='table'
    )
}}

WITH customer_agg AS (
    SELECT
        CONCAT(first_name, ' ', last_name) AS customer_name,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fraud_rate,
        AVG(amount) AS avg_amount,
        CASE
            WHEN SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) > 0.05 THEN 'HIGH'
            WHEN SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) > 0.01 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_name
)

SELECT * FROM customer_agg