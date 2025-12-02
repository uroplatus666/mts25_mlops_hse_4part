{{
    config(
        materialized='table'
    )
}}

WITH state_agg AS (
    SELECT
        us_state,
        COUNT(DISTINCT CONCAT(first_name, last_name)) AS unique_customers,  -- Пример уникальных клиентов
        COUNT(DISTINCT merchant) AS unique_merchants,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fraud_rate,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount
    FROM {{ ref('stg_transactions') }}
    GROUP BY us_state
)

SELECT * FROM state_agg
ORDER BY fraud_rate DESC