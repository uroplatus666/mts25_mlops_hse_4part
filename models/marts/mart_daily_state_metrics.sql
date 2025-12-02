{{ config(materialized = 'table') }}

WITH daily_agg AS (
    SELECT
        toDate(transaction_time)                                   AS transaction_date,
        us_state,
        count()                                                    AS transaction_count,
        sum(amount)                                                AS total_amount,
        avg(amount)                                                AS avg_amount,
        quantile(0.95)(amount)                                     AS p95_amount,
        countIf(amount > 1000)                                     AS large_tx_count,
        round(countIf(amount > 1000) * 100.0 / count(), 4)          AS large_tx_share,
        countIf(is_fraud = 1)                                      AS fraud_count,
        round(countIf(is_fraud = 1) * 100.0 / count(), 4)           AS fraud_rate
    FROM {{ ref('stg_transactions') }}
    GROUP BY transaction_date, us_state
)

SELECT
    transaction_date,
    us_state,
    transaction_count,
    total_amount,
    round(avg_amount, 2)                                       AS avg_amount,
    p95_amount,
    large_tx_count,
    large_tx_share,
    fraud_count,
    fraud_rate
FROM daily_agg
ORDER BY transaction_date DESC, fraud_rate DESC