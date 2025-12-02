-- Проверка, что fraud_count <= transaction_count
SELECT *
FROM {{ ref('mart_daily_state_metrics') }}
WHERE fraud_count > transaction_count