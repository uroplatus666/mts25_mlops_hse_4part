{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('transactions_db', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_time,
        merch AS merchant,
        cat_id AS category_id,
        toFloat64(amount) AS amount,
        name_1 AS first_name,
        name_2 AS last_name,
        gender,
        us_state,
        toFloat64(lat) AS customer_lat,
        toFloat64(lon) AS customer_lon,
        toFloat64(merchant_lat) AS merchant_lat,
        toFloat64(merchant_lon) AS merchant_lon,
        toUInt8(target) AS is_fraud
    FROM source
    WHERE amount >= 0
),

enriched AS (
    SELECT
        *,
        {{ amount_bucket('amount') }} AS amount_segment,
        -- Используем нативные функции ClickHouse, так как макросы dbt_date сбоят
        toDate(transaction_time) AS transaction_date,
        toHour(transaction_time) AS transaction_hour,
        dayOfWeek(transaction_time) AS day_of_week
    FROM cleaned
)

SELECT * FROM enriched