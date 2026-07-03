{{ config(
    materialized='incremental',
    unique_key=['transaction_month', 'original_currency'],
    incremental_strategy='merge'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_transactions') }}
    {% if is_incremental() %}
        WHERE _refined_at > (SELECT COALESCE(MAX(_marts_at), CAST('1900-01-01' AS TIMESTAMP)) FROM {{ this }})
    {% endif %}
)

SELECT 
    FORMAT_TIMESTAMP('%Y-%m', transaction_date) AS transaction_month,
    original_currency,
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount_usd), 2) AS total_revenue_usd,
    CURRENT_TIMESTAMP() AS _marts_at
FROM fact_data
GROUP BY 
    transaction_month, 
    original_currency