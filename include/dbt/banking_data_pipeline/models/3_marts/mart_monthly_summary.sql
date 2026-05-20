{{ config(
    materialized='table'
) }}

WITH fact_data AS (
    SELECT * FROM {{ ref('fact_transactions') }}
)

SELECT 
    -- Ekstrak Tahun dan Bulan untuk laporan bulanan
    FORMAT_TIMESTAMP('%Y-%m', transaction_date) AS transaction_month,
    original_currency,
    
    -- Metrik Perniagaan
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount_myr), 2) AS total_revenue_myr,
    
    CURRENT_TIMESTAMP() AS _marts_at
    
FROM fact_data
GROUP BY 
    transaction_month, 
    original_currency