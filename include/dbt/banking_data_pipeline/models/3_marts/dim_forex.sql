{{ config(
    materialized='view'
) }}

WITH silver_forex AS (
    SELECT * FROM {{ ref('int_forex_refined') }}
),

with_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['forex_date', 'base_currency']) }} AS forex_sk,
        
        forex_date,
        base_currency,
        exchange_rate_myr,
        exchange_rate_sgd,
        exchange_rate_eur,
        exchange_rate_gbp,
        
        {{ audit_columns('gold') }}

    FROM silver_forex
)

SELECT * FROM with_surrogate_key