{{ config(
    materialized='table'
) }}

WITH silver_forex AS (
    SELECT * FROM {{ ref('int_forex') }}
),

generate_surrogate_key AS (
    SELECT
        -- BIG TECH STANDARD: Generate Hash Key (Surrogate Key) based on unique attributes
        {{ dbt_utils.generate_surrogate_key(['forex_date', 'base_currency']) }} AS forex_sk,
        
        forex_date,
        base_currency,
        exchange_rate_myr,
        exchange_rate_sgd,
        exchange_rate_eur,
        exchange_rate_gbp,
        
        CURRENT_TIMESTAMP() AS _marts_at,
        '{{ invocation_id }}' AS _batch_id_gold

    FROM silver_forex
)

SELECT * FROM generate_surrogate_key