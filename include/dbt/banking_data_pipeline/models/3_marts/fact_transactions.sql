{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    partition_by={
      "field": "transaction_date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH silver_tx AS (
    SELECT * FROM {{ ref('int_transactions_refined') }}
    {% if is_incremental() %}
        WHERE _refined_at > (SELECT MAX(_refined_at) FROM {{ this }})
    {% endif %}
),

final_fact AS (
    SELECT
        transaction_id,
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_sk,
        
        transaction_date,
        transaction_type,
        
        original_amount,
        original_currency,
        exchange_rate_to_usd,
        
        ROUND(CAST(original_amount * exchange_rate_to_usd AS FLOAT64), 2) AS amount_usd,
        
        _refined_at,
        {{ audit_columns('gold') }}
        
    FROM silver_tx
)

SELECT * FROM final_fact