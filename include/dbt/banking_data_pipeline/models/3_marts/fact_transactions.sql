{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "transaction_date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH silver_transactions AS (
    SELECT * FROM {{ ref('int_transactions') }}
    WHERE _is_deleted = FALSE 
    
    {% if is_incremental() %}
        AND tx_ingest_at >= (
            SELECT TIMESTAMP_SUB(MAX(tx_ingest_at), INTERVAL 1 DAY) 
            FROM {{ this }}
        )
    {% endif %}
),

final_fact AS (
    SELECT
        transaction_id, -- Natural Key dikekalkan (Tak perlu Hash Key)
        account_id,
        transaction_date,
        
        -- BIG TECH STANDARD: Generate the same Hash Key to act as Foreign Key to Dim
        {{ dbt_utils.generate_surrogate_key(['DATE(transaction_date)', 'original_currency']) }} AS forex_sk,
        
        original_amount,
        original_currency,
        exchange_rate,
        amount_myr,
        
        status,
        tx_ingest_at,
        
        CURRENT_TIMESTAMP() AS _marts_at,
        '{{ invocation_id }}' AS _batch_id_gold

    FROM silver_transactions
)

SELECT * FROM final_fact