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

WITH clean_transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    WHERE _record_status = 'CLEAN'
    
    {% if is_incremental() %}
        AND _ingest_at >= (
            SELECT TIMESTAMP_SUB(MAX(tx_ingest_at), INTERVAL 1 DAY) 
            FROM {{ this }}
        )
    {% endif %}
),

deduped_transactions AS (
    SELECT *
    FROM clean_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _ingest_at DESC) = 1
),

clean_forex AS (
    SELECT * FROM {{ ref('int_forex') }}
),

joined_data AS (
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.amount AS original_amount,
        t.currency AS original_currency,
        
        CASE
            WHEN t.currency = 'MYR' THEN 1.0
            WHEN t.currency = 'USD' THEN f.exchange_rate_myr
            WHEN t.currency = 'SGD' THEN f.exchange_rate_sgd
            WHEN t.currency = 'EUR' THEN f.exchange_rate_eur
            WHEN t.currency = 'GBP' THEN f.exchange_rate_gbp
            ELSE NULL
        END AS exchange_rate,
        
        t.status,
        
        CASE 
            WHEN t.status = 'CANCELLED' THEN TRUE
            ELSE FALSE
        END AS _is_deleted,

        t._ingest_at AS tx_ingest_at,
        f._ingest_at AS forex_ingest_at

    FROM deduped_transactions t
    LEFT JOIN clean_forex f
        ON DATE(t.transaction_date) = f.forex_date
)

SELECT
    transaction_id,
    account_id,
    transaction_date,
    original_amount,
    original_currency,
    exchange_rate,
    
    ROUND(CAST(original_amount * exchange_rate AS FLOAT64), 2) AS amount_myr,
    
    status,
    _is_deleted,
    tx_ingest_at,
    forex_ingest_at,
    
    CURRENT_TIMESTAMP() AS _refined_at,
    '{{ invocation_id }}' AS _batch_id_silver
    
FROM joined_data