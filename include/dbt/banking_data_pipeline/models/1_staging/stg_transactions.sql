{{ config(
    materialized='incremental'
    ) }}

WITH raw_data AS (
    SELECT * 
    FROM {{ source('banking_raw', 'raw_transactions') }}
)

SELECT
    transaction_id,
    account_id,
    CAST(amount AS FLOAT64) AS amount,
    UPPER(TRIM(currency)) AS currency,
    UPPER(TRIM(transaction_type)) AS transaction_type,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    _ingest_at,

    CASE
        WHEN transaction_id IS NULL THEN 'CORRUPT'
        WHEN account_id IS NULL THEN 'CORRUPT'
        WHEN amount IS NULL THEN 'CORRUPT'
        WHEN currency IS NULL THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status,

    {{ audit_columns('bronze') }}
FROM raw_data