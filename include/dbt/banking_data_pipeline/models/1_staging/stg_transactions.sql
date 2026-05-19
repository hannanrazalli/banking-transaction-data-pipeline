{{ config(
    materialized='table'
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('banking_raw', 'raw_transactions') }}
)

SELECT
    -- Pengekalan lajur asal
    transaction_id,
    account_id,
    CAST(amount AS FLOAT64) AS amount,
    currency,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    UPPER(TRIM(status)) AS status,
    
    -- Lajur Audit Ingestion dari S3
    _ingest_at,

    -- Data Quality Gatekeeper: Menanda rekod sihat dan sakit
    CASE
        WHEN transaction_id IS NULL OR transaction_id = '' THEN 'CORRUPT'
        WHEN account_id IS NULL THEN 'CORRUPT'
        WHEN amount IS NULL OR CAST(amount AS FLOAT64) < 0 THEN 'CORRUPT'
        WHEN currency IS NULL OR currency = '' THEN 'CORRUPT'
        WHEN transaction_date IS NULL THEN 'CORRUPT'
        WHEN UPPER(TRIM(status)) NOT IN ('COMPLETED', 'FAILED', 'PENDING') THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status,

    -- Suntikan Macro Audit dbt
    {{ audit_columns('bronze') }}

FROM raw_data