{{ config(
    materialized='incremental'
    ) }}

WITH raw_data AS (
    SELECT * 
    FROM {{ source('banking_raw', 'raw_accounts') }}
)

SELECT
    account_id,
    customer_name,
    UPPER(TRIM(account_status)) AS account_status,
    UPPER(TRIM(account_tier)) AS account_tier,
    CAST(credit_limit AS FLOAT64) AS credit_limit,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    _ingest_at,
    
    CASE
        WHEN account_id IS NULL THEN 'CORRUPT'
        WHEN customer_name IS NULL THEN 'CORRUPT'
        WHEN credit_limit IS NULL THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status,
    
    {{ audit_columns('bronze') }}
FROM raw_data