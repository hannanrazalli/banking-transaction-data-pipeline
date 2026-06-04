{{ config(
    materialized='table'
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('banking_raw', 'raw_forex') }}
),

flattened_json AS (
    SELECT
        COALESCE(
            JSON_EXTRACT_SCALAR(forex_data, '$.date'),
            CAST(DATE(_ingest_at) AS STRING)
        ) AS forex_date,
        
        COALESCE(
            JSON_EXTRACT_SCALAR(forex_data, '$.base'),
            JSON_EXTRACT_SCALAR(forex_data, '$.base_code')
        ) AS base_currency,
        
        CAST(COALESCE(JSON_EXTRACT_SCALAR(forex_data, '$.rates.MYR'), JSON_EXTRACT_SCALAR(forex_data, '$.conversion_rates.MYR')) AS FLOAT64) AS exchange_rate_myr,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(forex_data, '$.rates.SGD'), JSON_EXTRACT_SCALAR(forex_data, '$.conversion_rates.SGD')) AS FLOAT64) AS exchange_rate_sgd,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(forex_data, '$.rates.EUR'), JSON_EXTRACT_SCALAR(forex_data, '$.conversion_rates.EUR')) AS FLOAT64) AS exchange_rate_eur,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(forex_data, '$.rates.GBP'), JSON_EXTRACT_SCALAR(forex_data, '$.conversion_rates.GBP')) AS FLOAT64) AS exchange_rate_gbp,

        _ingest_at
    FROM raw_data
)

SELECT
    CAST(forex_date AS DATE) AS forex_date,
    base_currency,
    exchange_rate_myr,
    exchange_rate_sgd,
    exchange_rate_eur,
    exchange_rate_gbp,
    _ingest_at,

    CASE
        WHEN forex_date IS NULL THEN 'CORRUPT'
        WHEN base_currency IS NULL THEN 'CORRUPT'
        WHEN exchange_rate_myr IS NULL OR exchange_rate_myr <= 0 THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status,

    {{ audit_columns('bronze') }}

FROM flattened_json