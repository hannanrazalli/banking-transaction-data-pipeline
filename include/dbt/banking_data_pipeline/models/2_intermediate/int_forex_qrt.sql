{{ config(
    materialized='incremental',
    unique_key=['forex_date', 'base_currency']
) }}

SELECT *
FROM {{ ref('stg_forex') }}
WHERE _record_status = 'CORRUPT'