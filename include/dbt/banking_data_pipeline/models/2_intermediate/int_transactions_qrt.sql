{{ config(
    materialized='incremental',
    unique_key=['transaction_id']
) }}

SELECT *
FROM {{ ref('stg_transactions') }}
WHERE _record_status = 'CORRUPT'