{{ config(
    materialized='incremental'
) }}

SELECT *
FROM {{ ref('stg_transactions') }}
WHERE _record_status = 'CORRUPT'