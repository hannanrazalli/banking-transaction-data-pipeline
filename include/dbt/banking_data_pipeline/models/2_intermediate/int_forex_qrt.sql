{{ config(
    materialized='incremental'
) }}

SELECT *
FROM {{ ref('stg_forex') }}
WHERE _record_status = 'CORRUPT'