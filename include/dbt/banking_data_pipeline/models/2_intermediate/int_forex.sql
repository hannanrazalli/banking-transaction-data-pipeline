{{ config(
    materialized='table'
) }}

WITH clean_data AS (
    SELECT *
    FROM {{ ref('stg_forex') }}
    WHERE _record_status = 'CLEAN'
),

deduped_data AS (
    SELECT *
    FROM clean_data
    -- Mengambil hanya data yang disedut paling terkini jika terdapat duplikasi tarikh
    QUALIFY ROW_NUMBER() OVER (PARTITION BY forex_date, base_currency ORDER BY _ingest_at DESC) = 1
)

SELECT * FROM deduped_data