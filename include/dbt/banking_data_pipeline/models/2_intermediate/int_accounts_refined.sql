{{ config(
    materialized='incremental',
    unique_key='account_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_accounts') }}
    {% if is_incremental() %}
        WHERE _ingest_at > (
            SELECT COALESCE(TIMESTAMP_SUB(MAX(_ingest_at), INTERVAL 1 HOUR), CAST('1900-01-01' AS TIMESTAMP)) 
            FROM {{ this }}
        )
    {% endif %}
),

deduplicate AS (
    SELECT * FROM source_data
    QUALIFY ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY updated_at DESC, _ingest_at DESC) = 1
),

final_staged AS (
    SELECT
        account_id,
        customer_name,
        account_status,
        account_tier,
        credit_limit,
        updated_at,
        _record_status,
        _ingest_at,
        {{ audit_columns('silver') }}
    FROM deduplicate
)

SELECT * FROM final_staged
WHERE _record_status = 'CLEAN'