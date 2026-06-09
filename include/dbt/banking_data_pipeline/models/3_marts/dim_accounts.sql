{{ config(
    materialized='incremental',
    unique_key='account_sk',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT
        account_id,
        customer_name,
        account_status,
        account_tier,
        credit_limit,
        _ingest_at AS valid_from
    FROM {{ ref('int_accounts_refined') }}
    {% if is_incremental() %}
        WHERE _ingest_at > (
            SELECT COALESCE(MAX(valid_from), CAST('1900-01-01' AS TIMESTAMP))
            FROM {{ this }}
        )
    {% endif %}
),

deduplicate AS (
    SELECT *
    FROM source_data
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY account_id
        ORDER BY valid_from DESC
    ) = 1
),

final_staged AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['account_id', 'customer_name', 'account_status', 'account_tier', 'credit_limit']) }} AS account_sk,
        account_id,
        customer_name,
        account_status,
        account_tier,
        credit_limit,
        valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,
        {{ audit_columns('gold') }}
    FROM deduplicate
)

SELECT *
FROM final_staged

{% if is_incremental() %}
UNION ALL

SELECT
    t.account_sk,
    t.account_id,
    t.customer_name,
    t.account_status,
    t.account_tier,
    t.credit_limit,
    t.valid_from,
    s.valid_from AS valid_to,
    FALSE AS is_current,
    {{ audit_columns('gold') }}
FROM {{ this }} t
INNER JOIN final_staged s
    ON t.account_id = s.account_id
WHERE t.is_current = TRUE
    AND t.account_sk <> s.account_sk
{% endif %}