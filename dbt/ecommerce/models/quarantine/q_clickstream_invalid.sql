{{ config(materialized='view') }}

WITH bronze_clickstream AS (

    SELECT
        RAW_PAYLOAD:event_id::STRING AS event_id,
        RAW_PAYLOAD:session_id::STRING AS session_id,
        RAW_PAYLOAD:event_type::STRING AS event_type,
        RAW_PAYLOAD:event_timestamp::STRING AS event_timestamp,
        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_clickstream') }}

),

validated AS (

    SELECT
        event_id,
        raw_payload,

        CASE
            WHEN NULLIF(TRIM(event_id), '') IS NULL
                THEN 'MISSING_EVENT_ID'

            WHEN NULLIF(TRIM(session_id), '') IS NULL
                THEN 'MISSING_SESSION_ID'

            WHEN event_type IS NULL
                OR LOWER(TRIM(event_type)) NOT IN (
                    'page_view',
                    'product_view',
                    'add_to_cart',
                    'remove_from_cart',
                    'checkout_started',
                    'purchase'
                )
                THEN 'INVALID_EVENT_TYPE'

            WHEN TRY_TO_TIMESTAMP_TZ(event_timestamp) IS NULL
                THEN 'INVALID_EVENT_TIMESTAMP'

            ELSE NULL
        END AS validation_rule

    FROM bronze_clickstream

),

duplicate_events AS (

    SELECT event_id
    FROM bronze_clickstream
    WHERE NULLIF(TRIM(event_id), '') IS NOT NULL
    GROUP BY event_id
    HAVING COUNT(*) > 1

)

SELECT
    'RAW_CLICKSTREAM' AS source_table,
    v.event_id AS source_record_id,
    v.raw_payload,

    COALESCE(
        v.validation_rule,
        CASE
            WHEN d.event_id IS NOT NULL
                THEN 'DUPLICATE_EVENT_ID'
        END
    ) AS validation_rule,

    'Clickstream failed validation: ' ||
    COALESCE(
        v.validation_rule,
        'DUPLICATE_EVENT_ID'
    ) AS failure_reason

FROM validated v

LEFT JOIN duplicate_events d
    ON v.event_id = d.event_id

WHERE v.validation_rule IS NOT NULL
   OR d.event_id IS NOT NULL
   