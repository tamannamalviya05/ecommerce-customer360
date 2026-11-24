
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_clickstream'
) }}

WITH extracted AS (

    SELECT
        TRIM(RAW_PAYLOAD:event_id::STRING) AS event_id,

        TRIM(RAW_PAYLOAD:session_id::STRING) AS session_id,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:customer_id::STRING
        ) AS customer_id,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:product_id::STRING
        ) AS product_id,

        LOWER(TRIM(
            RAW_PAYLOAD:event_type::STRING
        )) AS event_type,

        TRY_TO_TIMESTAMP_TZ(
            RAW_PAYLOAD:event_timestamp::STRING
        ) AS event_timestamp,

        TRIM(RAW_PAYLOAD:page_url::STRING) AS page_url,

        LOWER(TRIM(
            RAW_PAYLOAD:device_type::STRING
        )) AS device_type,

        LOWER(TRIM(
            RAW_PAYLOAD:referrer::STRING
        )) AS referrer,

        _INGESTED_AT AS ingested_at,
        _SOURCE_SYSTEM AS source_system,
        _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_clickstream') }}

),

deduplicated AS (

    SELECT *
    FROM extracted

    WHERE event_id IS NOT NULL
      AND event_id <> ''

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY
            ingested_at DESC NULLS LAST,
            batch_id DESC NULLS LAST
    ) = 1

),

validated AS (

    SELECT *
    FROM deduplicated

    WHERE session_id IS NOT NULL
      AND session_id <> ''
      AND customer_id > 0
      AND event_type IN (
          'page_view',
          'product_view',
          'add_to_cart',
          'remove_from_cart',
          'checkout_started',
          'purchase'
      )
      AND event_timestamp IS NOT NULL
      AND device_type IN (
          'desktop',
          'mobile',
          'tablet'
      )

)

SELECT * FROM validated
