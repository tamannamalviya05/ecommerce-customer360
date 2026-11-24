
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_orders'
) }}

WITH extracted AS (

    SELECT
        TRY_TO_NUMBER(
            COALESCE(
                RAW_PAYLOAD:payload:after:order_id,
                RAW_PAYLOAD:payload:before:order_id
            )::STRING
        ) AS order_id,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:payload:after:customer_id::STRING
        ) AS customer_id,

        UPPER(TRIM(
            RAW_PAYLOAD:payload:after:order_status::STRING
        )) AS order_status,

        TRY_TO_DECIMAL(
            RAW_PAYLOAD:payload:after:total_amount::STRING,
            18, 2
        ) AS total_amount,

        TRY_TO_TIMESTAMP_TZ(
            RAW_PAYLOAD:payload:after:created_at::STRING
        ) AS created_at,

        TRY_TO_TIMESTAMP_TZ(
            RAW_PAYLOAD:payload:after:updated_at::STRING
        ) AS updated_at,

        RAW_PAYLOAD:payload:op::STRING AS cdc_operation,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:payload:source:ts_ns::STRING
        ) AS source_ts_ns,

        RAW_PAYLOAD:payload:source:file::STRING AS source_file,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:payload:source:pos::STRING
        ) AS source_position,

        _INGESTED_AT AS ingested_at,
        _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_cdc_orders') }}
),

deduplicated AS (

    SELECT *
    FROM extracted

    WHERE order_id > 0
      AND cdc_operation IN ('c', 'u', 'r', 'd')

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY
            source_ts_ns DESC NULLS LAST,
            source_file DESC NULLS LAST,
            source_position DESC NULLS LAST,
            ingested_at DESC,
            batch_id DESC
    ) = 1

),

validated AS (

    SELECT
        order_id,
        customer_id,
        order_status,
        total_amount,
        created_at,
        updated_at,
        cdc_operation,
        ingested_at

    FROM deduplicated

    WHERE cdc_operation <> 'd'
      AND customer_id > 0
      AND NULLIF(order_status, '') IS NOT NULL
      AND total_amount >= 0
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL

)

SELECT * FROM validated
