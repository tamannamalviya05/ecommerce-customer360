
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_customers'
) }}

WITH extracted AS (

    SELECT
        TRY_TO_NUMBER(
            COALESCE(
                RAW_PAYLOAD:payload:after:customer_id,
                RAW_PAYLOAD:payload:before:customer_id
            )::STRING
        ) AS customer_id,

        RAW_PAYLOAD:payload:after:first_name::STRING AS first_name,
        RAW_PAYLOAD:payload:after:last_name::STRING AS last_name,
        LOWER(TRIM(
            RAW_PAYLOAD:payload:after:email::STRING
        )) AS email,

        RAW_PAYLOAD:payload:after:city::STRING AS city,
        RAW_PAYLOAD:payload:after:country::STRING AS country,

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
        TRY_TO_NUMBER(
            RAW_PAYLOAD:payload:source:pos::STRING
        ) AS source_position,
        RAW_PAYLOAD:payload:source:file::STRING AS source_file,

        _INGESTED_AT AS ingested_at,
        _BATCH_ID AS batch_id

    FROM C360_DEV.BRONZE.RAW_CDC_CUSTOMERS

),

deduplicated AS (

    SELECT *
    FROM extracted
    WHERE customer_id > 0
      AND cdc_operation IN ('c', 'u', 'r', 'd')

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
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
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        email,
        city,
        country,
        created_at,
        updated_at,
        cdc_operation,
        ingested_at

    FROM deduplicated

    WHERE cdc_operation <> 'd'
      AND NULLIF(TRIM(first_name), '') IS NOT NULL
      AND NULLIF(TRIM(last_name), '') IS NOT NULL
      AND email LIKE '%@%'
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL

)

SELECT * FROM validated
