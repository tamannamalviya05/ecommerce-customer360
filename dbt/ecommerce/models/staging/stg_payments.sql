
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_payments'
) }}

WITH extracted AS (

    SELECT
        TRIM(RAW_PAYLOAD:payment_id::STRING) AS payment_id,

        TRIM(RAW_PAYLOAD:order_id::STRING) AS order_id,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:customer_id::STRING
        ) AS customer_id,

        TRY_TO_DECIMAL(
            RAW_PAYLOAD:amount::STRING,
            18, 2
        ) AS amount,

        UPPER(TRIM(RAW_PAYLOAD:currency::STRING))
            AS currency,

        LOWER(TRIM(RAW_PAYLOAD:payment_method::STRING))
            AS payment_method,

        UPPER(TRIM(RAW_PAYLOAD:payment_status::STRING))
            AS payment_status,

        TRY_TO_TIMESTAMP_TZ(
            RAW_PAYLOAD:transaction_timestamp::STRING
        ) AS transaction_timestamp,

        _INGESTED_AT AS ingested_at,
        _SOURCE_SYSTEM AS source_system,
        _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_payments') }}

),

deduplicated AS (

    SELECT *
    FROM extracted

    WHERE payment_id IS NOT NULL
      AND payment_id <> ''

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY
            ingested_at DESC NULLS LAST,
            batch_id DESC NULLS LAST
    ) = 1

),

validated AS (

    SELECT
        payment_id,
        order_id,
        customer_id,
        amount,
        currency,
        payment_method,
        payment_status,
        transaction_timestamp,
        ingested_at,
        source_system,
        batch_id

    FROM deduplicated

    WHERE order_id IS NOT NULL
      AND order_id <> ''
      AND customer_id > 0
      AND amount > 0
      AND currency IN ('INR', 'USD', 'EUR', 'GBP')
      AND payment_method IS NOT NULL
      AND payment_method <> ''
      AND payment_status IN (
          'PENDING',
          'SUCCESS',
          'FAILED',
          'REFUNDED'
      )
      AND transaction_timestamp IS NOT NULL

)

SELECT * FROM validated
