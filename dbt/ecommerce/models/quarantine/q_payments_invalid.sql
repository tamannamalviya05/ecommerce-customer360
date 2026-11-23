{{ config(materialized='view') }}

WITH bronze_payments AS (

    SELECT
        RAW_PAYLOAD:payment_id::STRING AS payment_id,
        RAW_PAYLOAD:order_id::STRING AS order_id,
        TRY_TO_DECIMAL(
            RAW_PAYLOAD:amount::STRING, 18, 2
        ) AS amount,
        RAW_PAYLOAD:payment_status::STRING AS payment_status,
        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_payments') }}

),

validated AS (

    SELECT
        payment_id,
        raw_payload,

        CASE
            WHEN NULLIF(TRIM(payment_id), '') IS NULL
                THEN 'MISSING_PAYMENT_ID'

            WHEN NULLIF(TRIM(order_id), '') IS NULL
                THEN 'MISSING_ORDER_ID'

            WHEN amount IS NULL OR amount <= 0
                THEN 'INVALID_AMOUNT'

            WHEN payment_status IS NULL
                OR UPPER(TRIM(payment_status)) NOT IN (
                    'SUCCESS', 'FAILED', 'PENDING', 'REFUNDED'
                )
                THEN 'UNKNOWN_PAYMENT_STATUS'

            ELSE NULL
        END AS validation_rule

    FROM bronze_payments

)

SELECT
    'RAW_PAYMENTS' AS source_table,
    payment_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Payment failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL