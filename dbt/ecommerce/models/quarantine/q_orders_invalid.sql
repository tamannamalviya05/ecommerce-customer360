{{ config(materialized='view') }}

WITH bronze_orders AS (

    SELECT
        RAW_PAYLOAD:payload:op::STRING AS cdc_operation,

        CASE
            WHEN RAW_PAYLOAD:payload:op::STRING = 'd'
                THEN RAW_PAYLOAD:payload:before
            ELSE RAW_PAYLOAD:payload:after
        END AS order_data,

        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_cdc_orders') }}

),

validated AS (

    SELECT
        order_data:order_id::STRING AS order_id,
        raw_payload,

        CASE
            WHEN cdc_operation IS NULL
                OR cdc_operation NOT IN ('c', 'u', 'r', 'd')
                THEN 'INVALID_CDC_OPERATION'

            WHEN TRY_TO_NUMBER(
                order_data:order_id::STRING
            ) IS NULL
                OR TRY_TO_NUMBER(
                    order_data:order_id::STRING
                ) <= 0
                THEN 'INVALID_ORDER_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_NUMBER(
                        order_data:customer_id::STRING
                    ) IS NULL
                    OR TRY_TO_NUMBER(
                        order_data:customer_id::STRING
                    ) <= 0
                )
                THEN 'INVALID_CUSTOMER_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_DECIMAL(
                        order_data:total_amount::STRING, 18, 2
                    ) IS NULL
                    OR TRY_TO_DECIMAL(
                        order_data:total_amount::STRING, 18, 2
                    ) < 0
                )
                THEN 'INVALID_TOTAL_AMOUNT'

            ELSE NULL
        END AS validation_rule

    FROM bronze_orders

)

SELECT
    'RAW_CDC_ORDERS' AS source_table,
    order_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Orders CDC failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL