{{ config(materialized='view') }}

WITH bronze_order_items AS (

    SELECT
        RAW_PAYLOAD:payload:op::STRING AS cdc_operation,

        CASE
            WHEN RAW_PAYLOAD:payload:op::STRING = 'd'
                THEN RAW_PAYLOAD:payload:before
            ELSE RAW_PAYLOAD:payload:after
        END AS item_data,

        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_cdc_order_items') }}

),

validated AS (

    SELECT
        item_data:order_item_id::STRING AS order_item_id,
        raw_payload,

        CASE
            WHEN cdc_operation IS NULL
                OR cdc_operation NOT IN ('c', 'u', 'r', 'd')
                THEN 'INVALID_CDC_OPERATION'

            WHEN TRY_TO_NUMBER(
                item_data:order_item_id::STRING
            ) IS NULL
                OR TRY_TO_NUMBER(
                    item_data:order_item_id::STRING
                ) <= 0
                THEN 'INVALID_ORDER_ITEM_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_NUMBER(
                        item_data:order_id::STRING
                    ) IS NULL
                    OR TRY_TO_NUMBER(
                        item_data:order_id::STRING
                    ) <= 0
                )
                THEN 'INVALID_ORDER_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_NUMBER(
                        item_data:product_id::STRING
                    ) IS NULL
                    OR TRY_TO_NUMBER(
                        item_data:product_id::STRING
                    ) <= 0
                )
                THEN 'INVALID_PRODUCT_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_NUMBER(
                        item_data:quantity::STRING
                    ) IS NULL
                    OR TRY_TO_NUMBER(
                        item_data:quantity::STRING
                    ) <= 0
                )
                THEN 'INVALID_QUANTITY'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    TRY_TO_DECIMAL(
                        item_data:unit_price::STRING, 18, 2
                    ) IS NULL
                    OR TRY_TO_DECIMAL(
                        item_data:unit_price::STRING, 18, 2
                    ) < 0
                )
                THEN 'INVALID_UNIT_PRICE'

            ELSE NULL
        END AS validation_rule

    FROM bronze_order_items

)

SELECT
    'RAW_CDC_ORDER_ITEMS' AS source_table,
    order_item_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Order Items CDC failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL