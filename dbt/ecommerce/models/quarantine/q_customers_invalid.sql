{{ config(materialized='view') }}

WITH bronze_customers AS (

    SELECT
        RAW_PAYLOAD:payload:op::STRING AS cdc_operation,

        CASE
            WHEN RAW_PAYLOAD:payload:op::STRING = 'd'
                THEN RAW_PAYLOAD:payload:before
            ELSE RAW_PAYLOAD:payload:after
        END AS customer_data,

        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_cdc_customers') }}

),

validated AS (

    SELECT
        customer_data:customer_id::STRING AS customer_id,
        raw_payload,

        CASE
            WHEN cdc_operation IS NULL
                OR cdc_operation NOT IN ('c', 'u', 'r', 'd')
                THEN 'INVALID_CDC_OPERATION'

            WHEN TRY_TO_NUMBER(
                customer_data:customer_id::STRING
            ) IS NULL
                OR TRY_TO_NUMBER(
                    customer_data:customer_id::STRING
                ) <= 0
                THEN 'INVALID_CUSTOMER_ID'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    NULLIF(
                        TRIM(customer_data:first_name::STRING), ''
                    ) IS NULL
                    OR NULLIF(
                        TRIM(customer_data:last_name::STRING), ''
                    ) IS NULL
                )
                THEN 'MISSING_CUSTOMER_NAME'

            WHEN cdc_operation IN ('c', 'u', 'r')
                AND (
                    NULLIF(
                        TRIM(customer_data:email::STRING), ''
                    ) IS NULL
                    OR customer_data:email::STRING NOT LIKE '%@%'
                )
                THEN 'INVALID_EMAIL'

            ELSE NULL
        END AS validation_rule

    FROM bronze_customers

)

SELECT
    'RAW_CDC_CUSTOMERS' AS source_table,
    customer_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Customer CDC failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL