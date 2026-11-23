{{ config(materialized='view') }}

WITH bronze_catalog AS (

    SELECT DISTINCT
        RAW_PAYLOAD AS raw_payload
    FROM {{ source('bronze', 'raw_vendor_catalog') }}

),

validated AS (

    SELECT
        raw_payload:product_id::STRING AS product_id,
        raw_payload,

        CASE
            WHEN NULLIF(TRIM(raw_payload:product_id::STRING), '') IS NULL
                THEN 'MISSING_PRODUCT_ID'

            WHEN NULLIF(TRIM(raw_payload:product_name::STRING), '') IS NULL
                THEN 'MISSING_PRODUCT_NAME'

            WHEN NULLIF(TRIM(raw_payload:sku::STRING), '') IS NULL
                THEN 'MISSING_SKU'

            WHEN NULLIF(TRIM(raw_payload:vendor_id::STRING), '') IS NULL
                THEN 'MISSING_VENDOR_ID'

            WHEN TRY_TO_DECIMAL(
                raw_payload:list_price::STRING, 18, 2
            ) IS NULL
                OR TRY_TO_DECIMAL(
                    raw_payload:list_price::STRING, 18, 2
                ) < 0
                THEN 'INVALID_LIST_PRICE'

            WHEN TRY_TO_NUMBER(
                raw_payload:stock_quantity::STRING
            ) IS NULL
                OR TRY_TO_NUMBER(
                    raw_payload:stock_quantity::STRING
                ) < 0
                THEN 'INVALID_STOCK_QUANTITY'

            WHEN raw_payload:currency IS NULL
                OR UPPER(raw_payload:currency::STRING)
                    NOT IN ('USD', 'INR', 'EUR', 'GBP')
                THEN 'INVALID_CURRENCY'

            ELSE NULL
        END AS validation_rule

    FROM bronze_catalog

)

SELECT
    'RAW_VENDOR_CATALOG' AS source_table,
    product_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Vendor Catalog failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL