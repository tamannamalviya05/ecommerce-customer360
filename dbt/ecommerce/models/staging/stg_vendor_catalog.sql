
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_vendor_catalog'
) }}

WITH extracted AS (

    SELECT
        TRIM(RAW_PAYLOAD:product_id::STRING)
            AS product_id,

        TRIM(RAW_PAYLOAD:sku::STRING)
            AS sku,

        TRIM(RAW_PAYLOAD:product_name::STRING)
            AS product_name,

        TRIM(RAW_PAYLOAD:vendor_id::STRING)
            AS vendor_id,

        TRIM(RAW_PAYLOAD:brand::STRING)
            AS brand,

        TRIM(RAW_PAYLOAD:category::STRING)
            AS category,

        UPPER(TRIM(RAW_PAYLOAD:currency::STRING))
            AS currency,

        TRY_TO_DECIMAL(
            RAW_PAYLOAD:list_price::STRING,
            18, 2
        ) AS list_price,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:stock_quantity::STRING
        ) AS stock_quantity,

        TRY_TO_TIMESTAMP_TZ(
            RAW_PAYLOAD:updated_at::STRING
        ) AS updated_at,

        _INGESTED_AT AS ingested_at,
        _SOURCE_SYSTEM AS source_system,
        _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_vendor_catalog') }}

),

deduplicated AS (

    SELECT *
    FROM extracted

    WHERE product_id IS NOT NULL
      AND product_id <> ''

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY
            updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            batch_id DESC NULLS LAST
    ) = 1

),

validated AS (

    SELECT *
    FROM deduplicated

    WHERE sku IS NOT NULL
      AND sku <> ''
      AND product_name IS NOT NULL
      AND product_name <> ''
      AND vendor_id IS NOT NULL
      AND vendor_id <> ''
      AND brand IS NOT NULL
      AND brand <> ''
      AND category IS NOT NULL
      AND category <> ''
      AND currency IN ('INR', 'USD', 'EUR', 'GBP')
      AND list_price > 0
      AND stock_quantity >= 0
      AND stock_quantity = FLOOR(stock_quantity)
      AND updated_at IS NOT NULL

)

SELECT * FROM validated
