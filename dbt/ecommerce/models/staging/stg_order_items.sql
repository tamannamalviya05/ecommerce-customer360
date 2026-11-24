{{ config(
    materialized = 'view',
    schema='staging',
    alias='stg_order_items'
) }}

WITH extracted AS (

    SELECT 
     TRY_TO_NUMBER(
        COALESCE(
            RAW_PAYLOAD:payload:after:order_item_id,
            RAW_PAYLOAD:payload:before:order_item_id
        )::STRING
     ) AS order_item_id,

     TRY_TO_NUMBER(
        RAW_PAYLOAD:payload:after:order_id::STRING
     ) AS order_id,

     TRY_TO_NUMBER(
        RAW_PAYLOAD:payload:after:product_id::STRING
     ) AS product_id,

     TRY_TO_NUMBER(
        RAW_PAYLOAD:payload:after:quantity::STRING
     ) AS quantity,

     TRY_TO_DECIMAL(
        RAW_PAYLOAD:payload:after:unit_price::STRING, 18, 2
     ) AS unit_price,

     TRY_TO_DECIMAL(
        RAW_PAYLOAD:payload:after:discount_amount::STRING, 18, 2
     ) AS discount_amount,

     TRY_TO_DECIMAL(
        RAW_PAYLOAD:payload:after:tax_amount::STRING, 18, 2
     ) AS tax_amount,

     TRY_TO_DECIMAL(
        RAW_PAYLOAD:payload:after:shipping_amount::STRING, 18, 2
     ) AS shipping_amount,

     TRY_TO_DECIMAL(
        RAW_PAYLOAD:payload:after:line_total::STRING, 18, 2
     ) AS line_total,

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

     TRY_TO_NUMBER(
        RAW_PAYLOAD:payload:source:row::STRING
     ) AS source_row,

     _INGESTED_AT AS ingested_at,
     _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_cdc_order_items') }}
),

deduplicated AS (

    SELECT * FROM extracted

    WHERE order_item_id > 0
    AND cdc_operation IN ('c', 'u', 'r', 'd')

    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY source_ts_ns DESC NULLS LAST,
                                                                   source_file DESC NULLS LAST,
                                                                   source_position DESC NULLS LAST,
                                                                   source_row DESC NULLS LAST, 
                                                                   ingested_at DESC,
                                                                   batch_id DESC) = 1
),

validated AS (
    SELECT 
      order_item_id,
      order_id,
      product_id,
      quantity,
      unit_price,
      discount_amount,
      tax_amount,
      shipping_amount,
      line_total,
      created_at,
      updated_at,
      cdc_operation,
      ingested_at
    FROM deduplicated

    WHERE cdc_operation <> 'd'
     AND order_id > 0
     AND product_id > 0 
     AND quantity > 0
     AND quantity = FLOOR(quantity)
     AND unit_price >= 0
     AND discount_amount >= 0
     AND tax_amount >= 0
     AND shipping_amount >= 0
     AND line_total >= 0
     AND created_at IS NOT NULL
     AND updated_at IS NOT NULL
)

SELECT * FROM validated