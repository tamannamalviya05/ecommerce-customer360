
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_ad_spend'
) }}

WITH extracted AS (

    SELECT
        TRIM(RAW_PAYLOAD:spend_id::STRING) AS spend_id,

        TRIM(RAW_PAYLOAD:campaign_id::STRING) AS campaign_id,

        TRIM(RAW_PAYLOAD:campaign_name::STRING) AS campaign_name,

        LOWER(TRIM(RAW_PAYLOAD:channel::STRING)) AS channel,

        TRY_TO_DATE(
            RAW_PAYLOAD:spend_date::STRING
        ) AS spend_date,

        TRY_TO_DECIMAL(
            RAW_PAYLOAD:spend_amount::STRING,
            18, 2
        ) AS spend_amount,

        UPPER(TRIM(RAW_PAYLOAD:currency::STRING)) AS currency,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:impressions::STRING
        ) AS impressions,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:clicks::STRING
        ) AS clicks,

        TRY_TO_NUMBER(
            RAW_PAYLOAD:conversions::STRING
        ) AS conversions,

        _INGESTED_AT AS ingested_at,
        _SOURCE_SYSTEM AS source_system,
        _BATCH_ID AS batch_id

    FROM C360_DEV.BRONZE.RAW_AD_SPEND

),

deduplicated AS (

    SELECT *
    FROM extracted

    WHERE spend_id IS NOT NULL
      AND spend_id <> ''

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY spend_id
        ORDER BY
            ingested_at DESC NULLS LAST,
            batch_id DESC NULLS LAST
    ) = 1

),

validated AS (

    SELECT *
    FROM deduplicated

    WHERE campaign_id IS NOT NULL
      AND campaign_id <> ''
      AND campaign_name IS NOT NULL
      AND campaign_name <> ''
      AND channel IS NOT NULL
      AND channel <> ''
      AND spend_date IS NOT NULL
      AND spend_amount >= 0
      AND currency IN ('INR', 'USD', 'EUR', 'GBP')
      AND impressions >= 0
      AND clicks >= 0
      AND conversions >= 0
      AND impressions = FLOOR(impressions)
      AND clicks = FLOOR(clicks)
      AND conversions = FLOOR(conversions)
      AND clicks <= impressions
      AND conversions <= clicks

)

SELECT * FROM validated
