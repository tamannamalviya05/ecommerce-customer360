{{ config(materialized='view') }}

WITH bronze_ad_spend AS (

    SELECT
        RAW_PAYLOAD:spend_id::STRING AS spend_id,
        RAW_PAYLOAD:campaign_id::STRING AS campaign_id,
        TRY_TO_DECIMAL(
            RAW_PAYLOAD:spend_amount::STRING, 18, 2
        ) AS spend_amount,
        TRY_TO_NUMBER(
            RAW_PAYLOAD:clicks::STRING
        ) AS clicks,
        TRY_TO_NUMBER(
            RAW_PAYLOAD:impressions::STRING
        ) AS impressions,
        RAW_PAYLOAD:currency::STRING AS currency,
        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_ad_spend') }}

),

validated AS (

    SELECT
        spend_id,
        raw_payload,

        CASE
            WHEN NULLIF(TRIM(spend_id), '') IS NULL
                THEN 'MISSING_SPEND_ID'

            WHEN NULLIF(TRIM(campaign_id), '') IS NULL
                THEN 'MISSING_CAMPAIGN_ID'

            WHEN spend_amount IS NULL OR spend_amount < 0
                THEN 'NEGATIVE_SPEND'

            WHEN clicks IS NULL
                OR impressions IS NULL
                OR clicks < 0
                OR impressions < 0
                OR clicks > impressions
                THEN 'INVALID_CLICKS'

            WHEN currency IS NULL
                OR UPPER(TRIM(currency)) NOT IN (
                    'USD', 'INR', 'EUR', 'GBP'
                )
                THEN 'INVALID_CURRENCY'

            ELSE NULL
        END AS validation_rule

    FROM bronze_ad_spend

)

SELECT
    'RAW_AD_SPEND' AS source_table,
    spend_id AS source_record_id,
    raw_payload,
    validation_rule,
    'Ad Spend failed validation: ' || validation_rule
        AS failure_reason

FROM validated
WHERE validation_rule IS NOT NULL