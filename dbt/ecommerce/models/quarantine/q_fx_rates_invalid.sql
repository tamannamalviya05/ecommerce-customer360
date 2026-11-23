{{ config(materialized='view') }}

WITH bronze_fx_rates AS (

    SELECT
        RAW_PAYLOAD AS raw_payload

    FROM {{ source('bronze', 'raw_fx_rates') }}

),

validated AS (

    SELECT
        CONCAT(
            COALESCE(raw_payload:base_currency::STRING, 'NULL'),
            '_',
            COALESCE(raw_payload:quote_currency::STRING, 'NULL'),
            '_',
            COALESCE(raw_payload:rate_date::STRING, 'NULL')
        ) AS fx_rate_id,

        raw_payload,

        CASE
            WHEN NULLIF(
                TRIM(raw_payload:base_currency::STRING), ''
            ) IS NULL
                THEN 'MISSING_BASE_CURRENCY'

            WHEN NULLIF(
                TRIM(raw_payload:quote_currency::STRING), ''
            ) IS NULL
                THEN 'MISSING_QUOTE_CURRENCY'

            WHEN LENGTH(
                TRIM(raw_payload:base_currency::STRING)
            ) <> 3
                OR LENGTH(
                    TRIM(raw_payload:quote_currency::STRING)
                ) <> 3
                THEN 'INVALID_CURRENCY_CODE_LENGTH'

            WHEN UPPER(raw_payload:base_currency::STRING)
                = UPPER(raw_payload:quote_currency::STRING)
                THEN 'SAME_BASE_AND_QUOTE_CURRENCY'

            WHEN TRY_TO_DECIMAL(
                raw_payload:exchange_rate::STRING, 18, 6
            ) IS NULL
                OR TRY_TO_DECIMAL(
                    raw_payload:exchange_rate::STRING, 18, 6
                ) <= 0
                THEN 'INVALID_EXCHANGE_RATE'

            WHEN TRY_TO_DATE(
                raw_payload:rate_date::STRING
            ) IS NULL
                THEN 'INVALID_RATE_DATE'

            ELSE NULL
        END AS validation_rule

    FROM bronze_fx_rates

)

SELECT
    'RAW_FX_RATES' AS source_table,
    fx_rate_id AS source_record_id,
    raw_payload,
    validation_rule,
    'FX Rates failed validation: ' || validation_rule
        AS failure_reason

FROM validated

WHERE validation_rule IS NOT NULL