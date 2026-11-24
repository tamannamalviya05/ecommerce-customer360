
{{ config(
    materialized='view',
    schema='staging',
    alias='stg_fx_rates'
) }}

WITH extracted AS (

    SELECT
        UPPER(TRIM(RAW_PAYLOAD:base_currency::STRING))
            AS base_currency,

        UPPER(TRIM(RAW_PAYLOAD:quote_currency::STRING))
            AS quote_currency,

        TRY_TO_DECIMAL(
            RAW_PAYLOAD:exchange_rate::STRING,
            20, 8
        ) AS exchange_rate,

        TRY_TO_DATE(
            RAW_PAYLOAD:rate_date::STRING
        ) AS rate_date,

        _INGESTED_AT AS ingested_at,
        _SOURCE_SYSTEM AS source_system,
        _BATCH_ID AS batch_id

    FROM {{ source('bronze', 'raw_fx_rates') }}

),

validated AS (

    SELECT *
    FROM extracted

    WHERE base_currency IS NOT NULL
      AND LENGTH(base_currency) = 3
      AND REGEXP_LIKE(base_currency, '[A-Z]{3}')
      AND quote_currency IS NOT NULL
      AND LENGTH(quote_currency) = 3
      AND REGEXP_LIKE(quote_currency, '[A-Z]{3}')
      AND base_currency <> quote_currency
      AND exchange_rate > 0
      AND rate_date IS NOT NULL

),

deduplicated AS (

    SELECT *
    FROM validated

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            base_currency,
            quote_currency,
            rate_date
        ORDER BY
            ingested_at DESC NULLS LAST,
            batch_id DESC NULLS LAST
    ) = 1

)

SELECT
    *,
    CONCAT(
        base_currency, '|',
        quote_currency, '|',
        TO_VARCHAR(rate_date, 'YYYY-MM-DD')
    ) AS fx_rate_key
FROM deduplicated
