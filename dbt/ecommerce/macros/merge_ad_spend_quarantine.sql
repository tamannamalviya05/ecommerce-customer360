{% macro merge_ad_spend_quarantine() %}

    {% set invalid_ad_spend = ref('q_ad_spend_invalid') %}

    {% set merge_sql %}

        MERGE INTO
            C360_DEV.QUARANTINE.QUARANTINED_RECORDS AS target

        USING (
            SELECT
                SOURCE_TABLE,
                SOURCE_RECORD_ID,
                RAW_PAYLOAD,
                VALIDATION_RULE,
                FAILURE_REASON
            FROM {{ invalid_ad_spend }}
        ) AS source

        ON target.SOURCE_TABLE = source.SOURCE_TABLE
        AND EQUAL_NULL(
            target.SOURCE_RECORD_ID,
            source.SOURCE_RECORD_ID
        )
        AND target.VALIDATION_RULE = source.VALIDATION_RULE

        WHEN NOT MATCHED THEN
            INSERT (
                SOURCE_TABLE,
                SOURCE_RECORD_ID,
                RAW_PAYLOAD,
                VALIDATION_RULE,
                FAILURE_REASON,
                STATUS
            )
            VALUES (
                source.SOURCE_TABLE,
                source.SOURCE_RECORD_ID,
                source.RAW_PAYLOAD,
                source.VALIDATION_RULE,
                source.FAILURE_REASON,
                'PENDING'
            )

    {% endset %}

    {% if execute %}
        {% do run_query(merge_sql) %}
        {{ log("Ad Spend quarantine MERGE completed.", info=True) }}
    {% endif %}

{% endmacro %}