{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT
    event_date,
    entity_name,
    domain,

    SUM(CASE WHEN status = 'PROCESSED' THEN record_count ELSE 0 END) AS records_processed,
    SUM(CASE WHEN status = 'FAILED'    THEN record_count ELSE 0 END) AS records_failed,

    COUNT(*)                                                          AS executions_total,
    SUM(CASE WHEN status = 'PROCESSED' THEN 1 ELSE 0 END)            AS executions_success,
    SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END)            AS executions_failed,
    SUM(CASE WHEN status = 'SKIPPED'   THEN 1 ELSE 0 END)            AS executions_skipped,

    MAX(CASE WHEN status = 'PROCESSED' THEN process_datetime ELSE NULL END) AS last_success_datetime,
    MAX(CASE WHEN status = 'FAILED'    THEN process_datetime ELSE NULL END) AS last_error_datetime,
    MAX(CASE WHEN status = 'FAILED'    THEN error_message    ELSE NULL END) AS last_error_message

FROM {{ ref('stg_control_status') }}
GROUP BY
    event_date,
    entity_name,
    domain
