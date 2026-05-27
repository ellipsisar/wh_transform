{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT
    event_date,
    entity_name,
    domain,
    error_message   AS latest_error_message,
    error_datetime  AS latest_error_datetime

FROM (
    SELECT
        event_date,
        entity_name,
        domain,
        error_message,
        process_datetime AS error_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, entity_name, domain
            ORDER BY process_datetime DESC
        ) AS rn
    FROM {{ ref('stg_control_status') }}
    WHERE status = 'FAILED'
      AND error_message IS NOT NULL
) AS ranked

WHERE rn = 1
