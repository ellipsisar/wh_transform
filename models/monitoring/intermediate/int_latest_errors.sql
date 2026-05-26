{{
  config(
    materialized = 'view'
  )
}}

WITH failed_events AS (
    SELECT * FROM {{ ref('stg_control_status') }}
    WHERE status = 'FAILED'
      AND error_message IS NOT NULL
),

ranked AS (
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
    FROM failed_events
)

SELECT
    event_date,
    entity_name,
    domain,
    error_message AS latest_error_message,
    error_datetime AS latest_error_datetime
FROM ranked
WHERE rn = 1
