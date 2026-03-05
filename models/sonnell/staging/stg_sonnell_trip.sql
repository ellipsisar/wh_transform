{{ config(materialized='ephemeral') }}

WITH keep AS (
    SELECT
        svc_date,
        MAX(_md_processed_at) AS keep_day
    FROM {{ source('sonnell_raw', 'sonnell_trip') }}
    GROUP BY svc_date
)

SELECT
    ROW_NUMBER() OVER (ORDER BY t._md_processed_at) AS Id,
    t.svc_date,
    trip_key,
    sched_trip,
    vehicle_id,
    route_id,
    pattern_id,
    trip_start,
    trip_end,
    start_place,
    end_place,
    revenue_seconds,
    revenue_meters,
    insert_dt,
    CAST(GETDATE() AS datetime)                          AS CreatedAt,
    CAST(FORMAT(t._md_processed_at, 'yyyyMMdd') AS BIGINT) AS Version,
    NULL                                                 AS TempDataSeconds,
    NULL                                                 AS TempDataMeters
FROM {{ source('sonnell_raw', 'sonnell_trip') }} t
JOIN keep k
    ON t.svc_date = k.svc_date
    AND t._md_processed_at = k.keep_day
