{{ config(
    materialized='table',
    tags=['sonnell'],
    alias='SonnellTrips'
    ) 
}}

WITH keep AS (
    SELECT svc_date, MAX(_md_processed_at) AS keep_day
    FROM {{ source('external', 'sonnell_trip') }}
    GROUP BY svc_date
)
SELECT ROW_NUMBER() OVER (ORDER BY _md_processed_at) AS Id
      ,t.svc_date AS ServiceDate
      ,trip_key AS TripKey
      ,sched_trip AS ScheduleTrip
      ,vehicle_id AS VehicleId
      ,route_id AS RouteId
      ,pattern_id AS PatternId
      ,trip_start AS TripStart
      ,trip_end AS TripEnd
      ,start_place AS StartLocation
      ,end_place AS EndLocation
      ,revenue_seconds AS RevenueSeconds
      ,revenue_meters AS RevenueMeters
      ,insert_dt AS GeneratedAt
    ,CAST(GETDATE() AS datetime) as CreatedAt
    ,CAST(FORMAT(t._md_processed_at, 'yyyyMMdd') AS BIGINT) AS Version
      ,null as TempDataSeconds
      ,null as TempDataMeters
      ,CURRENT_TIMESTAMP as dbt_at
FROM {{ source('external', 'sonnell_trip') }} t
JOIN keep k
    ON t.svc_date = k.svc_date
    AND t._md_processed_at = k.keep_day