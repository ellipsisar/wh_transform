{{ config(
    materialized='table',
    tags=['sonnell'],
    alias='SonnellDailySummary'
    ) 
}}

WITH keep AS (
    SELECT svc_date, MAX(_md_processed_at) AS keep_day
    FROM {{ source('external', 'sonnell_subsystem') }}
    GROUP BY svc_date
)
SELECT ROW_NUMBER() OVER (ORDER BY t._md_processed_at) AS Id
    ,CAST(t.svc_date AS DATE) AS ServiceDate
    ,subsystem as Subsystem
    ,route_id as RouteId
    ,num_trips as TripCount
    ,revenue_meters as RevenueMeters
    ,revenue_seconds as RevenueSeconds
    ,CAST(insert_dt AS datetime) as GeneratedAt
    ,CAST(GETDATE() AS datetime) as CreatedAt
    ,CAST(FORMAT(t._md_processed_at, 'yyyyMMdd') AS BIGINT) AS Version
    ,CAST(null as datetime) AS CalculationDate
    ,CURRENT_TIMESTAMP as dbt_at
FROM {{ source('external', 'sonnell_subsystem') }} t
JOIN keep k
    ON t.svc_date = k.svc_date
    AND t._md_processed_at = k.keep_day