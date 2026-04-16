{{ config(materialized='ephemeral') }}

WITH keep AS (
    SELECT svc_date, MAX(_md_processed_at) AS keep_day
    FROM {{ source('external', 'sonnell_subsystem') }}
    GROUP BY svc_date
)
SELECT ROW_NUMBER() OVER (ORDER BY t._md_processed_at) AS Id
    ,CAST(t.svc_date AS DATE) AS svc_date
    ,subsystem
    ,route_id
    ,num_trips
    ,revenue_meters
    ,revenue_seconds
    ,CAST(insert_dt AS datetime) as insert_dt
    ,CAST(GETDATE() AS datetime) as CreatedAt
    ,CAST(FORMAT(t._md_processed_at, 'yyyyMMdd') AS BIGINT) AS Version
    ,CAST(null as datetime) AS CalculationDate
FROM {{ source('external', 'sonnell_subsystem') }} t
JOIN keep k
    ON t.svc_date = k.svc_date
    AND t._md_processed_at = k.keep_day