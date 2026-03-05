{{ config(materialized='ephemeral') }}

SELECT
    Id,
    svc_date                                 AS ServiceDate,
    subsystem                                AS GroupId,
    route_id                                 AS RouteId,
    num_trips                                AS NumTrips,
    ROUND(revenue_seconds / 3600.0, 2)       AS RevenueHours,
    ROUND(revenue_meters / 1609.34, 2)       AS RevenueMiles,
    CreatedAt,
    Version
FROM {{ ref('stg_SonnellDailySummary') }}
