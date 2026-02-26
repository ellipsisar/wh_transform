{{ config(materialized='ephemeral') }}

SELECT
    Id,
    ServiceDate,
    Subsystem                                AS GroupId,
    RouteId,
    TripCount                                AS NumTrips,
    ROUND(RevenueSeconds / 3600.0, 2)        AS RevenueHours,
    ROUND(RevenueMeters / 1609.34, 2)        AS RevenueMiles,
    CreatedAt,
    Version
FROM {{ source('sonnell', 'SonnellDailySummary') }}
