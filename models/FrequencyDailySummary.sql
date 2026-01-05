{{ config(
    materialized='table',
    tag='analytics'
) }}


WITH CTE_Vehicle 
AS
(
SELECT svc_date, fleet_key, count(vehicle_id) TotalVehiclesCount
FROM {{ source('korbato', 'vehicle_day') }}
GROUP BY svc_date, fleet_key
),

CTE_GtfsNormalized 
AS 
(
SELECT 
ST.stop_id,
DATEADD(
    DAY,
    CAST(LEFT(arrival_time, 2) AS int) / 24,
    DATETIME2FROMPARTS(
        YEAR(svc_date),
        MONTH(svc_date),
        DAY(svc_date),
        CAST(LEFT(arrival_time, 2) AS int) % 24,
        CAST(SUBSTRING(arrival_time, 4, 2) AS int),
        CAST(SUBSTRING(arrival_time, 7, 2) AS int),
        0, 0
    )
) gtfs_arrival_dt, 
DATEADD(
    DAY,
    CAST(LEFT(departure_time, 2) AS int) / 24,
    DATETIME2FROMPARTS(
        YEAR(svc_date),
        MONTH(svc_date),
        DAY(svc_date),
        CAST(LEFT(departure_time, 2) AS int) % 24,
        CAST(SUBSTRING(departure_time, 4, 2) AS int),
        CAST(SUBSTRING(departure_time, 7, 2) AS int),
        0, 0
    )
) gtfs_departure_dt

FROM {{ source('korbato', 'visit') }} Vi 
    LEFT JOIN {{ source('gtfs', 'gtfs_stop_times') }} ST
        ON Vi.stop_id = ST.stop_id
)
SELECT 
        T.svc_date AS ServiceDate,
        T.route_id AS Route,
        F.operator_id AS OperatorFleetId,
        COUNT(DISTINCT T.trip_key) AS OperatedTripsCount,
        SUM(CASE WHEN Gt.stop_id IS NULL THEN 1 ELSE 0 END) as CancelledTripsCount,
        SUM(CASE WHEN Vi.arrival > gtfs_arrival_dt OR Vi.departure > gtfs_departure_dt  THEN 1 ELSE 0 END) as DelayedTripsCount,
        SUM(CASE WHEN Vi.departure BETWEEN gtfs_arrival_dt AND gtfs_departure_dt THEN 1 ELSE 0 END) AS OnTimeDepartureCount,
        SUM(CASE WHEN Vi.arrival BETWEEN gtfs_arrival_dt  AND gtfs_departure_dt THEN 1 ELSE 0 END) as OnTimeArrivalCount,
        SUM(CASE WHEN Vi.departure > gtfs_departure_dt THEN 1 ELSE 0 END) as DelayedDepartureCount,
        SUM(CASE WHEN Vi.arrival > gtfs_arrival_dt  THEN 1 ELSE 0 END) as DelayedArrivalCount,
        count(distinct vehicle_id) VehiclesOperatedCount,
        MAX(TotalVehiclesCount) TotalVehiclesCount,
        --AVG(CASE WHEN Vi.departure > gtfs_departure_dt THEN DATEDIFF(MINUTE, gtfs_departure_dt, Vi.departure) ELSE NULL END) as AverageDelayMinutes
        0 AS AverageDelayMinutes
        
FROM {{ source('korbato', 'trip') }} T
INNER JOIN {{ source('korbato', 'vehicle_day') }} V
    ON V.Vehicle_day_key = T.vehicle_day_key
INNER JOIN {{ source('korbato', 'fleet') }} F
    ON F.fleet_key = V.fleet_key
INNER JOIN CTE_Vehicle C
        ON V.svc_date = C.svc_date
        AND V.fleet_key = C.fleet_key
INNER JOIN {{ source('korbato', 'visit') }} Vi
    ON Vi.Vehicle_day_key = V.Vehicle_day_key
    AND Vi.trip_key = T.trip_key
LEFT JOIN CTE_GtfsNormalized Gt
    ON Vi.stop_id = Gt.stop_id
WHERE in_service = 't'
GROUP BY 
 F.operator_id,
    T.svc_date,
    T.route_id

