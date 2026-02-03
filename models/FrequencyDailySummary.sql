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
)

SELECT 
        T.svc_date AS ServiceDate,
        T.route_id AS Route,
        F.operator_id AS OperatorFleetId,
        count(distinct T.trip_key) AS OperatedTripsCount,
        sum(CASE WHEN TM.sch_trip_id IS NULL THEN 1 ELSE 0 END) as CancelledTripsCount,
        sum(CASE WHEN T.Compliant='f' THEN 1 ELSE 0 END) as DelayedTripsCount,
        sum(CASE WHEN T.Compliant='t' THEN 1 ELSE 0 END) as OnTimeDepartureCount,
        0 as OnTimeArrivalCount,
        sum(CASE WHEN T.Compliant='f' THEN 1 ELSE 0 END) as DelayedDepartureCount,
        0 as DelayedArrivalCount,
        count(distinct vehicle_id) VehiclesOperatedCount,
        MAX(TotalVehiclesCount) TotalVehiclesCount,
        0 as AverageDelayMinutes

FROM {{ source('korbato', 'trip') }} T
INNER JOIN {{ source('korbato', 'vehicle_day') }} V
    ON V.Vehicle_day_key = T.vehicle_day_key
INNER JOIN {{ source('korbato', 'fleet') }} F
    ON F.fleet_key = V.fleet_key
INNER JOIN CTE_Vehicle C
        ON V.svc_date = C.svc_date
        AND V.fleet_key = C.fleet_key
LEFT JOIN {{ ref ('trip_gtfs_match') }} TM 
        ON TM.trip_key = T.trip_key
WHERE in_service = 't'
GROUP BY 
 F.operator_id,
    T.svc_date,
    T.route_id
