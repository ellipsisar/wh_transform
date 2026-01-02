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
        0 as CancelledTripsCount,
        0 as DelayedTripsCount,
        0 as OnTimeDepartureCount,
        0 as OnTimeArrivalCount,
        0 as DelayedDepartureCount,
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
WHERE in_service = 't'
GROUP BY 
 F.operator_id,
    T.svc_date,
    T.route_id
