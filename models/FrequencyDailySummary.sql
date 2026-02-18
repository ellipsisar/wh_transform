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
        count(distinct TM.trip_key) AS PlannedTripsCount,
        count(distinct CASE WHEN TM.sch_trip_id IS NULL THEN T.trip_key ELSE NULL END) as CancelledTripsCount,
        count(distinct CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
                AND (DATEDIFF(minute, TM.arrival_planned, TM.arrival_real) >= 5 
                    OR DATEDIFF(minute, TM.departure_planned, TM.departure_real) >= 5)
            THEN TM.trip_key
            ELSE NULL 
        END) as DelayedTripsCount,
        count(distinct CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
                AND DATEDIFF(minute, TM.departure_planned, TM.departure_real) < 5
            THEN TM.trip_key
            ELSE NULL 
        END) as OnTimeDepartureCount,
        count(distinct CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
                AND DATEDIFF(minute, TM.arrival_planned, TM.arrival_real) < 5
            THEN TM.trip_key
            ELSE NULL 
        END) as OnTimeArrivalCount,
        count(distinct CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
                AND DATEDIFF(minute, TM.departure_planned, TM.departure_real) >= 5
            THEN TM.trip_key
            ELSE NULL 
        END) as DelayedDepartureCount,
        count(distinct CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
                AND DATEDIFF(minute, TM.arrival_planned, TM.arrival_real) >= 5
            THEN TM.trip_key
            ELSE NULL 
        END) as DelayedArrivalCount,
        count(distinct CASE WHEN TM.sch_trip_id IS NOT NULL THEN TM.trip_key ELSE NULL END) as ExecutedPlannedTripsCount,
        count(distinct vehicle_id) VehiclesOperatedCount,
        MAX(TotalVehiclesCount) TotalVehiclesCount,
        AVG(CASE 
            WHEN TM.sch_trip_id IS NOT NULL 
            THEN CASE 
                WHEN DATEDIFF(minute, TM.arrival_planned, TM.arrival_real) > DATEDIFF(minute, TM.departure_planned, TM.departure_real)
                THEN DATEDIFF(minute, TM.arrival_planned, TM.arrival_real)
                ELSE DATEDIFF(minute, TM.departure_planned, TM.departure_real)
            END
            ELSE NULL 
        END) as AverageDelayMinutes

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
