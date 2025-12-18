{{ config(
    materialized='view',
    tag='analytics'
) }}

SELECT
    vd.svc_date as ServiceDate,
    route_id as Route,
    operator_id as OperatorFleetId,
    0 as OperatedTripsCount,
    0 as CancelledTripsCount,
    0 as DelayedTripsCount,
    0 as OnTimeDepartureCount,
    0 as OnTimeArrivalCount,
    0 as DelayedDepartureCount,
    0 as DelayedArrivalCount,
    0 as VehiclesOperatedCount,
    0 as TotalVehiclesCount,
    0 as AverageDelayMinutes
FROM {{ source('korbato', 'vehicle_day') }} vd
JOIN {{ source('korbato', 'trip') }} t 
    ON vd.vehicle_day_key = t.vehicle_day_key
JOIN {{ source('korbato', 'fleet') }} f
    ON vd.fleet_key = f.fleet_key
GROUP BY vd.svc_date, route_id, operator_id