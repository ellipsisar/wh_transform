{{ config(
    materialized='table',
    tag='analytics'
) }}

SELECT 
        F.operator_id,
        T.svc_date,
        T.route_id,
        count(trip_key) viajes_operados,
        0 as cant_viajes_cancellados,
        0 as cant_viajes_atrasados,
        0 as on_time_departure,
        0 as on_time_arrival,
        0 as delayed_departure,
        0 as delayed_arrival,
        count(distinct vehicle_id) cant_vehiculos_que_operaron,
        count(vehicle_id) cant_total_vehiculos,
        0 as promedio_atraso_viajes
    FROM {{ source('korbato', 'trip') }}  T 
    INNER JOIN{{ source('korbato', 'vehicle_day') }} V ON V.Vehicle_day_key = T.vehicle_day_key
    INNER JOIN {{ source('korbato', 'fleet') }} F ON F.fleet_key = V.fleet_key
    WHERE in_service = 't'
    GROUP BY 
    F.operator_id,
        T.svc_date,
        T.route_id;