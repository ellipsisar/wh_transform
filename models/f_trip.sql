{{ config(
    materialized='view'
) }}

SELECT
    t.trip_key,
    v.vehicle_day_key,
    t.pattern_id AS pattern_key,
    t.svc_date AS date_key,
    t.dir_id AS direction_key,
    t.svc_date AS soe_date,
    t.route_id,
    t.in_service AS in_service,
    MIN(v.arrival) AS start_time,
    MAX(v.departure) AS end_time
FROM staging.trip t
JOIN staging.vehicle_day vday ON t.vehicle_day_key = vday.vehicle_day_key
JOIN staging.visit v ON t.trip_key = v.trip_key
GROUP BY t.trip_key, v.vehicle_day_key, t.pattern_id, t.svc_date, t.dir_id, t.route_id, t.in_service;