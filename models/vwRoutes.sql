{{ config(
    materialized='view',
    tag='analytics'
) }}

SELECT DISTINCT
    route_id,
    'N/A' AS route_name,
    operator_id
FROM {{ source('korbato', 'vehicle_day') }} vd
JOIN {{ source('korbato', 'trip') }} t 
    ON vd.vehicle_day_key = t.vehicle_day_key
JOIN {{ source('korbato', 'fleet') }} f
    ON vd.fleet_key = f.fleet_key
