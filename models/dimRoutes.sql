{{ config(
    materialized='table',
    tag='analytics'
) }}

select distinct operator_id as operator_id,
                r.route_id as route_id,
                r.route_long_name as route_name
from {{ source('raw', 'routes') }} R
INNER JOIN {{ source('korbato', 'trip') }}  T ON T.route_id = R.route_id
INNER JOIN {{ source('korbato', 'vehicle_day') }} V ON V.Vehicle_day_key = T.vehicle_day_key
INNER JOIN {{ source('korbato', 'fleet') }} F ON F.fleet_key = V.fleet_key