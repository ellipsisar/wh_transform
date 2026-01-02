{{ config(
    materialized='table',
    tag='analytics'
) }}

SELECT DISTINCT operator_id, fleet_name as operator_name
FROM {{ source('korbato', 'fleet') }}