{{ config(
    materialized='table',
    tag='analytics'
) }}

SELECT DISTINCT operator_id, fleet_name as operator_name
FROM {{ source('korbato', 'fleet') }}
UNION ALL
SELECT  'SN' as operator_id,
        'Sonnell' as operator_name