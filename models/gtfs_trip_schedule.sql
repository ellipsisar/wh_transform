{{ config(
    materialized='table',
    tag='analytics'
) }}

SELECT *
FROM {{ source('gtfs', 'gtfs_trips') }}  t