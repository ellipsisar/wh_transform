{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  vehicle_day_key,
        svc_date,
        vehicle_id,
        fleet_key,
        num_cars,
        insert_dt,
        _md_filename,
        _md_processed_at,
        ROW_NUMBER() OVER (PARTITION BY vehicle_day_key, svc_date ORDER BY _md_processed_at DESC) AS rn
FROM {{ ref('intermediate_vehicle_day') }}
