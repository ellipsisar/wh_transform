{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  CAST(vehicle_day_key AS int)          AS vehicle_day_key,
        CAST(svc_date AS date)                AS svc_date,
        CAST(vehicle_id AS nvarchar(4000))    AS vehicle_id,
        CAST(fleet_key AS nvarchar(4000))     AS fleet_key,
        CAST(num_cars AS int)                 AS num_cars,
        CAST(insert_dt AS datetime2)          AS insert_dt,
        CAST(_md_filename AS nvarchar(4000))  AS _md_filename,
        CAST(_md_processed_at AS datetime2)   AS _md_processed_at
FROM {{ source('external', 'vehicle_day') }}
