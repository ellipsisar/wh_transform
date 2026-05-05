{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  CAST(svc_date AS date)                  AS svc_date,
        CAST(trip_key AS nvarchar(max))          AS trip_key,
        CAST(sch_trip_id AS nvarchar(max))       AS sch_trip_id,
        CAST(insert_dt AS datetime2(7))          AS insert_dt,
        CAST(_md_filename AS nvarchar(max))      AS _md_filename,
        CAST(_md_processed_at AS datetime2(7))   AS _md_processed_at
FROM {{ source('external', 'trip_sch_match') }}
