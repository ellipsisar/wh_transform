{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  CAST(pattern_id AS nvarchar(4000))      AS pattern_id,
        CAST(route_id AS nvarchar(4000))         AS route_id,
        CAST(gtfs_dir AS nvarchar(4000))         AS gtfs_dir,
        CAST(dir_id AS nvarchar(4000))           AS dir_id,
        CAST(in_service AS nvarchar(4000))       AS in_service,
        CAST(_md_filename AS nvarchar(4000))     AS _md_filename,
        CAST(_md_processed_at AS datetime2(7))   AS _md_processed_at
FROM {{ source('external', 'pattern') }}
