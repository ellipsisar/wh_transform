{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  pattern_id,
        route_id,
        gtfs_dir,
        dir_id,
        in_service,
        _md_filename,
        _md_processed_at,
        ROW_NUMBER() OVER (PARTITION BY pattern_id ORDER BY _md_processed_at DESC) AS rn
FROM {{ ref('intermediate_pattern') }}
