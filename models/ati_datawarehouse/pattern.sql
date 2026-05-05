{{
    config(
    materialized='table',
    dist='HASH(pattern_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse']
)}}

SELECT  pattern_id,
        route_id,
        gtfs_dir,
        dir_id,
        in_service,
        _md_filename,
        _md_processed_at
FROM {{ ref('staging_pattern') }}
WHERE rn = 1
