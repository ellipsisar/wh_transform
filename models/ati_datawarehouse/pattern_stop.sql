{{
    config(
    materialized='table',
    dist='HASH(pattern_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse']
)}}

SELECT  pattern_id,
        stop_seq,
        stop_id,
        meters,
        _md_filename,
        _md_processed_at
FROM {{ ref('staging_pattern_stop') }}
WHERE rn = 1
