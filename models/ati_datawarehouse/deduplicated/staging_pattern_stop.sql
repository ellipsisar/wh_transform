{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  pattern_id,
        stop_seq,
        stop_id,
        meters,
        _md_filename,
        _md_processed_at,
        ROW_NUMBER() OVER (PARTITION BY pattern_id ORDER BY _md_processed_at DESC) AS rn
FROM {{ ref('intermediate_pattern_stop') }}
