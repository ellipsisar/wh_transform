{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  CAST(pattern_id AS nvarchar(4000))     AS pattern_id,
        CAST(stop_seq AS int)                  AS stop_seq,
        CAST(stop_id AS nvarchar(4000))        AS stop_id,
        CAST(meters AS float)                  AS meters,
        CAST(_md_filename AS nvarchar(4000))   AS _md_filename,
        CAST(_md_processed_at AS datetime2(7)) AS _md_processed_at
FROM {{ source('external', 'pattern_stop') }}
