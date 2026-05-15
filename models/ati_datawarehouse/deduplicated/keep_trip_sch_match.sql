{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}


SELECT  svc_date,
            MAX(_md_processed_at) AS keep_day
FROM {{ ref('intermediate_trip_sch_match') }}
GROUP BY svc_date