{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  b.svc_date,
        b.trip_key,
        b.sch_trip_id,
        b.insert_dt,
        b._md_filename,
        b._md_processed_at
FROM {{ ref('intermediate_trip_sch_match') }} b
JOIN {{ref('keep_trip_sch_match')}}  k ON  b.svc_date = k.svc_date 
                                        AND b._md_processed_at = k.keep_day