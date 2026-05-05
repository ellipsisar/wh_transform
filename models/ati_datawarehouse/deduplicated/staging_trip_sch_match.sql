{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

-- Dedup strategy: keep all records from the latest _md_processed_at batch per svc_date.
-- Mirrors the SQL copy step from the original pipeline.
WITH base AS (
    SELECT * FROM {{ ref('intermediate_trip_sch_match') }}
),
keep AS (
    SELECT  svc_date,
            MAX(_md_processed_at) AS keep_day
    FROM base
    GROUP BY svc_date
)

SELECT  b.svc_date,
        b.trip_key,
        b.sch_trip_id,
        b.insert_dt,
        b._md_filename,
        b._md_processed_at
FROM base b
JOIN keep k
    ON  b.svc_date = k.svc_date
    AND b._md_processed_at = k.keep_day
