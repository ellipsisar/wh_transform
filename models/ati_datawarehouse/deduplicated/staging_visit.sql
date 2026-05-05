{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  svc_date,
        visit_key,
        vehicle_day_key,
        seq_in_day,
        arrival,
        door_open,
        door_close,
        departure,
        trip_key,
        seq_in_trip,
        seq_in_pattern,
        scheduled,
        stop_id,
        lat,
        lon,
        ons,
        offs,
        load_out,
        apc_ons,
        apc_offs,
        apc_load_out,
        insert_dt,
        _md_filename,
        _md_processed_at,
        ROW_NUMBER() OVER (PARTITION BY visit_key, svc_date ORDER BY _md_processed_at DESC) AS rn
FROM {{ ref('intermediate_visit') }}
