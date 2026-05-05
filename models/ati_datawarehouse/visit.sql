{{
    config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(svc_date)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse'],
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE){% endif %}"
)}}

SELECT  visit_key,
        svc_date,
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
        _md_processed_at
FROM {{ ref('staging_visit') }}
WHERE rn = 1
{% if is_incremental() %}
  AND svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE)
{% endif %}
