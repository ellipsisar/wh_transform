{{
    config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(trip_key)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse'],
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE){% endif %}"
)}}

SELECT  trip_key,
        svc_date,
        vehicle_day_key,
        in_service,
        trip_id,
        sched_trip,
        num_visits,
        route_id,
        dir_id,
        pattern_id,
        insert_dt,
        _md_filename,
        _md_processed_at
FROM {{ ref('staging_trip') }}
WHERE rn = 1
{% if is_incremental() %}
  AND svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE)
{% endif %}
