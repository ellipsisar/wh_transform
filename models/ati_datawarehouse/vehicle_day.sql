{{
    config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(svc_date)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse'],
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE){% endif %}"
)}}

SELECT  vehicle_day_key,
        svc_date,
        vehicle_id,
        fleet_key,
        num_cars,
        insert_dt,
        _md_filename,
        _md_processed_at
FROM {{ ref('staging_vehicle_day') }}
WHERE rn = 1
{% if is_incremental() %}
  AND svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE)
{% endif %}
