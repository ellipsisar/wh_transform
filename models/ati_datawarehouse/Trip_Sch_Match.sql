{{
    config(
    materialized='incremental',
    incremental_strategy='append',
    dist='ROUND_ROBIN',
    index='HEAP',
    tags=['ati_datawarehouse'],
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE){% endif %}"
)}}

;

SELECT  svc_date,
        trip_key,
        sch_trip_id,
        insert_dt,
        _md_filename,
        _md_processed_at
FROM {{ ref('staging_trip_sch_match') }}
{% if is_incremental() %}
WHERE svc_date >= CAST(DATEADD(day, -1, GETDATE()) AS DATE)
{% endif %}
