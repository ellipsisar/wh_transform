{{
    config(
    materialized='table',
    dist='HASH(svc_date)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse']    
)}}

SELECT 	trip_key,
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
WHERE rn=1