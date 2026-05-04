{{
    config(
    materialized='ephemeral',
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
	    _md_processed_at,	
		ROW_NUMBER() OVER (PARTITION BY trip_key, svc_date ORDER BY _md_processed_at DESC) as rn
FROM {{ ref('intermediate_trip') }}