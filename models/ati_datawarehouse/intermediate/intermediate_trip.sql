{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']    
)}}

SELECT 	CAST(svc_date AS DATE) as svc_date,
		CAST(trip_key as nvarchar(4000)) as trip_key,
		CAST(vehicle_day_key as int) as vehicle_day_key,
		CAST(in_service as nvarchar(4000)) as in_service,
		CAST(trip_id as nvarchar(4000)) as trip_id,
		CAST(sched_trip as float) as sched_trip,
		CAST(num_visits as int) as num_visits,
		CAST(route_id as nvarchar(4000)) as route_id,
		CAST(dir_id as nvarchar(4000)) as dir_id,
		CAST(pattern_id as nvarchar(4000)) as pattern_id,
		CAST(insert_dt as datetime2) as insert_dt,
		CAST(_md_filename as nvarchar(4000)) as _md_filename,
		CAST(_md_processed_at as datetime2) as _md_processed_at
FROM {{ source('external', 'trip') }}
