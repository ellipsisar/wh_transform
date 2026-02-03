{{ config(
    materialized='ephemeral'
) }}

select  v.trip_key,
        v.svc_date, 
        cast(v.arrival as time) as arrival_real,
        cast(v.departure as time) as departure_real,
        m.sch_trip_id, 
        s.arrival_time as arrival_planned, 
        s.departure_time as departure_planned
from {{ source('korbato', 'visit') }} as v
join  {{ source('korbato', 'trip_sch_match') }} as m on v.svc_date = m.svc_date and v.trip_key = m.trip_key
join {{ source('gtfs', 'gtfs_stop_times') }} as s on m.sch_trip_id = s.trip_id and v.seq_in_pattern = s.stop_sequence



