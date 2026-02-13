{{ config(
    materialized='ephemeral'
) }}


select  v.trip_key,
        v.svc_date, 
        try_cast(v.arrival as time) as arrival_real,
        try_cast(v.departure as time) as departure_real,
        m.sch_trip_id, 
        try_cast(
            right('0' + cast((cast(parsename(replace(s.arrival_time, ':', '.'), 3) as int) % 24) as varchar(2)), 2) + ':' +
            right('0' + parsename(replace(s.arrival_time, ':', '.'), 2), 2) + ':' +
            right('0' + parsename(replace(s.arrival_time, ':', '.'), 1), 2)
        as time) as arrival_planned, 
        try_cast(
            right('0' + cast((cast(parsename(replace(s.departure_time, ':', '.'), 3) as int) % 24) as varchar(2)), 2) + ':' +
            right('0' + parsename(replace(s.departure_time, ':', '.'), 2), 2) + ':' +
            right('0' + parsename(replace(s.departure_time, ':', '.'), 1), 2)
        as time) as departure_planned
from {{ source('korbato', 'visit') }} as v
join  {{ source('korbato', 'trip_sch_match') }} as m on v.svc_date = m.svc_date and v.trip_key = m.trip_key
join {{ source('gtfs', 'gtfs_stop_times') }} as s on m.sch_trip_id = s.trip_id and v.seq_in_pattern = s.stop_sequence
