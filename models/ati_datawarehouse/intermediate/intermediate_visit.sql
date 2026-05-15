{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']
)}}

SELECT  TRY_CAST(svc_date AS date)                 AS svc_date,
        CAST(visit_key AS nvarchar(4000))          AS visit_key,
        CAST(vehicle_day_key AS nvarchar(4000))    AS vehicle_day_key,
        TRY_CAST(seq_in_day AS int)                AS seq_in_day,
        TRY_CAST(arrival AS datetime2(7))          AS arrival,
        TRY_CAST(door_open AS datetime2(7))        AS door_open,
        TRY_CAST(door_close AS datetime2(7))       AS door_close,
        TRY_CAST(departure AS datetime2(7))        AS departure,
        CAST(trip_key AS nvarchar(4000))           AS trip_key,
        TRY_CAST(seq_in_trip AS int)               AS seq_in_trip,
        TRY_CAST(seq_in_pattern AS int)            AS seq_in_pattern,
        CAST(scheduled AS nvarchar(4000))          AS scheduled,
        CAST(stop_id AS nvarchar(4000))            AS stop_id,
        TRY_CAST(lat AS real)                      AS lat,
        TRY_CAST(lon AS real)                      AS lon,
        TRY_CAST(ons AS real)                      AS ons,
        TRY_CAST(offs AS real)                     AS offs,
        TRY_CAST(load_out AS real)                 AS load_out,
        TRY_CAST(apc_ons AS real)                  AS apc_ons,
        TRY_CAST(apc_offs AS real)                 AS apc_offs,
        TRY_CAST(apc_load_out AS real)             AS apc_load_out,
        TRY_CAST(insert_dt AS datetime2(7))        AS insert_dt,
        CAST(_md_filename AS nvarchar(4000))       AS _md_filename,
        CAST(_md_processed_at AS datetime2(7))     AS _md_processed_at
FROM {{ source('external', 'visit') }}
