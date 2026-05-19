{{
    config(
        materialized='table',
        tags=['hms'],
        alias='dbt_HMS_MonthlyDataTrip',
        dist='HASH(Id)',
        index='CLUSTERED COLUMNSTORE INDEX'
    )
}}

-- Replicate HMS_Transform SP: full load (TRUNCATE + INSERT equivalent).
-- materialized='table' drops and recreates on every run — no incremental logic.
-- Id is a synthetic row number ordered by _md_processed_at, matching SP behavior.

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY _md_processed_at) AS INT)  AS Id,
    [rank],
    month                                                        AS [Month],
    [date]                                                       AS [Date],
    day                                                          AS [Day],
    vessel                                                       AS Vessel,
    route                                                        AS Route,
    scheduled_departure_time                                     AS Scheduled_Departure_Time,
    actual_departure_time                                        AS Actual_Departure_Time,
    origin                                                       AS Origin,
    destination                                                  AS Destination,
    arrival_time                                                 AS Arrival_Time,
    travel_time                                                  AS Travel_Time,
    pax                                                          AS PAX,
    vehicles                                                     AS Vehicles,
    variance_schd_vs_act                                         AS Variance_Schd_vs_Act,
    val                                                          AS Val,
    otp                                                          AS OTP,
    trip_status                                                  AS Trip_Status,
    miles                                                        AS Miles,
    comments                                                     AS Comments,
    trip_type                                                    AS Trip_Type,
    travel_time_minutes                                          AS Travel_Time_Minutes,
    day_of_week                                                  AS DayOfWeek,
    _md_filename,
    _md_processed_at

FROM {{ ref('stg_hms__trips') }}
