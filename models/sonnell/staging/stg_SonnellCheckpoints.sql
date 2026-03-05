{{ config(materialized='ephemeral') }}

WITH keep AS (
    SELECT
        svc_date,
        MAX(_md_processed_at) AS keep_day
    FROM {{ source('sonnell_raw', 'sonnell_checkpoin') }}
    GROUP BY svc_date
),

month_version AS (
    SELECT
        YEAR(a.svc_date)          AS [year],
        MONTH(a.svc_date)         AS [month],
        MAX(a._md_processed_at)   AS vers
    FROM {{ source('sonnell_raw', 'sonnell_checkpoin') }} a
    JOIN keep
        ON a.svc_date = keep.svc_date
        AND a._md_processed_at = keep.keep_day
    GROUP BY YEAR(a.svc_date), MONTH(a.svc_date)
),

src AS (
    SELECT
        TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(s.svc_date)), ''))  AS ServiceDate,
        CAST(s.trip_key   AS nvarchar(50))                       AS TripKey,
        s.sched_trip                                             AS ScheduleTrip,
        CAST(s.vehicle_id AS nvarchar(50))                       AS VehicleId,
        s.subsystem                                              AS Subsystem,
        CAST(s.visit_key  AS nvarchar(50))                       AS VisitKey,
        TRY_CONVERT(int, s.seq_in_day)                           AS SequenceInDay,
        TRY_CONVERT(int, s.seq_in_trip)                          AS SequenceInTrip,
        s.stop_id                                                AS StopId,
        s.compliant                                              AS Compliant,
        TRY_CONVERT(datetime2(7),
            CASE
                WHEN a.arrival_clean LIKE '%[+-]__:__' THEN LEFT(a.arrival_clean, LEN(a.arrival_clean) - 6)
                WHEN a.arrival_clean LIKE '%[+-]__'    THEN LEFT(a.arrival_clean, LEN(a.arrival_clean) - 3)
                ELSE a.arrival_clean
            END
        ) AS ArrivalTime,
        TRY_CONVERT(datetime2(7),
            CASE
                WHEN a.departure_clean LIKE '%[+-]__:__' THEN LEFT(a.departure_clean, LEN(a.departure_clean) - 6)
                WHEN a.departure_clean LIKE '%[+-]__'    THEN LEFT(a.departure_clean, LEN(a.departure_clean) - 3)
                ELSE a.departure_clean
            END
        ) AS DepartureTime,
        TRY_CONVERT(datetime2(7),
            CASE
                WHEN a.sched_arrival_clean LIKE '%[+-]__:__' THEN LEFT(a.sched_arrival_clean, LEN(a.sched_arrival_clean) - 6)
                WHEN a.sched_arrival_clean LIKE '%[+-]__'    THEN LEFT(a.sched_arrival_clean, LEN(a.sched_arrival_clean) - 3)
                ELSE a.sched_arrival_clean
            END
        ) AS ScheduledArrivaltime,
        TRY_CONVERT(datetime2(7),
            CASE
                WHEN a.sched_departure_clean LIKE '%[+-]__:__' THEN LEFT(a.sched_departure_clean, LEN(a.sched_departure_clean) - 6)
                WHEN a.sched_departure_clean LIKE '%[+-]__'    THEN LEFT(a.sched_departure_clean, LEN(a.sched_departure_clean) - 3)
                ELSE a.sched_departure_clean
            END
        ) AS ScheduleDepartureTime,
        TRY_CONVERT(datetime2(7),
            CASE
                WHEN a.insert_clean LIKE '%[+-]__:__' THEN LEFT(a.insert_clean, LEN(a.insert_clean) - 6)
                WHEN a.insert_clean LIKE '%[+-]__'    THEN LEFT(a.insert_clean, LEN(a.insert_clean) - 3)
                ELSE a.insert_clean
            END
        ) AS GeneratedAt,
        s._md_processed_at
    FROM {{ source('sonnell_raw', 'sonnell_checkpoin') }} s
    CROSS APPLY (
        SELECT
            CASE
                WHEN RIGHT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.arrival)),           ''), 'T', ' '), 'Z', ''), 1) = '.'
                    THEN LEFT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.arrival)),        ''), 'T', ' '), 'Z', ''),
                              LEN(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.arrival)),    ''), 'T', ' '), 'Z', '')) - 1)
                ELSE REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.arrival)),                 ''), 'T', ' '), 'Z', '')
            END AS arrival_clean,
            CASE
                WHEN RIGHT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.departure)),         ''), 'T', ' '), 'Z', ''), 1) = '.'
                    THEN LEFT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.departure)),      ''), 'T', ' '), 'Z', ''),
                              LEN(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.departure)),  ''), 'T', ' '), 'Z', '')) - 1)
                ELSE REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.departure)),               ''), 'T', ' '), 'Z', '')
            END AS departure_clean,
            CASE
                WHEN RIGHT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_arrival)),     ''), 'T', ' '), 'Z', ''), 1) = '.'
                    THEN LEFT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_arrival)),  ''), 'T', ' '), 'Z', ''),
                              LEN(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_arrival)),''), 'T', ' '), 'Z', '')) - 1)
                ELSE REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_arrival)),           ''), 'T', ' '), 'Z', '')
            END AS sched_arrival_clean,
            CASE
                WHEN RIGHT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_departure)),   ''), 'T', ' '), 'Z', ''), 1) = '.'
                    THEN LEFT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_departure)),''), 'T', ' '), 'Z', ''),
                              LEN(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_departure)),''), 'T', ' '), 'Z', '')) - 1)
                ELSE REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.sched_departure)),         ''), 'T', ' '), 'Z', '')
            END AS sched_departure_clean,
            CASE
                WHEN RIGHT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.insert_dt)),         ''), 'T', ' '), 'Z', ''), 1) = '.'
                    THEN LEFT(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.insert_dt)),      ''), 'T', ' '), 'Z', ''),
                              LEN(REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.insert_dt)),  ''), 'T', ' '), 'Z', '')) - 1)
                ELSE REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(s.insert_dt)),               ''), 'T', ' '), 'Z', '')
            END AS insert_clean
    ) a
)

SELECT
    ServiceDate,
    TripKey,
    ScheduleTrip,
    VehicleId,
    Subsystem,
    VisitKey,
    SequenceInDay,
    ArrivalTime,
    DepartureTime,
    ScheduledArrivaltime,
    ScheduleDepartureTime,
    SequenceInTrip,
    StopId,
    Compliant,
    GeneratedAt,
    GETDATE()                                          AS CreatedAt,
    CAST(FORMAT(mv.vers, 'yyyyMMdd') AS BIGINT)        AS Version
FROM src
JOIN keep k
    ON src.ServiceDate = k.svc_date
    AND src._md_processed_at = k.keep_day
JOIN month_version mv
    ON YEAR(src.ServiceDate)  = mv.[year]
    AND MONTH(src.ServiceDate) = mv.[month]
