{{
    config(
        materialized='ephemeral',
        tags=['hms']
    )
}}

-- Replicates the three transformation steps of HMS_Transform SP:
--   1. cast_table  : type casts + datetime construction from date+time parts
--   2. clean_table : filter rows with critical nulls
--   3. ranked_table: dedup to most-recent record per (date, vessel, route, scheduled_departure_time)
--
-- Implemented as nested subqueries (not CTEs) so that when dbt inlines this
-- ephemeral model into a parent, no nested WITH clauses are generated.
-- Synapse T-SQL rejects nested CTEs (WITH inside a CTE body).

SELECT *
FROM (
    -- ranked_table: ROW_NUMBER to keep most recent _md_processed_at per trip
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY [date], vessel, route, scheduled_departure_time
            ORDER BY _md_processed_at DESC
        ) AS [rank]
    FROM (
        -- clean_table: drop rows missing critical fields
        SELECT *
        FROM (
            -- cast_table: explicit type casts matching HMS_Transform SP
            SELECT
                CAST([month]              AS VARCHAR(400))    AS [month],
                TRY_CAST([date]           AS DATE)            AS [date],
                CAST([day]                AS VARCHAR(400))    AS [day],
                LTRIM(RTRIM(CAST(vessel   AS VARCHAR(400))))  AS vessel,
                LTRIM(RTRIM(CAST(route    AS VARCHAR(400))))  AS route,
                -- datetime fields: date part + time part concatenated (matches SP)
                CAST(CONCAT(CONVERT(CHAR(10), TRY_CAST([date] AS DATE), 120), ' ', TRY_CAST(scheduled_departure_time AS TIME)) AS DATETIME2) AS scheduled_departure_time,
                CAST(CONCAT(CONVERT(CHAR(10), TRY_CAST([date] AS DATE), 120), ' ', TRY_CAST(actual_departure_time   AS TIME)) AS DATETIME2) AS actual_departure_time,
                CAST(origin               AS VARCHAR(400))    AS origin,
                CAST(destination          AS VARCHAR(400))    AS destination,
                CAST(CONCAT(CONVERT(CHAR(10), TRY_CAST([date] AS DATE), 120), ' ', TRY_CAST(arrival_time           AS TIME)) AS DATETIME2) AS arrival_time,
                CAST(travel_time          AS VARCHAR(400))    AS travel_time,
                -- float-to-int cast handles decimal source values (e.g. "2.0")
                CAST(CAST(pax      AS FLOAT) AS INT)          AS pax,
                CAST(CAST(vehicles AS FLOAT) AS INT)          AS vehicles,
                CAST(variance_schd_vs_act AS VARCHAR(400))    AS variance_schd_vs_act,
                CAST(CAST(val      AS FLOAT) AS INT)          AS val,
                CAST(otp              AS VARCHAR(400))        AS otp,
                CAST(trip_status      AS VARCHAR(400))        AS trip_status,
                CAST(miles            AS DECIMAL(18, 2))      AS miles,
                CAST(comments         AS VARCHAR(400))        AS comments,
                CAST(trip_type        AS VARCHAR(400))        AS trip_type,
                -- DATEDIFF on source columns before aliasing (matches SP CTE behaviour)
                DATEDIFF(minute, actual_departure_time, arrival_time) AS travel_time_minutes,
                -- Monday=0 encoding: ((WEEKDAY+5) % 7), where WEEKDAY: Sun=1...Sat=7
                ((DATEPART(WEEKDAY, TRY_CAST([date] AS DATE)) + 5) % 7) AS day_of_week,
                CAST(_md_filename     AS VARCHAR(400))        AS _md_filename,
                CAST(_md_processed_at AS DATETIME2)           AS _md_processed_at
            FROM {{ source('hms', 'hms_trips') }}
        ) AS cast_table
        WHERE [date]                    IS NOT NULL
            AND vessel                  IS NOT NULL
            AND route                   IS NOT NULL
            AND scheduled_departure_time IS NOT NULL
            AND pax                     IS NOT NULL
    ) AS clean_table
) AS ranked_table
WHERE [rank] = 1
