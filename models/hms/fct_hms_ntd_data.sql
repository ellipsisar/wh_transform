{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['hms'],
        alias='dbt_HmsNtdData',
        dist='HASH(Date)',
        index='CLUSTERED COLUMNSTORE INDEX',
        pre_hook="
            {% if is_incremental() and var('hms_ntd_reprocess', false) %}
            DELETE FROM {{ this }}
            WHERE YEAR([Date]) = {{ var('hms_ntd_reprocess_year') }}
                AND MONTH([Date]) = {{ var('hms_ntd_reprocess_month') }}
            {% elif is_incremental() %}
            DELETE FROM {{ this }}
            WHERE YEAR([Date]) = YEAR(GETDATE())
                AND MONTH([Date]) = MONTH(GETDATE())
            {% endif %}
        "
    )
}}

-- Replicate sp_UpsertMonthlyTripSummary: reads from HMS_MonthlyDataTrip,
-- aggregates by (Vessel, Route, Date, DayOfWeek, Origin, Trip_Status),
-- then deduplicates to one row per (Date, Vessel, Route, IsOutbound, IsMissed).
-- MERGE replaced with DELETE+INSERT (Synapse incremental pattern).
-- Default window: current month. Use hms_ntd_reprocess vars for specific months.

WITH trip_summary AS (
    SELECT
        CAST(Vessel AS VARCHAR(255))                                        AS Vessel,
        CAST(Route AS VARCHAR(255))                                         AS Route,
        CAST([Date] AS DATE)                                                AS [Date],
        DayOfWeek,
        CASE
            WHEN LOWER(LTRIM(RTRIM(Origin))) NOT IN ('vieques', 'culebra')
            THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
        END                                                                 AS IsOutbound,
        -- NULL, empty string, or 'MISSED TRIP' all count as missed (matches SP exactly)
        CASE
            WHEN Trip_Status IS NULL
                OR LTRIM(RTRIM(Trip_Status)) = ''
                OR Trip_Status = 'MISSED TRIP'
            THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
        END                                                                 AS IsMissed,
        ISNULL(SUM(Travel_Time_Minutes), 0)                                 AS TotalTravelMinutes,
        ISNULL(SUM(TRY_CAST(REPLACE(CAST(Miles AS VARCHAR(50)), ',', '.') AS DECIMAL(12, 2))), 0) AS TotalTravelMiles,
        ISNULL(SUM(TRY_CAST(PAX AS INT)), 0)                               AS TotalPassengers,
        ISNULL(SUM(TRY_CAST(Vehicles AS INT)), 0)                          AS TotalVehicles
    FROM {{ ref('fct_hms_monthly_trips') }}
    WHERE [Date] IS NOT NULL
        AND Origin IS NOT NULL
        AND Destination IS NOT NULL
    {% if is_incremental() and var('hms_ntd_reprocess', false) %}
        AND YEAR([Date]) = {{ var('hms_ntd_reprocess_year') }}
        AND MONTH([Date]) = {{ var('hms_ntd_reprocess_month') }}
    {% elif is_incremental() %}
        AND YEAR([Date]) = YEAR(GETDATE())
        AND MONTH([Date]) = MONTH(GETDATE())
    {% endif %}
    GROUP BY Vessel, Route, [Date], DayOfWeek, Origin, Trip_Status
),

-- Dedup: multiple Origin/Trip_Status combos can map to the same (IsOutbound, IsMissed).
-- Keep one row per grain key, matching SP's ROW_NUMBER deduplication step.
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY [Date], Vessel, Route, IsOutbound, IsMissed
            ORDER BY [Date] DESC
        ) AS row_num
    FROM trip_summary
)

SELECT
    Vessel,
    Route,
    IsOutbound,
    IsMissed,
    [Date],
    DayOfWeek,
    CAST(TotalTravelMiles AS DECIMAL(12, 2))                               AS TotalTravelMiles,
    TotalTravelMinutes,
    TotalPassengers,
    TotalVehicles
FROM deduplicated
WHERE row_num = 1
