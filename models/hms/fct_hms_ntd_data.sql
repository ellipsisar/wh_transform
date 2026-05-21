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
-- aggregates directly to grain (Vessel, Route, Date, IsOutbound, IsMissed).
-- MERGE replaced with DELETE+INSERT (Synapse incremental pattern).
-- Default window: current month. Use hms_ntd_reprocess vars for specific months.
-- FIX: single-pass aggregation eliminates non-deterministic dedup bug.

SELECT
    Vessel,
    Route,
    IsOutbound,
    IsMissed,
    [Date],
    MAX(DayOfWeek)                                         AS DayOfWeek,
    CAST(ISNULL(SUM(TotalTravelMiles), 0) AS DECIMAL(12,2)) AS TotalTravelMiles,
    ISNULL(SUM(TotalTravelMinutes), 0)                     AS TotalTravelMinutes,
    ISNULL(SUM(TotalPassengers), 0)                        AS TotalPassengers,
    ISNULL(SUM(TotalVehicles), 0)                          AS TotalVehicles
FROM (
    SELECT
        CAST(Vessel          AS VARCHAR(255))              AS Vessel,
        CAST(Route           AS VARCHAR(255))              AS Route,
        CAST([Date]          AS DATE)                      AS [Date],
        DayOfWeek,
        CASE
            WHEN LOWER(LTRIM(RTRIM(Origin))) NOT IN ('vieques', 'culebra')
            THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT)
        END                                                AS IsOutbound,
        CASE
            WHEN Trip_Status IS NULL
                OR LTRIM(RTRIM(Trip_Status)) = ''
                OR Trip_Status = 'MISSED TRIP'
            THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT)
        END                                                AS IsMissed,
        ISNULL(Travel_Time_Minutes, 0)                     AS TotalTravelMinutes,
        ISNULL(TRY_CAST(REPLACE(CAST(Miles AS VARCHAR(50)), ',', '.') AS DECIMAL(12,2)), 0) AS TotalTravelMiles,
        ISNULL(TRY_CAST(PAX AS INT), 0)                   AS TotalPassengers,
        ISNULL(TRY_CAST(Vehicles AS INT), 0)              AS TotalVehicles
    FROM {{ ref('fct_hms_monthly_trips') }}
    WHERE [Date]        IS NOT NULL
        AND Origin      IS NOT NULL
        AND Destination IS NOT NULL
    {% if is_incremental() and var('hms_ntd_reprocess', false) %}
        AND YEAR([Date]) = {{ var('hms_ntd_reprocess_year') }}
        AND MONTH([Date]) = {{ var('hms_ntd_reprocess_month') }}
    {% elif is_incremental() %}
        AND YEAR([Date]) = YEAR(GETDATE())
        AND MONTH([Date]) = MONTH(GETDATE())
    {% endif %}
) AS trip_data
GROUP BY Vessel, Route, [Date], IsOutbound, IsMissed
