{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sonnell'],
        alias='dbt_SonnellSubsystemCost',
        dist='ROUND_ROBIN',
        index='CLUSTERED COLUMNSTORE INDEX',
        pre_hook="
            {% if is_incremental() and not var('sonnell_reprocess', false) %}
            UPDATE t
            SET t.CurrentVersion = 0
            FROM {{ this }} AS t
            INNER JOIN {{ source('sonnell', 'SonnellDailySummary') }} AS b
                ON t.ServiceDate = b.ServiceDate
            WHERE t.Version <> b.Version
                AND t.CurrentVersion = 1
                AND NOT EXISTS (
                    SELECT 1 FROM {{ this }} AS t2
                    WHERE t2.ServiceDate = b.ServiceDate
                    AND t2.Version = b.Version
                )
            {% elif is_incremental() and var('sonnell_reprocess', false) %}
            DELETE FROM {{ this }}
            WHERE [Month] = {{ var('sonnell_reprocess_month') }}
                AND [Year] = {{ var('sonnell_reprocess_year') }}
            {% endif %}
        ",
        post_hook=[
            "{% if is_incremental() and var('sonnell_reprocess', false) %}
            UPDATE t
            SET t.EffectiveRateMiles = r.RateMiles,
                t.EffectiveRateHours = r.RateHours
            FROM {{ this }} AS t
            INNER JOIN {{ source('sonnell', 'SonnellRates') }} AS r
                ON t.GroupId = r.GroupId
                AND r.IsActive = 1
            WHERE t.ServiceDate BETWEEN r.StartDate AND r.EndDate
                AND t.[Month] = {{ var('sonnell_reprocess_month') }}
                AND t.[Year] = {{ var('sonnell_reprocess_year') }}
                AND (t.RevenueHoursAmount IS NULL OR t.RevenueHoursAmount = 0)
                AND (t.EffectiveRateHours IS NULL OR t.EffectiveRateMiles IS NULL)
            {% elif is_incremental() %}
            UPDATE t
            SET t.EffectiveRateMiles = r.RateMiles,
                t.EffectiveRateHours = r.RateHours
            FROM {{ this }} AS t
            INNER JOIN {{ source('sonnell', 'SonnellRates') }} AS r
                ON t.GroupId = r.GroupId
            WHERE t.CurrentVersion = 1
                AND t.ServiceDate BETWEEN r.StartDate AND r.EndDate
                AND (t.RevenueHoursAmount IS NULL OR t.RevenueHoursAmount = 0)
                AND (t.EffectiveRateHours IS NULL OR t.EffectiveRateMiles IS NULL)
            {% endif %}",
            "{% if is_incremental() and var('sonnell_reprocess', false) %}
            UPDATE t
            SET t.RevenueMilesAmount = ROUND(t.RevenueMiles * t.EffectiveRateMiles, 2),
                t.RevenueHoursAmount = ROUND(t.RevenueHours * t.EffectiveRateHours, 2)
            FROM {{ this }} AS t
            WHERE t.[Month] = {{ var('sonnell_reprocess_month') }}
                AND t.[Year] = {{ var('sonnell_reprocess_year') }}
                AND (t.RevenueMilesAmount IS NULL OR t.RevenueHoursAmount IS NULL)
            {% elif is_incremental() %}
            UPDATE t
            SET t.RevenueMilesAmount = ROUND(t.RevenueMiles * t.EffectiveRateMiles, 2),
                t.RevenueHoursAmount = ROUND(t.RevenueHours * t.EffectiveRateHours, 2)
            FROM {{ this }} AS t
            WHERE t.CurrentVersion = 1
                AND (t.RevenueMilesAmount IS NULL OR t.RevenueHoursAmount IS NULL)
            {% endif %}"
        ]
    )
}}

{#
  GRAIN:  DailySummaryId  (= SonnellDailySummary.Id, one row per source record)
  VERSIONING:  operates at ServiceDate level — when a new Version arrives for a
               ServiceDate, ALL prior records for that date are marked CurrentVersion=0.

  EXECUTION MODES:
    1) Daily incremental  (default)        — inserts any DailySummary not yet in target
    2) Monthly reprocess  (sonnell_reprocess vars) — delete+insert for a specific month/year
    3) Full refresh        (--full-refresh)  — rebuilds from all DailySummary history

  INCREMENTAL STRATEGY:
    The daily mode uses NOT EXISTS (ServiceDate, Version) as the sole dedup mechanism.
    No CreatedAt or closed-month filter is applied — this improves on the SP by handling
    late-arriving data, missed runs, and current-month data without manual intervention.
    The pre_hook only marks old versions when the replacement hasn't been inserted yet.

  RATE/AMOUNT LOGIC:
    Rates and amounts are computed inline via LEFT JOIN with SonnellRates.
    post_hooks act as a safety net to back-fill rows whose rate was missing at
    insert time but was added to SonnellRates later (replicates SP Steps 3 & 4).
#}

{% if not is_incremental() %}
{# ── FULL REFRESH: load every DailySummary record, set CurrentVersion by max Version per ServiceDate ── #}

WITH source_data AS (

    SELECT
        s.Id,
        s.ServiceDate,
        s.GroupId,
        s.RouteId,
        s.NumTrips,
        s.RevenueHours,
        s.RevenueMiles,
        s.Version,
        r.RateMiles,
        r.RateHours,
        MAX(s.Version) OVER (PARTITION BY s.ServiceDate) AS max_version
    FROM {{ ref('stg_sonnell_daily_summary') }} AS s
    LEFT JOIN {{ ref('stg_sonnell_rates') }} AS r
        ON s.GroupId = r.GroupId
        AND s.ServiceDate BETWEEN r.StartDate AND r.EndDate

)

SELECT
    CAST(Id AS INT)                                                     AS Id,
    CAST(Id AS INT)                                                     AS DailySummaryId,
    ServiceDate,
    YEAR(ServiceDate)                                                   AS [Year],
    MONTH(ServiceDate)                                                  AS [Month],
    CAST(DATENAME(MONTH, ServiceDate) AS NVARCHAR(50))                  AS MonthName,
    CAST(GroupId AS NVARCHAR(50))                                       AS GroupId,
    CAST(RouteId AS NVARCHAR(10))                                       AS RouteId,
    NumTrips,
    CAST(RevenueHours AS DECIMAL(18, 2))                                AS RevenueHours,
    CAST(RevenueMiles AS DECIMAL(18, 2))                                AS RevenueMiles,
    CAST(ROUND(RevenueHours * RateHours, 2) AS DECIMAL(18, 2))         AS RevenueHoursAmount,
    CAST(ROUND(RevenueMiles * RateMiles, 2) AS DECIMAL(18, 2))         AS RevenueMilesAmount,
    CAST(RateMiles AS DECIMAL(5, 2))                                    AS EffectiveRateMiles,
    CAST(RateHours AS DECIMAL(5, 2))                                    AS EffectiveRateHours,
    CAST(NULL AS NVARCHAR(50))                                          AS InvoiceId,
    CAST(SYSDATETIME() AS DATETIME)                                     AS CreatedAt,
    Version,
    CASE
        WHEN Version = max_version THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END                                                                 AS CurrentVersion
FROM source_data


{% elif var('sonnell_reprocess', false) %}
{# ── REPROCESS MODE (SP2): delete+insert for a given Month/Year.                         ── #}
{# ── pre_hook already DELETEd the month. IsActive=1 on rates matches SP2 exactly.         ── #}

SELECT
    CAST(NULL AS INT)                                                   AS Id,
    CAST(s.Id AS INT)                                                   AS DailySummaryId,
    s.ServiceDate,
    YEAR(s.ServiceDate)                                                 AS [Year],
    MONTH(s.ServiceDate)                                                AS [Month],
    CAST(DATENAME(MONTH, s.ServiceDate) AS NVARCHAR(50))                AS MonthName,
    CAST(s.GroupId AS NVARCHAR(50))                                     AS GroupId,
    CAST(s.RouteId AS NVARCHAR(10))                                     AS RouteId,
    s.NumTrips,
    CAST(s.RevenueHours AS DECIMAL(18, 2))                              AS RevenueHours,
    CAST(s.RevenueMiles AS DECIMAL(18, 2))                              AS RevenueMiles,
    CAST(ROUND(s.RevenueHours * r.RateHours, 2) AS DECIMAL(18, 2))     AS RevenueHoursAmount,
    CAST(ROUND(s.RevenueMiles * r.RateMiles, 2) AS DECIMAL(18, 2))     AS RevenueMilesAmount,
    CAST(r.RateMiles AS DECIMAL(5, 2))                                  AS EffectiveRateMiles,
    CAST(r.RateHours AS DECIMAL(5, 2))                                  AS EffectiveRateHours,
    CAST(NULL AS NVARCHAR(50))                                          AS InvoiceId,
    CAST(SYSDATETIME() AS DATETIME)                                     AS CreatedAt,
    s.Version,
    CAST(1 AS BIT)                                                      AS CurrentVersion
FROM {{ ref('stg_sonnell_daily_summary') }} AS s
LEFT JOIN {{ ref('stg_sonnell_rates') }} AS r
    ON s.GroupId = r.GroupId
    AND s.ServiceDate BETWEEN r.StartDate AND r.EndDate
    AND r.IsActive = 1
WHERE MONTH(s.ServiceDate) = {{ var('sonnell_reprocess_month') }}
    AND YEAR(s.ServiceDate) = {{ var('sonnell_reprocess_year') }}
    AND NOT EXISTS (
        SELECT 1 FROM {{ this }} AS t
        WHERE t.DailySummaryId = CAST(s.Id AS INT)
    )


{% else %}
{# ── DAILY INCREMENTAL (SP1): insert any DailySummary records not yet in target.           ── #}
{# ── pre_hook already set CurrentVersion=0 on superseded records.                          ── #}
{# ── NOT EXISTS (ServiceDate, Version) is the primary dedup — no date restriction needed.  ── #}
{# ── No IsActive filter on rates — matches SP1.                                           ── #}

SELECT
    CAST(s.Id AS INT)                                                   AS Id,
    CAST(s.Id AS INT)                                                   AS DailySummaryId,
    s.ServiceDate,
    YEAR(s.ServiceDate)                                                 AS [Year],
    MONTH(s.ServiceDate)                                                AS [Month],
    CAST(DATENAME(MONTH, s.ServiceDate) AS NVARCHAR(50))                AS MonthName,
    CAST(s.GroupId AS NVARCHAR(50))                                     AS GroupId,
    CAST(s.RouteId AS NVARCHAR(10))                                     AS RouteId,
    s.NumTrips,
    CAST(s.RevenueHours AS DECIMAL(18, 2))                              AS RevenueHours,
    CAST(s.RevenueMiles AS DECIMAL(18, 2))                              AS RevenueMiles,
    CAST(ROUND(s.RevenueHours * r.RateHours, 2) AS DECIMAL(18, 2))     AS RevenueHoursAmount,
    CAST(ROUND(s.RevenueMiles * r.RateMiles, 2) AS DECIMAL(18, 2))     AS RevenueMilesAmount,
    CAST(r.RateMiles AS DECIMAL(5, 2))                                  AS EffectiveRateMiles,
    CAST(r.RateHours AS DECIMAL(5, 2))                                  AS EffectiveRateHours,
    CAST(NULL AS NVARCHAR(50))                                          AS InvoiceId,
    CAST(SYSDATETIME() AS DATETIME)                                     AS CreatedAt,
    s.Version,
    CAST(1 AS BIT)                                                      AS CurrentVersion
FROM {{ ref('stg_sonnell_daily_summary') }} AS s
LEFT JOIN {{ ref('stg_sonnell_rates') }} AS r
    ON s.GroupId = r.GroupId
    AND s.ServiceDate BETWEEN r.StartDate AND r.EndDate
WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS t
        WHERE t.ServiceDate = s.ServiceDate
        AND t.Version = s.Version
    )

{% endif %}
