{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sonnell'],
        alias='dbt_SonnellSubsystemOffset',
        dist='ROUND_ROBIN',
        index='CLUSTERED COLUMNSTORE INDEX',
        pre_hook="
            {% if is_incremental() and not var('sonnell_offset_reprocess', false) %}
            UPDATE t
            SET t.CurrentVersion = 0
            FROM {{ this }} AS t
            INNER JOIN {{ source('sonnell', 'SonnellCheckpoints') }} AS b
                ON t.[Year] = YEAR(b.ServiceDate)
                AND t.[Month] = MONTH(b.ServiceDate)
                AND t.Subsystem = b.Subsystem
            WHERE t.Version <> b.Version
                AND t.CurrentVersion = 1
                AND NOT EXISTS (
                    SELECT 1 FROM {{ this }} AS t2
                    WHERE t2.[Year] = YEAR(b.ServiceDate)
                    AND t2.[Month] = MONTH(b.ServiceDate)
                    AND t2.Subsystem = b.Subsystem
                    AND t2.Version = b.Version
                )
            {% elif is_incremental() and var('sonnell_offset_reprocess', false) %}
            DELETE FROM {{ this }}
            WHERE [Month] = {{ var('sonnell_offset_reprocess_month') }}
                AND [Year] = {{ var('sonnell_offset_reprocess_year') }}
            {% endif %}
        ",
        post_hook=[
            "{% if is_incremental() and var('sonnell_offset_reprocess', false) %}
            UPDATE o
            SET o.TotalSubsystemCost = cs.TotalCost
            FROM {{ this }} AS o
            CROSS APPLY (
                SELECT SUM(s.RevenueMilesAmount + s.RevenueHoursAmount) AS TotalCost
                FROM {{ ref('fct_sonnell_subsystem_cost') }} AS s
                WHERE s.GroupId = o.Subsystem
                    AND s.[Year] = o.[Year]
                    AND s.[Month] = o.[Month]
            ) AS cs
            WHERE o.[Year] = {{ var('sonnell_offset_reprocess_year') }}
                AND o.[Month] = {{ var('sonnell_offset_reprocess_month') }}
            {% elif is_incremental() %}
            UPDATE o
            SET o.TotalSubsystemCost = mc.TotalCost
            FROM {{ this }} AS o
            INNER JOIN (
                SELECT [Year], [Month], GroupId,
                    SUM(RevenueMilesAmount + RevenueHoursAmount) AS TotalCost
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE CurrentVersion = 1
                GROUP BY [Year], [Month], GroupId
                HAVING SUM(RevenueMilesAmount + RevenueHoursAmount) > 0
            ) AS mc
                ON o.[Year] = mc.[Year]
                AND o.[Month] = mc.[Month]
                AND o.Subsystem = mc.GroupId
            WHERE o.CurrentVersion = 1
                AND o.TotalSubsystemCost IS NULL
            {% endif %}",
            "{% if is_incremental() and var('sonnell_offset_reprocess', false) %}
            UPDATE o
            SET o.EffectiveRate = b.IncentivesOffsets
            FROM {{ this }} AS o
            INNER JOIN {{ source('sonnell', 'SonnellParameters') }} AS b
                ON o.Subsystem = b.GroupId
                AND (o.OnTimePerformance / 100.0) > b.CheckPointOTPMin
                AND (o.OnTimePerformance / 100.0) <= b.CheckPointOTPMax
            WHERE o.[Year] = {{ var('sonnell_offset_reprocess_year') }}
                AND o.[Month] = {{ var('sonnell_offset_reprocess_month') }}
            {% elif is_incremental() %}
            UPDATE o
            SET o.EffectiveRate = b.IncentivesOffsets
            FROM {{ this }} AS o
            INNER JOIN {{ source('sonnell', 'SonnellParameters') }} AS b
                ON o.Subsystem = b.GroupId
                AND (o.OnTimePerformance / 100.0) > b.CheckPointOTPMin
                AND (o.OnTimePerformance / 100.0) <= b.CheckPointOTPMax
            WHERE o.CurrentVersion = 1
                AND o.EffectiveRate IS NULL
            {% endif %}",
            "{% if is_incremental() and var('sonnell_offset_reprocess', false) %}
            UPDATE o
            SET o.TotalOffset = ROUND(o.TotalSubsystemCost * o.EffectiveRate, 2)
            FROM {{ this }} AS o
            WHERE o.[Year] = {{ var('sonnell_offset_reprocess_year') }}
                AND o.[Month] = {{ var('sonnell_offset_reprocess_month') }}
                AND o.TotalOffset IS NULL
            {% elif is_incremental() %}
            UPDATE o
            SET o.TotalOffset = ROUND(o.TotalSubsystemCost * o.EffectiveRate, 2)
            FROM {{ this }} AS o
            WHERE o.CurrentVersion = 1
                AND o.TotalOffset IS NULL
            {% endif %}"
        ]
    )
}}

{#
  GRAIN:  (Year, Month, Subsystem, Version)
  VERSIONING:  operates at (Year, Month, Subsystem) level — when a new Version
               arrives, ALL prior records for that key are marked CurrentVersion=0.

  EXECUTION MODES:
    1) Daily incremental  (default)                       — inserts new checkpoint aggregations
    2) Monthly reprocess  (sonnell_offset_reprocess vars) — delete+insert for a specific month/year
    3) Full refresh        (--full-refresh)               — rebuilds from all checkpoint history

  INCREMENTAL STRATEGY:
    NOT EXISTS (Year, Month, Subsystem, Version) is the sole dedup mechanism.
    No CreatedAt or closed-month filter is applied — this improves on the SP by
    handling late-arriving data and missed runs without manual intervention.

  DEPENDENCIES:
    fct_sonnell_subsystem_cost (via ref) — dbt ensures costs are processed first.

  COST/RATE/OFFSET LOGIC:
    TotalSubsystemCost, EffectiveRate, and TotalOffset are computed inline via
    LEFT JOINs. post_hooks act as safety nets to back-fill rows whose cost or
    parameter data was missing at insert time.
#}

{% if not is_incremental() %}
{# ── FULL REFRESH: load every checkpoint aggregation, set CurrentVersion by max Version ── #}

WITH checkpoint_raw AS (

    SELECT
        YEAR(c.ServiceDate)                                                    AS [Year],
        MONTH(c.ServiceDate)                                                   AS [Month],
        DATENAME(MONTH, c.ServiceDate)                                         AS MonthName,
        c.Subsystem,
        c.Version,
        COUNT(*)                                                               AS TotalCheckpoints,
        SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END)                    AS TotalCompliant1,
        SUM(CASE WHEN c.Compliant = 'f' THEN 1 ELSE 0 END)                    AS TotalCompliant0,
        CASE
            WHEN COUNT(*) > 0
            THEN ROUND(SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
            ELSE 0
        END                                                                    AS OnTimePerformance
    FROM {{ ref('stg_sonnell_checkpoints') }} AS c
    GROUP BY YEAR(c.ServiceDate), MONTH(c.ServiceDate), DATENAME(MONTH, c.ServiceDate),
             c.Subsystem, c.Version

),

checkpoint_agg AS (

    SELECT
        *,
        MAX(Version) OVER (PARTITION BY [Year], [Month], Subsystem)            AS max_version
    FROM checkpoint_raw

),

monthly_costs AS (

    SELECT
        [Year], [Month], GroupId,
        SUM(RevenueMilesAmount + RevenueHoursAmount)                           AS TotalCost
    FROM {{ ref('fct_sonnell_subsystem_cost') }}
    WHERE CurrentVersion = 1
    GROUP BY [Year], [Month], GroupId

)

SELECT
    CAST(NULL AS INT)                                                          AS Id,
    a.[Year],
    a.[Month],
    CAST(a.MonthName AS NVARCHAR(50))                                          AS MonthName,
    CAST(a.Subsystem AS NVARCHAR(10))                                          AS Subsystem,
    CAST(a.TotalCheckpoints AS BIGINT)                                         AS TotalCheckpoints,
    CAST(a.TotalCompliant1 AS BIGINT)                                          AS TotalCompliant1,
    CAST(a.TotalCompliant0 AS BIGINT)                                          AS TotalCompliant0,
    CAST(a.OnTimePerformance AS DECIMAL(4, 2))                                 AS OnTimePerformance,
    CAST(p.IncentivesOffsets AS DECIMAL(4, 4))                                 AS EffectiveRate,
    CAST(
        CASE WHEN mc.TotalCost > 0 THEN mc.TotalCost ELSE NULL END
        AS DECIMAL(10, 2)
    )                                                                          AS TotalSubsystemCost,
    CAST(
        ROUND(
            CASE WHEN mc.TotalCost > 0 THEN mc.TotalCost ELSE NULL END
            * p.IncentivesOffsets, 2
        )
        AS DECIMAL(10, 4)
    )                                                                          AS TotalOffset,
    CAST(NULL AS NVARCHAR(50))                                                 AS InvoiceId,
    a.Version,
    CASE
        WHEN a.Version = a.max_version THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END                                                                        AS CurrentVersion
FROM checkpoint_agg AS a
LEFT JOIN monthly_costs AS mc
    ON a.[Year] = mc.[Year]
    AND a.[Month] = mc.[Month]
    AND a.Subsystem = mc.GroupId
LEFT JOIN {{ ref('stg_sonnell_parameters') }} AS p
    ON a.Subsystem = p.GroupId
    AND (a.OnTimePerformance / 100.0) > p.CheckPointOTPMin
    AND (a.OnTimePerformance / 100.0) <= p.CheckPointOTPMax


{% elif var('sonnell_offset_reprocess', false) %}
{# ── REPROCESS MODE (SP2+SP3): delete+insert for a given Month/Year.                        ── #}
{# ── pre_hook already DELETEd the month. Costs without CurrentVersion filter matches SP2.    ── #}

WITH checkpoint_agg AS (

    SELECT
        YEAR(c.ServiceDate)                                                    AS [Year],
        MONTH(c.ServiceDate)                                                   AS [Month],
        DATENAME(MONTH, c.ServiceDate)                                         AS MonthName,
        c.Subsystem,
        c.Version,
        COUNT(*)                                                               AS TotalCheckpoints,
        SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END)                    AS TotalCompliant1,
        SUM(CASE WHEN c.Compliant = 'f' THEN 1 ELSE 0 END)                    AS TotalCompliant0,
        CASE
            WHEN COUNT(*) > 0
            THEN ROUND(SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
            ELSE 0
        END                                                                    AS OnTimePerformance
    FROM {{ ref('stg_sonnell_checkpoints') }} AS c
    WHERE YEAR(c.ServiceDate) = {{ var('sonnell_offset_reprocess_year') }}
        AND MONTH(c.ServiceDate) = {{ var('sonnell_offset_reprocess_month') }}
        AND NOT EXISTS (
            SELECT 1 FROM {{ this }} AS o
            WHERE o.[Year] = YEAR(c.ServiceDate)
            AND o.[Month] = MONTH(c.ServiceDate)
            AND o.Subsystem = c.Subsystem
        )
    GROUP BY YEAR(c.ServiceDate), MONTH(c.ServiceDate), DATENAME(MONTH, c.ServiceDate),
             c.Subsystem, c.Version

),

monthly_costs AS (

    SELECT
        [Year], [Month], GroupId,
        SUM(RevenueMilesAmount + RevenueHoursAmount)                           AS TotalCost
    FROM {{ ref('fct_sonnell_subsystem_cost') }}
    GROUP BY [Year], [Month], GroupId

)

SELECT
    CAST(NULL AS INT)                                                          AS Id,
    a.[Year],
    a.[Month],
    CAST(a.MonthName AS NVARCHAR(50))                                          AS MonthName,
    CAST(a.Subsystem AS NVARCHAR(10))                                          AS Subsystem,
    CAST(a.TotalCheckpoints AS BIGINT)                                         AS TotalCheckpoints,
    CAST(a.TotalCompliant1 AS BIGINT)                                          AS TotalCompliant1,
    CAST(a.TotalCompliant0 AS BIGINT)                                          AS TotalCompliant0,
    CAST(a.OnTimePerformance AS DECIMAL(4, 2))                                 AS OnTimePerformance,
    CAST(p.IncentivesOffsets AS DECIMAL(4, 4))                                 AS EffectiveRate,
    CAST(mc.TotalCost AS DECIMAL(10, 2))                                       AS TotalSubsystemCost,
    CAST(
        ROUND(mc.TotalCost * p.IncentivesOffsets, 2)
        AS DECIMAL(10, 4)
    )                                                                          AS TotalOffset,
    CAST(NULL AS NVARCHAR(50))                                                 AS InvoiceId,
    a.Version,
    CAST(1 AS BIT)                                                             AS CurrentVersion
FROM checkpoint_agg AS a
LEFT JOIN monthly_costs AS mc
    ON a.[Year] = mc.[Year]
    AND a.[Month] = mc.[Month]
    AND a.Subsystem = mc.GroupId
LEFT JOIN {{ ref('stg_sonnell_parameters') }} AS p
    ON a.Subsystem = p.GroupId
    AND (a.OnTimePerformance / 100.0) > p.CheckPointOTPMin
    AND (a.OnTimePerformance / 100.0) <= p.CheckPointOTPMax


{% else %}
{# ── DAILY INCREMENTAL (SP1): insert new checkpoint aggregations not yet in target.          ── #}
{# ── pre_hook already set CurrentVersion=0 on superseded records.                            ── #}
{# ── NOT EXISTS (Year, Month, Subsystem, Version) is the primary dedup.                      ── #}

WITH checkpoint_agg AS (

    SELECT
        YEAR(c.ServiceDate)                                                    AS [Year],
        MONTH(c.ServiceDate)                                                   AS [Month],
        DATENAME(MONTH, c.ServiceDate)                                         AS MonthName,
        c.Subsystem,
        c.Version,
        COUNT(*)                                                               AS TotalCheckpoints,
        SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END)                    AS TotalCompliant1,
        SUM(CASE WHEN c.Compliant = 'f' THEN 1 ELSE 0 END)                    AS TotalCompliant0,
        CASE
            WHEN COUNT(*) > 0
            THEN ROUND(SUM(CASE WHEN c.Compliant = 't' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
            ELSE 0
        END                                                                    AS OnTimePerformance
    FROM {{ ref('stg_sonnell_checkpoints') }} AS c
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS o
        WHERE o.[Year] = YEAR(c.ServiceDate)
        AND o.[Month] = MONTH(c.ServiceDate)
        AND o.Subsystem = c.Subsystem
        AND o.Version = c.Version
    )
    GROUP BY YEAR(c.ServiceDate), MONTH(c.ServiceDate), DATENAME(MONTH, c.ServiceDate),
             c.Subsystem, c.Version

),

monthly_costs AS (

    SELECT
        [Year], [Month], GroupId,
        SUM(RevenueMilesAmount + RevenueHoursAmount)                           AS TotalCost
    FROM {{ ref('fct_sonnell_subsystem_cost') }}
    WHERE CurrentVersion = 1
    GROUP BY [Year], [Month], GroupId

)

SELECT
    CAST(NULL AS INT)                                                          AS Id,
    a.[Year],
    a.[Month],
    CAST(a.MonthName AS NVARCHAR(50))                                          AS MonthName,
    CAST(a.Subsystem AS NVARCHAR(10))                                          AS Subsystem,
    CAST(a.TotalCheckpoints AS BIGINT)                                         AS TotalCheckpoints,
    CAST(a.TotalCompliant1 AS BIGINT)                                          AS TotalCompliant1,
    CAST(a.TotalCompliant0 AS BIGINT)                                          AS TotalCompliant0,
    CAST(a.OnTimePerformance AS DECIMAL(4, 2))                                 AS OnTimePerformance,
    CAST(p.IncentivesOffsets AS DECIMAL(4, 4))                                 AS EffectiveRate,
    CAST(
        CASE WHEN mc.TotalCost > 0 THEN mc.TotalCost ELSE NULL END
        AS DECIMAL(10, 2)
    )                                                                          AS TotalSubsystemCost,
    CAST(
        ROUND(
            CASE WHEN mc.TotalCost > 0 THEN mc.TotalCost ELSE NULL END
            * p.IncentivesOffsets, 2
        )
        AS DECIMAL(10, 4)
    )                                                                          AS TotalOffset,
    CAST(NULL AS NVARCHAR(50))                                                 AS InvoiceId,
    a.Version,
    CAST(1 AS BIT)                                                             AS CurrentVersion
FROM checkpoint_agg AS a
LEFT JOIN monthly_costs AS mc
    ON a.[Year] = mc.[Year]
    AND a.[Month] = mc.[Month]
    AND a.Subsystem = mc.GroupId
LEFT JOIN {{ ref('stg_sonnell_parameters') }} AS p
    ON a.Subsystem = p.GroupId
    AND (a.OnTimePerformance / 100.0) > p.CheckPointOTPMin
    AND (a.OnTimePerformance / 100.0) <= p.CheckPointOTPMax

{% endif %}
