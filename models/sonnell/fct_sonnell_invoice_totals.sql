{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sonnell'],
        alias='dbt_SonnellInvoiceTotals',
        dist='ROUND_ROBIN',
        index='CLUSTERED COLUMNSTORE INDEX',
        pre_hook="
            {% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE t
            SET t.CurrentVersion = 0
            FROM {{ this }} AS t
            INNER JOIN {{ ref('fct_sonnell_subsystem_offset') }} AS o
                ON t.[Year] = o.[Year]
                AND t.[Month] = o.[Month]
            WHERE t.Version <> o.Version
                AND t.CurrentVersion = 1
                AND o.CurrentVersion = 1
                AND NOT EXISTS (
                    SELECT 1 FROM {{ this }} AS t2
                    WHERE t2.[Year] = o.[Year]
                    AND t2.[Month] = o.[Month]
                    AND t2.Version = o.Version
                )
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            DELETE FROM {{ this }}
            WHERE [Type] IN ('Regular', 'Offset')
                AND [Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}
        ",
        post_hook=[
            "{% if is_incremental() and var('sonnell_invoice_reprocess', false) %}
            ;WITH SingleID AS (SELECT NEWID() AS InvoiceID)
            UPDATE t
            SET t.InvoiceID = s.InvoiceID
            FROM {{ this }} AS t
            CROSS JOIN SingleID AS s
            WHERE t.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND t.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
                AND t.InvoiceID IS NULL
            {% elif is_incremental() %}
            ;WITH YearMonthGroups AS (
                SELECT DISTINCT [Year], [Month], NEWID() AS GroupUUID
                FROM {{ this }}
            )
            UPDATE t
            SET t.InvoiceID = ymg.GroupUUID
            FROM {{ this }} AS t
            INNER JOIN YearMonthGroups AS ymg
                ON t.[Year] = ymg.[Year]
                AND t.[Month] = ymg.[Month]
            WHERE t.CurrentVersion = 1
                AND t.InvoiceID IS NULL
            {% endif %}",
            "{% if is_incremental() and var('sonnell_invoice_reprocess', false) %}
            DELETE FROM {{ source('sonnell', 'SonnellOffsetApplied') }}
            WHERE [Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}",
            "{% if is_incremental() %}
            ;WITH REGULAR AS (
                SELECT [Year], [Month], MonthName, Subsystem,
                       SUM(Total) AS TotalInvoiceRegular,
                       SUM(Total) * 0.04 AS FourPercentRegularTotal
                FROM {{ this }}
                WHERE [Type] = 'Regular' AND CurrentVersion = 1
                GROUP BY [Year], [Month], MonthName, Subsystem
            ),
            OFFSET_TOTALS AS (
                SELECT [Year], [Month], Subsystem,
                       SUM(Total) AS TotalInvoiceOffset
                FROM {{ this }}
                WHERE [Type] = 'Offset' AND CurrentVersion = 1
                GROUP BY [Year], [Month], Subsystem
            ),
            PERCENTAGE AS (
                SELECT
                    R.[Year], R.[Month], R.MonthName, R.Subsystem,
                    R.TotalInvoiceRegular, R.FourPercentRegularTotal,
                    O.TotalInvoiceOffset AS TotalCalculatedOffset,
                    (O.TotalInvoiceOffset / R.TotalInvoiceRegular) * 100 AS CalculatedOffsetPercentage,
                    CASE
                        WHEN O.TotalInvoiceOffset IS NULL THEN 0
                        WHEN O.TotalInvoiceOffset > R.FourPercentRegularTotal THEN 1
                        ELSE 0
                    END AS ApplySuggestedOffset,
                    CASE
                        WHEN O.TotalInvoiceOffset IS NULL THEN NULL
                        WHEN O.TotalInvoiceOffset > R.FourPercentRegularTotal THEN R.FourPercentRegularTotal
                        ELSE O.TotalInvoiceOffset
                    END AS TotalInvoiceSuggestedOffset
                FROM REGULAR R
                LEFT JOIN OFFSET_TOTALS O
                    ON R.[Year] = O.[Year] AND R.[Month] = O.[Month] AND R.Subsystem = O.Subsystem
            )
            INSERT INTO {{ source('sonnell', 'SonnellOffsetApplied') }}
                (InvoiceID, [Year], [Month], MonthName, Subsystem, TotalInvoiceRegular,
                 PercentageRegular, TotalCalculatedOffset, CalculatedOffsetPercentage,
                 ApplySuggestedOffset, TotalInvoiceSuggestedOffset)
            SELECT
                IT.InvoiceID, P.[Year], P.[Month], P.MonthName, P.Subsystem,
                P.TotalInvoiceRegular, P.FourPercentRegularTotal, P.TotalCalculatedOffset,
                CAST(P.CalculatedOffsetPercentage AS DECIMAL(10,2)),
                P.ApplySuggestedOffset, P.TotalInvoiceSuggestedOffset
            FROM PERCENTAGE P
            JOIN (
                SELECT DISTINCT [Year], [Month], InvoiceID
                FROM {{ this }}
                WHERE CurrentVersion = 1 AND InvoiceID IS NOT NULL
            ) IT ON P.[Year] = IT.[Year] AND P.[Month] = IT.[Month]
            WHERE NOT EXISTS (
                SELECT 1 FROM {{ source('sonnell', 'SonnellOffsetApplied') }} AS ex
                WHERE ex.[Year] = P.[Year] AND ex.[Month] = P.[Month]
                    AND ex.Subsystem = P.Subsystem AND ex.InvoiceID = IT.InvoiceID
            )
            {% endif %}",
            "{% if is_incremental() and var('sonnell_invoice_reprocess', false) %}
            ;WITH InvID AS (
                SELECT TOP 1 InvoiceID FROM {{ this }}
                WHERE [Month] = {{ var('sonnell_invoice_reprocess_month') }}
                    AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}
                    AND InvoiceID IS NOT NULL
            )
            UPDATE o SET o.InvoiceId = i.InvoiceID
            FROM {{ ref('fct_sonnell_subsystem_offset') }} AS o
            CROSS JOIN InvID AS i
            WHERE o.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND o.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
                AND o.CurrentVersion = 1
                AND o.InvoiceId IS NULL
            {% elif is_incremental() %}
            UPDATE o SET o.InvoiceId = src.InvoiceID
            FROM {{ ref('fct_sonnell_subsystem_offset') }} AS o
            INNER JOIN {{ this }} AS src
                ON o.[Year] = src.[Year] AND o.[Month] = src.[Month]
                AND o.Subsystem = src.Subsystem
            WHERE o.CurrentVersion = 1 AND src.CurrentVersion = 1
                AND o.InvoiceId IS NULL
            {% endif %}",
            "{% if is_incremental() and var('sonnell_invoice_reprocess', false) %}
            ;WITH InvID AS (
                SELECT TOP 1 InvoiceID FROM {{ this }}
                WHERE [Month] = {{ var('sonnell_invoice_reprocess_month') }}
                    AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}
                    AND InvoiceID IS NOT NULL
            )
            UPDATE c SET c.InvoiceId = i.InvoiceID
            FROM {{ ref('fct_sonnell_subsystem_cost') }} AS c
            CROSS JOIN InvID AS i
            WHERE c.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND c.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
                AND c.CurrentVersion = 1
                AND c.InvoiceId IS NULL
            {% elif is_incremental() %}
            UPDATE c SET c.InvoiceId = src.InvoiceID
            FROM {{ ref('fct_sonnell_subsystem_cost') }} AS c
            INNER JOIN {{ this }} AS src
                ON c.[Year] = src.[Year] AND c.[Month] = src.[Month]
                AND c.GroupId = src.Subsystem
            WHERE c.CurrentVersion = 1 AND src.CurrentVersion = 1
                AND c.InvoiceId IS NULL
            {% endif %}",
            "{% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE inv SET inv.Trips = agg.TotalTrips
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], GroupId, SUM(NumTrips) AS TotalTrips
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE CurrentVersion = 1 AND GroupId IN ('MB', 'TC')
                GROUP BY [Year], [Month], GroupId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Subsystem = agg.GroupId
            WHERE inv.[Type] = 'Regular' AND inv.CurrentVersion = 1 AND inv.Trips IS NULL
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            UPDATE inv SET inv.Trips = agg.TotalTrips
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], GroupId, SUM(NumTrips) AS TotalTrips
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE GroupId IN ('MB', 'TC')
                GROUP BY [Year], [Month], GroupId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Subsystem = agg.GroupId
            WHERE inv.[Type] = 'Regular'
                AND inv.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND inv.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}",
            "{% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE inv SET inv.Trips = agg.TotalTrips
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], RouteId, SUM(NumTrips) AS TotalTrips
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE CurrentVersion = 1 AND RouteId IN ('E20', '20', 'E30', '30')
                GROUP BY [Year], [Month], RouteId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Routes = agg.RouteId
            WHERE inv.[Type] = 'Regular' AND inv.Routes IN ('E20', '20', 'E30', '30')
                AND inv.CurrentVersion = 1 AND inv.Trips IS NULL
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            UPDATE inv SET inv.Trips = agg.TotalTrips
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], RouteId, SUM(NumTrips) AS TotalTrips
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE RouteId IN ('E20', '20', 'E30', '30')
                GROUP BY [Year], [Month], RouteId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Routes = agg.RouteId
            WHERE inv.[Type] = 'Regular' AND inv.Routes IN ('E20', '20', 'E30', '30')
                AND inv.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND inv.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}",
            "{% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE inv
            SET inv.RevenueMiles = agg.TotalRevenueMiles, inv.RevenueHours = agg.TotalRevenueHours
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], GroupId,
                       SUM(RevenueMiles) AS TotalRevenueMiles, SUM(RevenueHours) AS TotalRevenueHours
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE CurrentVersion = 1 AND GroupId IN ('MB', 'TC')
                GROUP BY [Year], [Month], GroupId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Subsystem = agg.GroupId
            WHERE inv.[Type] = 'Regular' AND inv.Subsystem IN ('MB', 'TC') AND inv.CurrentVersion = 1
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            UPDATE inv
            SET inv.RevenueMiles = agg.TotalRevenueMiles, inv.RevenueHours = agg.TotalRevenueHours
            FROM {{ this }} AS inv
            INNER JOIN (
                SELECT [Year], [Month], GroupId,
                       SUM(RevenueMiles) AS TotalRevenueMiles, SUM(RevenueHours) AS TotalRevenueHours
                FROM {{ ref('fct_sonnell_subsystem_cost') }}
                WHERE GroupId IN ('MB', 'TC')
                GROUP BY [Year], [Month], GroupId
            ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
                AND inv.Subsystem = agg.GroupId
            WHERE inv.[Type] = 'Regular' AND inv.Subsystem IN ('MB', 'TC')
                AND inv.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND inv.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}",
            "{% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE inv
            SET inv.CompliantCheckpoints = o.TotalCompliant1,
                inv.NonCompliantCheckpoints = o.TotalCompliant0
            FROM {{ this }} AS inv
            INNER JOIN {{ ref('fct_sonnell_subsystem_offset') }} AS o
                ON inv.[Year] = o.[Year] AND inv.[Month] = o.[Month]
                AND inv.Subsystem = o.Subsystem
            WHERE inv.[Type] = 'Offset' AND inv.CurrentVersion = 1 AND o.CurrentVersion = 1
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            UPDATE inv
            SET inv.CompliantCheckpoints = o.TotalCompliant1,
                inv.NonCompliantCheckpoints = o.TotalCompliant0
            FROM {{ this }} AS inv
            INNER JOIN {{ ref('fct_sonnell_subsystem_offset') }} AS o
                ON inv.[Year] = o.[Year] AND inv.[Month] = o.[Month]
                AND inv.Subsystem = o.Subsystem
            WHERE inv.[Type] = 'Offset'
                AND inv.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND inv.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}",
            "{% if is_incremental() and not var('sonnell_invoice_reprocess', false) %}
            UPDATE {{ this }}
            SET CreatedAt = GETDATE(), UpdatedAt = GETDATE()
            WHERE CurrentVersion = 1 AND CreatedAt IS NULL
            {% elif is_incremental() and var('sonnell_invoice_reprocess', false) %}
            UPDATE {{ this }}
            SET CreatedAt = GETDATE(), UpdatedAt = GETDATE()
            WHERE [Month] = {{ var('sonnell_invoice_reprocess_month') }}
                AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}
            {% endif %}"
        ]
    )
}}

{#
  GRAIN:  (Year, Month, Subsystem, Type, Version)
  VERSIONING:  operates at (Year, Month) level — when a new Version arrives,
               ALL prior invoice lines for that period are marked CurrentVersion=0.

  EXECUTION MODES:
    1) Full refresh         (--full-refresh)               — rebuilds from all offset/cost/rate history
    2) Daily incremental    (default)                      — inserts new invoice lines for closed months
    3) Monthly reprocess    (sonnell_invoice_reprocess vars) — delete+insert for a specific month/year

  3 UNION ALL BLOCKS (per mode):
    Block 1 — Regular MB/TC: one line per subsystem per month from offset + cost
    Block 2 — Regular MU:    one line per route ('20','30') recalculated from DailySummary × Rates
    Block 3 — Offset:        one line per subsystem per month from offset table

  DEPENDENCIES:
    fct_sonnell_subsystem_offset (ref), fct_sonnell_subsystem_cost (ref),
    SonnellDailySummary (source), SonnellRates (source) — for MU recalculation.
#}


{% if not is_incremental() %}
{# ── FULL REFRESH: all offset/cost history, MAX(Version) for CurrentVersion ── #}

WITH mb_tc_inner AS (

    SELECT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        A.TotalSubsystemCost, A.TotalOffset,
        SUM(B.RevenueMiles) AS RevenueMiles,
        B.EffectiveRateMiles,
        SUM(B.RevenueHours) AS RevenueHours,
        B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem IN ('MB', 'TC')
        AND B.CurrentVersion = 1
    GROUP BY A.[Year], A.[Month], A.MonthName, A.Subsystem, A.TotalSubsystemCost,
             A.TotalOffset, B.EffectiveRateMiles, B.EffectiveRateHours, A.Version, B.Version

),

mu_inner AS (

    SELECT DISTINCT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        B.RouteId,
        A.TotalSubsystemCost, A.TotalOffset,
        B.EffectiveRateMiles, B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem = 'MU'
        AND B.CurrentVersion = 1

),

mu_rates AS (

    SELECT
        [Year], [Month], MonthName, Subsystem, RouteId,
        SUM(RevenueMiles) AS RevenueMiles,
        SUM(RevenueHours) AS RevenueHours,
        CAST(SUM(RevenueMilesAmount) AS DECIMAL(10,2)) AS RevenueMilesAmount,
        CAST(SUM(RevenueHoursAmount) AS DECIMAL(10,2)) AS RevenueHoursAmount,
        EffectiveRateHours, EffectiveRateMiles
    FROM (
        SELECT
            YEAR(t1.ServiceDate) AS [Year],
            MONTH(t1.ServiceDate) AS [Month],
            DATENAME(MONTH, t1.ServiceDate) AS MonthName,
            t1.Subsystem, t1.RouteId,
            ROUND(SUM(t1.RevenueMeters) / 1609.34, 2) AS RevenueMiles,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0, 2) AS RevenueHours,
            ROUND(SUM(t1.RevenueMeters) / 1609.34 * t2.RateMiles, 2) AS RevenueMilesAmount,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0 * t2.RateHours, 2) AS RevenueHoursAmount,
            t2.RateHours AS EffectiveRateHours,
            t2.RateMiles AS EffectiveRateMiles
        FROM {{ source('sonnell', 'SonnellDailySummary') }} AS t1
        INNER JOIN {{ source('sonnell', 'SonnellRates') }} AS t2
            ON t1.Subsystem = t2.GroupId
        WHERE t1.ServiceDate BETWEEN t2.StartDate AND t2.EndDate
            AND t1.Subsystem = 'MU'
        GROUP BY YEAR(t1.ServiceDate), MONTH(t1.ServiceDate), DATENAME(MONTH, t1.ServiceDate),
                 t1.Subsystem, t1.RouteId, t2.RateHours, t2.RateMiles
    ) P1
    GROUP BY [Year], [Month], MonthName, Subsystem, RouteId, EffectiveRateHours, EffectiveRateMiles

),

all_invoice AS (

    SELECT
        t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)) AS MonthName,
        CAST(t1.Subsystem AS NVARCHAR(50)) AS Subsystem,
        CAST('-' AS NVARCHAR(50)) AS Routes,
        CAST(CONCAT(t1.Subsystem, ' ', t1.MonthName, 'All', t1.[Year], ' (',
            CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)), ' miles @ $', t2.EffectiveRateMiles,
            '; ', CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)), ' hours @ $', t2.EffectiveRateHours, ')')
            AS NVARCHAR(255)) AS Description,
        CAST(t1.TotalSubsystemCost AS DECIMAL(10,2)) AS Total,
        CAST(NULL AS DECIMAL(10,2)) AS OffsetPercentage,
        CAST('Regular' AS NVARCHAR(50)) AS [Type],
        CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)) AS RevenueMiles,
        CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)) AS RevenueHours,
        CAST(t2.EffectiveRateMiles AS DECIMAL(10,2)) AS MilesRate,
        CAST(t2.EffectiveRateHours AS DECIMAL(10,2)) AS HoursRate,
        CAST(NULL AS INT) AS Checkpoints,
        CAST(NULL AS INT) AS CompliantCheckpoints,
        CAST(NULL AS INT) AS NonCompliantCheckpoints,
        CAST(NULL AS DECIMAL(8,2)) AS CheckpointsOTP,
        t1.Version
    FROM mb_tc_inner AS t1
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS t2
        ON t1.[Year] = t2.[Year] AND t1.[Month] = t2.[Month] AND t1.Subsystem = t2.GroupId
    GROUP BY t1.[Year], t1.[Month], t1.MonthName, t1.Subsystem, t2.EffectiveRateMiles,
             t2.EffectiveRateHours, t1.TotalSubsystemCost, t1.TotalOffset, t1.Version

    UNION ALL

    SELECT
        t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)),
        CAST(t1.Subsystem + ' - ' + t1.RouteId AS NVARCHAR(50)),
        CAST(t1.RouteId AS NVARCHAR(50)),
        CAST(CASE
            WHEN t1.RouteId = '20' THEN CONCAT(P2.Subsystem, ' (Metro Urbano) - Route E20 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
            WHEN t1.RouteId = '30' THEN CONCAT(P2.Subsystem, '(Metro Urbano) - Route E30 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
            ELSE CONCAT(t1.Subsystem, ' Route XXX ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
        END AS NVARCHAR(255)),
        CAST(P2.RevenueMilesAmount + P2.RevenueHoursAmount AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST('Regular' AS NVARCHAR(50)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(P2.EffectiveRateMiles AS DECIMAL(10,2)),
        CAST(P2.EffectiveRateHours AS DECIMAL(10,2)),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS DECIMAL(8,2)),
        t1.Version
    FROM mu_inner AS t1
    INNER JOIN mu_rates AS P2
        ON t1.[Year] = P2.[Year] AND t1.[Month] = P2.[Month]
        AND t1.Subsystem = P2.Subsystem AND t1.RouteId = P2.RouteId

    UNION ALL

    SELECT
        [Year], [Month], CAST(MonthName AS NVARCHAR(50)),
        CAST(Subsystem AS NVARCHAR(50)),
        CAST('-' AS NVARCHAR(50)),
        CAST(CONCAT(Subsystem, ' ', MonthName, ' ', [Year], ' ', 'Offset ',
            '(Checkpoints: ', TotalCheckpoints, ';', ' OTP: ', OnTimePerformance, '%',
            ' ==> ', 'Effective rate: ', CAST(EffectiveRate * 100 AS DECIMAL(4,2)), '%')
            AS NVARCHAR(255)),
        CAST(TotalOffset AS DECIMAL(10,2)),
        CAST((TotalOffset / TotalSubsystemCost) * 100 AS DECIMAL(10,2)),
        CAST('Offset' AS NVARCHAR(50)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(TotalCheckpoints AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(OnTimePerformance AS DECIMAL(8,2)),
        Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }}

),

with_max AS (
    SELECT *, MAX(Version) OVER (PARTITION BY [Year], [Month]) AS max_version
    FROM all_invoice
)

SELECT
    [Year], [Month], MonthName, Subsystem, Routes, Description, Total, OffsetPercentage,
    [Type], RevenueMiles, RevenueHours, MilesRate, HoursRate, Checkpoints,
    CompliantCheckpoints, NonCompliantCheckpoints, CheckpointsOTP, Version,
    CASE WHEN Version = max_version THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS CurrentVersion,
    CAST(NULL AS NVARCHAR(50)) AS InvoiceID,
    CAST(NULL AS INT) AS Trips,
    CAST(NULL AS DATETIME) AS CreatedAt,
    CAST(NULL AS DATETIME) AS UpdatedAt
FROM with_max


{% elif var('sonnell_invoice_reprocess', false) %}
{# ── REPROCESS MODE: delete+insert for a specific Month/Year.                              ── #}
{# ── No CurrentVersion filter on offset/cost. IsActive=1 on SonnellRates for MU.           ── #}

WITH mb_tc_inner AS (

    SELECT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        A.TotalSubsystemCost, A.TotalOffset,
        SUM(B.RevenueMiles) AS RevenueMiles,
        B.EffectiveRateMiles,
        SUM(B.RevenueHours) AS RevenueHours,
        B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem IN ('MB', 'TC')
        AND A.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
        AND A.[Year] = {{ var('sonnell_invoice_reprocess_year') }}
    GROUP BY A.[Year], A.[Month], A.MonthName, A.Subsystem, A.TotalSubsystemCost,
             A.TotalOffset, B.EffectiveRateMiles, B.EffectiveRateHours, A.Version, B.Version

),

mu_inner AS (

    SELECT DISTINCT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        B.RouteId,
        A.TotalSubsystemCost, A.TotalOffset,
        B.EffectiveRateMiles, B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem = 'MU'
        AND A.[Month] = {{ var('sonnell_invoice_reprocess_month') }}
        AND A.[Year] = {{ var('sonnell_invoice_reprocess_year') }}

),

mu_rates AS (

    SELECT
        [Year], [Month], MonthName, Subsystem, RouteId,
        SUM(RevenueMiles) AS RevenueMiles,
        SUM(RevenueHours) AS RevenueHours,
        CAST(SUM(RevenueMilesAmount) AS DECIMAL(10,2)) AS RevenueMilesAmount,
        CAST(SUM(RevenueHoursAmount) AS DECIMAL(10,2)) AS RevenueHoursAmount,
        EffectiveRateHours, EffectiveRateMiles
    FROM (
        SELECT
            YEAR(t1.ServiceDate) AS [Year],
            MONTH(t1.ServiceDate) AS [Month],
            DATENAME(MONTH, t1.ServiceDate) AS MonthName,
            t1.Subsystem, t1.RouteId,
            ROUND(SUM(t1.RevenueMeters) / 1609.34, 2) AS RevenueMiles,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0, 2) AS RevenueHours,
            ROUND(SUM(t1.RevenueMeters) / 1609.34 * t2.RateMiles, 2) AS RevenueMilesAmount,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0 * t2.RateHours, 2) AS RevenueHoursAmount,
            t2.RateHours AS EffectiveRateHours,
            t2.RateMiles AS EffectiveRateMiles
        FROM {{ source('sonnell', 'SonnellDailySummary') }} AS t1
        INNER JOIN {{ source('sonnell', 'SonnellRates') }} AS t2
            ON t1.Subsystem = t2.GroupId AND t2.IsActive = 1
        WHERE t1.ServiceDate BETWEEN t2.StartDate AND t2.EndDate
            AND t1.Subsystem = 'MU'
            AND MONTH(t1.ServiceDate) = {{ var('sonnell_invoice_reprocess_month') }}
            AND YEAR(t1.ServiceDate) = {{ var('sonnell_invoice_reprocess_year') }}
        GROUP BY YEAR(t1.ServiceDate), MONTH(t1.ServiceDate), DATENAME(MONTH, t1.ServiceDate),
                 t1.Subsystem, t1.RouteId, t2.RateHours, t2.RateMiles
    ) P1
    GROUP BY [Year], [Month], MonthName, Subsystem, RouteId, EffectiveRateHours, EffectiveRateMiles

)

SELECT
    t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)) AS MonthName,
    CAST(t1.Subsystem AS NVARCHAR(50)) AS Subsystem,
    CAST('-' AS NVARCHAR(50)) AS Routes,
    CAST(CONCAT(t1.Subsystem, ' ', t1.MonthName, 'All', t1.[Year], ' (',
        CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)), ' miles @ $', t2.EffectiveRateMiles,
        '; ', CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)), ' hours @ $', t2.EffectiveRateHours, ')')
        AS NVARCHAR(255)) AS Description,
    CAST(t1.TotalSubsystemCost AS DECIMAL(10,2)) AS Total,
    CAST(NULL AS DECIMAL(10,2)) AS OffsetPercentage,
    CAST('Regular' AS NVARCHAR(50)) AS [Type],
    CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)) AS RevenueMiles,
    CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)) AS RevenueHours,
    CAST(t2.EffectiveRateMiles AS DECIMAL(10,2)) AS MilesRate,
    CAST(t2.EffectiveRateHours AS DECIMAL(10,2)) AS HoursRate,
    CAST(NULL AS INT) AS Checkpoints,
    CAST(NULL AS INT) AS CompliantCheckpoints,
    CAST(NULL AS INT) AS NonCompliantCheckpoints,
    CAST(NULL AS DECIMAL(8,2)) AS CheckpointsOTP,
    t1.Version,
    CAST(1 AS BIT) AS CurrentVersion,
    CAST(NULL AS NVARCHAR(50)) AS InvoiceID,
    CAST(NULL AS INT) AS Trips,
    CAST(NULL AS DATETIME) AS CreatedAt,
    CAST(NULL AS DATETIME) AS UpdatedAt
FROM mb_tc_inner AS t1
INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS t2
    ON t1.[Year] = t2.[Year] AND t1.[Month] = t2.[Month] AND t1.Subsystem = t2.GroupId
GROUP BY t1.[Year], t1.[Month], t1.MonthName, t1.Subsystem, t2.EffectiveRateMiles,
         t2.EffectiveRateHours, t1.TotalSubsystemCost, t1.TotalOffset, t1.Version

UNION ALL

SELECT
    t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)),
    CAST(t1.Subsystem + ' - ' + t1.RouteId AS NVARCHAR(50)),
    CAST(t1.RouteId AS NVARCHAR(50)),
    CAST(CASE
        WHEN t1.RouteId = '20' THEN CONCAT(P2.Subsystem, ' (Metro Urbano) - Route E20 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
        WHEN t1.RouteId = '30' THEN CONCAT(P2.Subsystem, '(Metro Urbano) - Route E30 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
        ELSE CONCAT(t1.Subsystem, ' Route XXX ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
    END AS NVARCHAR(255)),
    CAST(P2.RevenueMilesAmount + P2.RevenueHoursAmount AS DECIMAL(10,2)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST('Regular' AS NVARCHAR(50)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(P2.EffectiveRateMiles AS DECIMAL(10,2)),
    CAST(P2.EffectiveRateHours AS DECIMAL(10,2)),
    CAST(NULL AS INT),
    CAST(NULL AS INT),
    CAST(NULL AS INT),
    CAST(NULL AS DECIMAL(8,2)),
    t1.Version,
    CAST(1 AS BIT),
    CAST(NULL AS NVARCHAR(50)),
    CAST(NULL AS INT),
    CAST(NULL AS DATETIME),
    CAST(NULL AS DATETIME)
FROM mu_inner AS t1
INNER JOIN mu_rates AS P2
    ON t1.[Year] = P2.[Year] AND t1.[Month] = P2.[Month]
    AND t1.Subsystem = P2.Subsystem AND t1.RouteId = P2.RouteId

UNION ALL

SELECT
    [Year], [Month], CAST(MonthName AS NVARCHAR(50)),
    CAST(Subsystem AS NVARCHAR(50)),
    CAST('-' AS NVARCHAR(50)),
    CAST(CONCAT(Subsystem, ' ', MonthName, ' ', [Year], ' ', 'Offset ',
        '(Checkpoints: ', TotalCheckpoints, ';', ' OTP: ', OnTimePerformance, '%',
        ' ==> ', 'Effective rate: ', CAST(EffectiveRate * 100 AS DECIMAL(4,2)), '%')
        AS NVARCHAR(255)),
    CAST(TotalOffset AS DECIMAL(10,2)),
    CAST((TotalOffset / TotalSubsystemCost) * 100 AS DECIMAL(10,2)),
    CAST('Offset' AS NVARCHAR(50)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(NULL AS DECIMAL(10,2)),
    CAST(TotalCheckpoints AS INT),
    CAST(NULL AS INT),
    CAST(NULL AS INT),
    CAST(OnTimePerformance AS DECIMAL(8,2)),
    Version,
    CAST(1 AS BIT),
    CAST(NULL AS NVARCHAR(50)),
    CAST(NULL AS INT),
    CAST(NULL AS DATETIME),
    CAST(NULL AS DATETIME)
FROM {{ ref('fct_sonnell_subsystem_offset') }}
WHERE [Month] = {{ var('sonnell_invoice_reprocess_month') }}
    AND [Year] = {{ var('sonnell_invoice_reprocess_year') }}


{% else %}
{# ── DAILY INCREMENTAL: closed months, CurrentVersion=1 on sources, NOT EXISTS dedup ── #}

WITH mb_tc_inner AS (

    SELECT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        A.TotalSubsystemCost, A.TotalOffset,
        SUM(B.RevenueMiles) AS RevenueMiles,
        B.EffectiveRateMiles,
        SUM(B.RevenueHours) AS RevenueHours,
        B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem IN ('MB', 'TC')
        AND CAST(CONCAT(B.[Year], '-', B.[Month], '-01') AS DATE)
            < CAST(CONCAT(DATEPART(YEAR, GETDATE()), '-', DATEPART(MONTH, GETDATE()), '-01') AS DATE)
        AND B.CurrentVersion = 1
        AND A.CurrentVersion = 1
    GROUP BY A.[Year], A.[Month], A.MonthName, A.Subsystem, A.TotalSubsystemCost,
             A.TotalOffset, B.EffectiveRateMiles, B.EffectiveRateHours, A.Version, B.Version

),

mu_inner AS (

    SELECT DISTINCT
        A.[Year], A.[Month], A.MonthName, A.Subsystem,
        B.RouteId,
        A.TotalSubsystemCost, A.TotalOffset,
        B.EffectiveRateMiles, B.EffectiveRateHours,
        COALESCE(A.Version, B.Version) AS Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }} AS A
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS B
        ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
    WHERE A.Subsystem = 'MU'
        AND CAST(CONCAT(B.[Year], '-', B.[Month], '-01') AS DATE)
            < CAST(CONCAT(DATEPART(YEAR, GETDATE()), '-', DATEPART(MONTH, GETDATE()), '-01') AS DATE)
        AND B.CurrentVersion = 1
        AND A.CurrentVersion = 1

),

mu_rates AS (

    SELECT
        [Year], [Month], MonthName, Subsystem, RouteId,
        SUM(RevenueMiles) AS RevenueMiles,
        SUM(RevenueHours) AS RevenueHours,
        CAST(SUM(RevenueMilesAmount) AS DECIMAL(10,2)) AS RevenueMilesAmount,
        CAST(SUM(RevenueHoursAmount) AS DECIMAL(10,2)) AS RevenueHoursAmount,
        EffectiveRateHours, EffectiveRateMiles
    FROM (
        SELECT
            YEAR(t1.ServiceDate) AS [Year],
            MONTH(t1.ServiceDate) AS [Month],
            DATENAME(MONTH, t1.ServiceDate) AS MonthName,
            t1.Subsystem, t1.RouteId,
            ROUND(SUM(t1.RevenueMeters) / 1609.34, 2) AS RevenueMiles,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0, 2) AS RevenueHours,
            ROUND(SUM(t1.RevenueMeters) / 1609.34 * t2.RateMiles, 2) AS RevenueMilesAmount,
            ROUND(SUM(t1.RevenueSeconds) / 3600.0 * t2.RateHours, 2) AS RevenueHoursAmount,
            t2.RateHours AS EffectiveRateHours,
            t2.RateMiles AS EffectiveRateMiles
        FROM {{ source('sonnell', 'SonnellDailySummary') }} AS t1
        INNER JOIN {{ source('sonnell', 'SonnellRates') }} AS t2
            ON t1.Subsystem = t2.GroupId
        WHERE t1.ServiceDate BETWEEN t2.StartDate AND t2.EndDate
            AND t1.Subsystem = 'MU'
            AND CAST(CONCAT(YEAR(t1.ServiceDate), '-', MONTH(t1.ServiceDate), '-01') AS DATE)
                < CAST(CONCAT(DATEPART(YEAR, GETDATE()), '-', DATEPART(MONTH, GETDATE()), '-01') AS DATE)
        GROUP BY YEAR(t1.ServiceDate), MONTH(t1.ServiceDate), DATENAME(MONTH, t1.ServiceDate),
                 t1.Subsystem, t1.RouteId, t2.RateHours, t2.RateMiles
    ) P1
    GROUP BY [Year], [Month], MonthName, Subsystem, RouteId, EffectiveRateHours, EffectiveRateMiles

),

result AS (

    SELECT
        t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)) AS MonthName,
        CAST(t1.Subsystem AS NVARCHAR(50)) AS Subsystem,
        CAST('-' AS NVARCHAR(50)) AS Routes,
        CAST(CONCAT(t1.Subsystem, ' ', t1.MonthName, 'All', t1.[Year], ' (',
            CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)), ' miles @ $', t2.EffectiveRateMiles,
            '; ', CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)), ' hours @ $', t2.EffectiveRateHours, ')')
            AS NVARCHAR(255)) AS Description,
        CAST(t1.TotalSubsystemCost AS DECIMAL(10,2)) AS Total,
        CAST(NULL AS DECIMAL(10,2)) AS OffsetPercentage,
        CAST('Regular' AS NVARCHAR(50)) AS [Type],
        CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)) AS RevenueMiles,
        CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)) AS RevenueHours,
        CAST(t2.EffectiveRateMiles AS DECIMAL(10,2)) AS MilesRate,
        CAST(t2.EffectiveRateHours AS DECIMAL(10,2)) AS HoursRate,
        CAST(NULL AS INT) AS Checkpoints,
        CAST(NULL AS INT) AS CompliantCheckpoints,
        CAST(NULL AS INT) AS NonCompliantCheckpoints,
        CAST(NULL AS DECIMAL(8,2)) AS CheckpointsOTP,
        t1.Version
    FROM mb_tc_inner AS t1
    INNER JOIN {{ ref('fct_sonnell_subsystem_cost') }} AS t2
        ON t1.[Year] = t2.[Year] AND t1.[Month] = t2.[Month] AND t1.Subsystem = t2.GroupId
    GROUP BY t1.[Year], t1.[Month], t1.MonthName, t1.Subsystem, t2.EffectiveRateMiles,
             t2.EffectiveRateHours, t1.TotalSubsystemCost, t1.TotalOffset, t1.Version

    UNION ALL

    SELECT
        t1.[Year], t1.[Month], CAST(t1.MonthName AS NVARCHAR(50)),
        CAST(t1.Subsystem + ' - ' + t1.RouteId AS NVARCHAR(50)),
        CAST(t1.RouteId AS NVARCHAR(50)),
        CAST(CASE
            WHEN t1.RouteId = '20' THEN CONCAT(P2.Subsystem, ' (Metro Urbano) - Route E20 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
            WHEN t1.RouteId = '30' THEN CONCAT(P2.Subsystem, '(Metro Urbano) - Route E30 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
            ELSE CONCAT(t1.Subsystem, ' Route XXX ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
        END AS NVARCHAR(255)),
        CAST(P2.RevenueMilesAmount + P2.RevenueHoursAmount AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST('Regular' AS NVARCHAR(50)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(P2.EffectiveRateMiles AS DECIMAL(10,2)),
        CAST(P2.EffectiveRateHours AS DECIMAL(10,2)),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS DECIMAL(8,2)),
        t1.Version
    FROM mu_inner AS t1
    INNER JOIN mu_rates AS P2
        ON t1.[Year] = P2.[Year] AND t1.[Month] = P2.[Month]
        AND t1.Subsystem = P2.Subsystem AND t1.RouteId = P2.RouteId

    UNION ALL

    SELECT
        [Year], [Month], CAST(MonthName AS NVARCHAR(50)),
        CAST(Subsystem AS NVARCHAR(50)),
        CAST('-' AS NVARCHAR(50)),
        CAST(CONCAT(Subsystem, ' ', MonthName, ' ', [Year], ' ', 'Offset ',
            '(Checkpoints: ', TotalCheckpoints, ';', ' OTP: ', OnTimePerformance, '%',
            ' ==> ', 'Effective rate: ', CAST(EffectiveRate * 100 AS DECIMAL(4,2)), '%')
            AS NVARCHAR(255)),
        CAST(TotalOffset AS DECIMAL(10,2)),
        CAST((TotalOffset / TotalSubsystemCost) * 100 AS DECIMAL(10,2)),
        CAST('Offset' AS NVARCHAR(50)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(NULL AS DECIMAL(10,2)),
        CAST(TotalCheckpoints AS INT),
        CAST(NULL AS INT),
        CAST(NULL AS INT),
        CAST(OnTimePerformance AS DECIMAL(8,2)),
        Version
    FROM {{ ref('fct_sonnell_subsystem_offset') }}
    WHERE CAST(CONCAT([Year], '-', [Month], '-01') AS DATE)
        < CAST(CONCAT(DATEPART(YEAR, GETDATE()), '-', DATEPART(MONTH, GETDATE()), '-01') AS DATE)
        AND CurrentVersion = 1

)

SELECT
    r.[Year], r.[Month], r.MonthName, r.Subsystem, r.Routes, r.Description, r.Total,
    r.OffsetPercentage, r.[Type], r.RevenueMiles, r.RevenueHours, r.MilesRate, r.HoursRate,
    r.Checkpoints, r.CompliantCheckpoints, r.NonCompliantCheckpoints, r.CheckpointsOTP,
    r.Version,
    CAST(1 AS BIT) AS CurrentVersion,
    CAST(NULL AS NVARCHAR(50)) AS InvoiceID,
    CAST(NULL AS INT) AS Trips,
    CAST(NULL AS DATETIME) AS CreatedAt,
    CAST(NULL AS DATETIME) AS UpdatedAt
FROM result AS r
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} AS existing
    WHERE existing.[Year] = r.[Year]
        AND existing.[Month] = r.[Month]
        AND existing.Subsystem = r.Subsystem
        AND existing.[Type] = r.[Type]
        AND existing.Version = r.Version
)

{% endif %}
