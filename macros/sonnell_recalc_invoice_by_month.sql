{#
  Macro: sonnell_recalc_invoice_by_month
  Equivalent to SP: Sonnell_CalculateInvoiceTotalsByMonth (SP3)

  Usage:
    dbt run-operation sonnell_recalc_invoice_by_month --args '{"year": 2026, "month": 1}'

  Note: ref() and source() are not available in run-operation context,
  so table names are constructed via api.Relation.create().
#}

{% macro sonnell_recalc_invoice_by_month(year, month) %}

    {% if year is none or month is none %}
        {{ exceptions.raise_compiler_error("Error: both 'year' and 'month' parameters are required. Example: --args '{\"year\": 2026, \"month\": 1}'") }}
    {% endif %}

    {% set invoice_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'dbt_SonnellInvoiceTotals'
    ) %}

    {% set offset_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'dbt_SonnellSubsystemOffset'
    ) %}

    {% set cost_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'dbt_SonnellSubsystemCost'
    ) %}

    {% set daily_summary_rel = api.Relation.create(
        database = target.database,
        schema   = 'dbo',
        identifier = 'SonnellDailySummary'
    ) %}

    {% set rates_rel = api.Relation.create(
        database = target.database,
        schema   = 'dbo',
        identifier = 'SonnellRates'
    ) %}

    {% set offset_applied_rel = api.Relation.create(
        database = target.database,
        schema   = 'dbo',
        identifier = 'SonnellOffsetApplied'
    ) %}


    {# Step 1: Delete existing invoice records for the month #}
    {% call statement('delete_invoice', fetch_result=False) %}
        DELETE FROM {{ invoice_rel }}
        WHERE [Type] IN ('Regular', 'Offset')
            AND [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Deleted existing invoice records for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 2: Delete existing offset applied records for the month #}
    {% call statement('delete_offset_applied', fetch_result=False) %}
        DELETE FROM {{ offset_applied_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Deleted existing offset applied records for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 3: Insert invoice totals (3 UNION ALL blocks — no CurrentVersion filter, IsActive=1 on rates for MU) #}
    {% call statement('insert_invoice', fetch_result=False) %}
        ;WITH mb_tc_inner AS (
            SELECT
                A.[Year], A.[Month], A.MonthName, A.Subsystem,
                A.TotalSubsystemCost, A.TotalOffset,
                SUM(B.RevenueMiles) AS RevenueMiles,
                B.EffectiveRateMiles,
                SUM(B.RevenueHours) AS RevenueHours,
                B.EffectiveRateHours,
                COALESCE(A.Version, B.Version) AS Version
            FROM {{ offset_rel }} AS A
            INNER JOIN {{ cost_rel }} AS B
                ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
            WHERE A.Subsystem IN ('MB', 'TC')
                AND A.[Month] = {{ month }}
                AND A.[Year] = {{ year }}
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
            FROM {{ offset_rel }} AS A
            INNER JOIN {{ cost_rel }} AS B
                ON A.[Year] = B.[Year] AND A.[Month] = B.[Month] AND A.Subsystem = B.GroupId
            WHERE A.Subsystem = 'MU'
                AND A.[Month] = {{ month }}
                AND A.[Year] = {{ year }}
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
                FROM {{ daily_summary_rel }} AS t1
                INNER JOIN {{ rates_rel }} AS t2
                    ON t1.Subsystem = t2.GroupId AND t2.IsActive = 1
                WHERE t1.ServiceDate BETWEEN t2.StartDate AND t2.EndDate
                    AND t1.Subsystem = 'MU'
                    AND MONTH(t1.ServiceDate) = {{ month }}
                    AND YEAR(t1.ServiceDate) = {{ year }}
                GROUP BY YEAR(t1.ServiceDate), MONTH(t1.ServiceDate), DATENAME(MONTH, t1.ServiceDate),
                         t1.Subsystem, t1.RouteId, t2.RateHours, t2.RateMiles
            ) P1
            GROUP BY [Year], [Month], MonthName, Subsystem, RouteId, EffectiveRateHours, EffectiveRateMiles
        )
        INSERT INTO {{ invoice_rel }}
            ([Year], [Month], MonthName, Subsystem, Routes, Description, Total, OffsetPercentage,
             [Type], RevenueMiles, RevenueHours, MilesRate, HoursRate, Checkpoints,
             CompliantCheckpoints, NonCompliantCheckpoints, CheckpointsOTP, Version, CurrentVersion)

        SELECT
            t1.[Year], t1.[Month], t1.MonthName, t1.Subsystem,
            '-' AS Routes,
            CONCAT(t1.Subsystem, ' ', t1.MonthName, 'All', t1.[Year], ' (',
                CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)), ' miles @ $', t2.EffectiveRateMiles,
                '; ', CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)), ' hours @ $', t2.EffectiveRateHours, ')') AS Description,
            CAST(t1.TotalSubsystemCost AS DECIMAL(10,2)) AS Total,
            NULL AS OffsetPercentage,
            'Regular' AS [Type],
            CAST(SUM(t2.RevenueMiles) AS DECIMAL(10,2)) AS RevenueMiles,
            CAST(SUM(t2.RevenueHours) AS DECIMAL(10,2)) AS RevenueHours,
            t2.EffectiveRateMiles AS MilesRate,
            t2.EffectiveRateHours AS HoursRate,
            NULL AS Checkpoints,
            NULL AS CompliantCheckpoints,
            NULL AS NonCompliantCheckpoints,
            NULL AS CheckpointsOTP,
            t1.Version, 1 AS CurrentVersion
        FROM mb_tc_inner AS t1
        INNER JOIN {{ cost_rel }} AS t2
            ON t1.[Year] = t2.[Year] AND t1.[Month] = t2.[Month] AND t1.Subsystem = t2.GroupId
        GROUP BY t1.[Year], t1.[Month], t1.MonthName, t1.Subsystem, t2.EffectiveRateMiles,
                 t2.EffectiveRateHours, t1.TotalSubsystemCost, t1.TotalOffset, t1.Version

        UNION ALL

        SELECT
            t1.[Year], t1.[Month], t1.MonthName,
            t1.Subsystem + ' - ' + t1.RouteId,
            t1.RouteId,
            CASE
                WHEN t1.RouteId = '20' THEN CONCAT(P2.Subsystem, ' (Metro Urbano) - Route E20 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
                WHEN t1.RouteId = '30' THEN CONCAT(P2.Subsystem, '(Metro Urbano) - Route E30 ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
                ELSE CONCAT(t1.Subsystem, ' Route XXX ', t1.MonthName, ' ', t1.[Year], ' (', CAST(P2.RevenueMiles AS DECIMAL(10,2)), ' miles @ $', P2.EffectiveRateMiles, '; ', CAST(P2.RevenueHours AS DECIMAL(10,2)), ' hours @ $', P2.EffectiveRateHours, ')')
            END,
            CAST(P2.RevenueMilesAmount + P2.RevenueHoursAmount AS DECIMAL(10,2)),
            NULL, 'Regular', NULL, NULL,
            P2.EffectiveRateMiles, P2.EffectiveRateHours,
            NULL, NULL, NULL, NULL,
            t1.Version, 1
        FROM mu_inner AS t1
        INNER JOIN mu_rates AS P2
            ON t1.[Year] = P2.[Year] AND t1.[Month] = P2.[Month]
            AND t1.Subsystem = P2.Subsystem AND t1.RouteId = P2.RouteId

        UNION ALL

        SELECT
            [Year], [Month], MonthName, Subsystem, '-',
            CONCAT(Subsystem, ' ', MonthName, ' ', [Year], ' ', 'Offset ',
                '(Checkpoints: ', TotalCheckpoints, ';', ' OTP: ', OnTimePerformance, '%',
                ' ==> ', 'Effective rate: ', CAST(EffectiveRate * 100 AS DECIMAL(4,2)), '%'),
            TotalOffset,
            CAST((TotalOffset / TotalSubsystemCost) * 100 AS DECIMAL(10,2)),
            'Offset', NULL, NULL, NULL, NULL,
            TotalCheckpoints, NULL, NULL, OnTimePerformance,
            Version, 1
        FROM {{ offset_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Inserted invoice totals for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 4: Generate InvoiceID (single NEWID for the entire month, matching SP3) #}
    {% call statement('gen_invoice_id', fetch_result=False) %}
        ;WITH SingleID AS (SELECT NEWID() AS InvoiceID)
        UPDATE t
        SET t.InvoiceID = s.InvoiceID
        FROM {{ invoice_rel }} AS t
        CROSS JOIN SingleID AS s
        WHERE t.[Month] = {{ month }}
            AND t.[Year] = {{ year }}
            AND t.InvoiceID IS NULL
    {% endcall %}

    {# Step 5: Insert 4% rule into SonnellOffsetApplied #}
    {% call statement('insert_offset_applied', fetch_result=False) %}
        ;WITH REGULAR AS (
            SELECT [Year], [Month], MonthName, Subsystem,
                   SUM(Total) AS TotalInvoiceRegular,
                   SUM(Total) * 0.04 AS FourPercentRegularTotal
            FROM {{ invoice_rel }}
            WHERE [Type] = 'Regular' AND CurrentVersion = 1
                AND [Month] = {{ month }} AND [Year] = {{ year }}
            GROUP BY [Year], [Month], MonthName, Subsystem
        ),
        OFFSET_TOTALS AS (
            SELECT [Year], [Month], Subsystem,
                   SUM(Total) AS TotalInvoiceOffset
            FROM {{ invoice_rel }}
            WHERE [Type] = 'Offset' AND CurrentVersion = 1
                AND [Month] = {{ month }} AND [Year] = {{ year }}
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
        INSERT INTO {{ offset_applied_rel }}
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
            FROM {{ invoice_rel }}
            WHERE CurrentVersion = 1 AND InvoiceID IS NOT NULL
                AND [Month] = {{ month }} AND [Year] = {{ year }}
        ) IT ON P.[Year] = IT.[Year] AND P.[Month] = IT.[Month]
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Inserted offset applied records for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 6: Propagate InvoiceID to offset table (all subsystems, matching SP3) #}
    {% call statement('propagate_invoice_offset', fetch_result=False) %}
        ;WITH InvID AS (
            SELECT TOP 1 InvoiceID FROM {{ invoice_rel }}
            WHERE [Month] = {{ month }} AND [Year] = {{ year }} AND InvoiceID IS NOT NULL
        )
        UPDATE o SET o.InvoiceId = i.InvoiceID
        FROM {{ offset_rel }} AS o
        CROSS JOIN InvID AS i
        WHERE o.[Month] = {{ month }} AND o.[Year] = {{ year }}
            AND o.CurrentVersion = 1 AND o.InvoiceId IS NULL
    {% endcall %}

    {# Step 7: Propagate InvoiceID to cost table (all subsystems, matching SP3) #}
    {% call statement('propagate_invoice_cost', fetch_result=False) %}
        ;WITH InvID AS (
            SELECT TOP 1 InvoiceID FROM {{ invoice_rel }}
            WHERE [Month] = {{ month }} AND [Year] = {{ year }} AND InvoiceID IS NOT NULL
        )
        UPDATE c SET c.InvoiceId = i.InvoiceID
        FROM {{ cost_rel }} AS c
        CROSS JOIN InvID AS i
        WHERE c.[Month] = {{ month }} AND c.[Year] = {{ year }}
            AND c.CurrentVersion = 1 AND c.InvoiceId IS NULL
    {% endcall %}

    {# Step 8: Trips for MB/TC (no CurrentVersion filter on cost, matching SP3) #}
    {% call statement('trips_mb_tc', fetch_result=False) %}
        UPDATE inv SET inv.Trips = agg.TotalTrips
        FROM {{ invoice_rel }} AS inv
        INNER JOIN (
            SELECT [Year], [Month], GroupId, SUM(NumTrips) AS TotalTrips
            FROM {{ cost_rel }}
            WHERE GroupId IN ('MB', 'TC')
            GROUP BY [Year], [Month], GroupId
        ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
            AND inv.Subsystem = agg.GroupId
        WHERE inv.[Type] = 'Regular'
            AND inv.[Month] = {{ month }} AND inv.[Year] = {{ year }}
    {% endcall %}

    {# Step 9: Trips for MU routes (no CurrentVersion filter on cost, matching SP3) #}
    {% call statement('trips_mu', fetch_result=False) %}
        UPDATE inv SET inv.Trips = agg.TotalTrips
        FROM {{ invoice_rel }} AS inv
        INNER JOIN (
            SELECT [Year], [Month], RouteId, SUM(NumTrips) AS TotalTrips
            FROM {{ cost_rel }}
            WHERE RouteId IN ('E20', '20', 'E30', '30')
            GROUP BY [Year], [Month], RouteId
        ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
            AND inv.Routes = agg.RouteId
        WHERE inv.[Type] = 'Regular' AND inv.Routes IN ('E20', '20', 'E30', '30')
            AND inv.[Month] = {{ month }} AND inv.[Year] = {{ year }}
    {% endcall %}

    {# Step 10: RevenueMiles + RevenueHours for MB/TC (no CurrentVersion on cost) #}
    {% call statement('revenue_mb_tc', fetch_result=False) %}
        UPDATE inv
        SET inv.RevenueMiles = agg.TotalRevenueMiles, inv.RevenueHours = agg.TotalRevenueHours
        FROM {{ invoice_rel }} AS inv
        INNER JOIN (
            SELECT [Year], [Month], GroupId,
                   SUM(RevenueMiles) AS TotalRevenueMiles, SUM(RevenueHours) AS TotalRevenueHours
            FROM {{ cost_rel }}
            WHERE GroupId IN ('MB', 'TC')
            GROUP BY [Year], [Month], GroupId
        ) AS agg ON inv.[Year] = agg.[Year] AND inv.[Month] = agg.[Month]
            AND inv.Subsystem = agg.GroupId
        WHERE inv.[Type] = 'Regular' AND inv.Subsystem IN ('MB', 'TC')
            AND inv.[Month] = {{ month }} AND inv.[Year] = {{ year }}
    {% endcall %}

    {# Step 11: Checkpoints for Offset (no CurrentVersion on offset, matching SP3) #}
    {% call statement('checkpoints_offset', fetch_result=False) %}
        UPDATE inv
        SET inv.CompliantCheckpoints = o.TotalCompliant1,
            inv.NonCompliantCheckpoints = o.TotalCompliant0
        FROM {{ invoice_rel }} AS inv
        INNER JOIN {{ offset_rel }} AS o
            ON inv.[Year] = o.[Year] AND inv.[Month] = o.[Month]
            AND inv.Subsystem = o.Subsystem
        WHERE inv.[Type] = 'Offset'
            AND inv.[Month] = {{ month }} AND inv.[Year] = {{ year }}
    {% endcall %}

    {# Step 12: CreatedAt + UpdatedAt #}
    {% call statement('timestamps', fetch_result=False) %}
        UPDATE {{ invoice_rel }}
        SET CreatedAt = GETDATE(), UpdatedAt = GETDATE()
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}

    {# Step 13: Count and log result #}
    {% call statement('count_results', fetch_result=True) %}
        SELECT COUNT(*) AS cnt
        FROM {{ invoice_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
            AND [Type] IN ('Regular', 'Offset')
    {% endcall %}

    {% set result = load_result('count_results') %}
    {% if result and result['data'] %}
        {{ log("Result: " ~ result['data'][0][0] ~ " line(s) were added for " ~ year ~ "-" ~ month ~ " for Invoice totals", info=True) }}
    {% else %}
        {{ log("Result: reprocess completed for " ~ year ~ "-" ~ month, info=True) }}
    {% endif %}

{% endmacro %}
