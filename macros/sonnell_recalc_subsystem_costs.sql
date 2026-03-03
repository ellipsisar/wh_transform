{#
  Macro: sonnell_recalc_subsystem_costs
  Equivalent to SP: Sonnell_CalculateSubsystemCostsByMonth

  Usage:
    dbt run-operation sonnell_recalc_subsystem_costs --args '{"year": 2026, "month": 1}'

  Note: ref() and source() are not available in run-operation context,
  so table names are constructed via api.Relation.create().
#}

{% macro sonnell_recalc_subsystem_costs(year, month) %}

    {% if year is none or month is none %}
        {{ exceptions.raise_compiler_error("Error: both 'year' and 'month' parameters are required. Example: --args '{\"year\": 2026, \"month\": 1}'") }}
    {% endif %}

    {% set target_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'SonnellSubsystemCost'
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

    {# Step 1: Delete existing records for the month #}
    {% call statement('delete_month', fetch_result=False) %}
        DELETE FROM {{ target_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Deleted existing records for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 2: Insert from DailySummary with rates (IsActive=1) and amounts inline #}
    {% call statement('insert_month', fetch_result=False) %}
        INSERT INTO {{ target_rel }}
            (DailySummaryId, ServiceDate, [Year], [Month], MonthName, GroupId, RouteId,
             NumTrips, RevenueHours, RevenueMiles, RevenueHoursAmount, RevenueMilesAmount,
             EffectiveRateMiles, EffectiveRateHours, CreatedAt, Version, CurrentVersion)
        SELECT
            CAST(P.Id AS INT)                                                   AS DailySummaryId,
            P.ServiceDate,
            YEAR(P.ServiceDate)                                                 AS [Year],
            MONTH(P.ServiceDate)                                                AS [Month],
            DATENAME(MONTH, P.ServiceDate)                                      AS MonthName,
            P.Subsystem                                                         AS GroupId,
            P.RouteId,
            P.TripCount                                                         AS NumTrips,
            ROUND(P.RevenueSeconds / 3600.0, 2)                                AS RevenueHours,
            ROUND(P.RevenueMeters / 1609.34, 2)                                AS RevenueMiles,
            ROUND(ROUND(P.RevenueSeconds / 3600.0, 2) * R.RateHours, 2)        AS RevenueHoursAmount,
            ROUND(ROUND(P.RevenueMeters / 1609.34, 2) * R.RateMiles, 2)        AS RevenueMilesAmount,
            R.RateMiles                                                         AS EffectiveRateMiles,
            R.RateHours                                                         AS EffectiveRateHours,
            SYSDATETIME()                                                       AS CreatedAt,
            P.Version,
            1                                                                   AS CurrentVersion
        FROM {{ daily_summary_rel }} AS P
        LEFT JOIN {{ rates_rel }} AS R
            ON P.Subsystem = R.GroupId
            AND P.ServiceDate BETWEEN R.StartDate AND R.EndDate
            AND R.IsActive = 1
        WHERE MONTH(P.ServiceDate) = {{ month }}
            AND YEAR(P.ServiceDate) = {{ year }}
            AND NOT EXISTS (
                SELECT 1 FROM {{ target_rel }} AS S
                WHERE S.DailySummaryId = CAST(P.Id AS INT)
            )
    {% endcall %}

    {# Step 3: Back-fill rates for any rows that missed inline (safety net, matches SP2 Step 4) #}
    {% call statement('update_rates', fetch_result=False) %}
        UPDATE A
        SET
            A.EffectiveRateMiles = B.RateMiles,
            A.EffectiveRateHours = B.RateHours
        FROM {{ target_rel }} AS A
        INNER JOIN {{ rates_rel }} AS B
            ON A.GroupId = B.GroupId
            AND B.IsActive = 1
        WHERE A.ServiceDate BETWEEN B.StartDate AND B.EndDate
            AND A.[Month] = {{ month }}
            AND A.[Year] = {{ year }}
            AND (A.RevenueHoursAmount IS NULL OR A.RevenueHoursAmount = 0)
            AND (A.EffectiveRateHours IS NULL OR A.EffectiveRateMiles IS NULL)
    {% endcall %}

    {# Step 4: Compute amounts (matches SP2 Step 5) #}
    {% call statement('update_amounts', fetch_result=False) %}
        UPDATE A
        SET
            A.RevenueMilesAmount = ROUND(A.RevenueMiles * A.EffectiveRateMiles, 2),
            A.RevenueHoursAmount = ROUND(A.RevenueHours * A.EffectiveRateHours, 2)
        FROM {{ target_rel }} AS A
        WHERE A.[Month] = {{ month }}
            AND A.[Year] = {{ year }}
            AND (A.RevenueMilesAmount IS NULL OR A.RevenueHoursAmount IS NULL)
    {% endcall %}

    {# Step 5: Count and log result (matches SP2 final SELECT) #}
    {% call statement('count_results', fetch_result=True) %}
        SELECT COUNT(*) AS cnt
        FROM {{ target_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}

    {% set result = load_result('count_results') %}
    {% if result and result['data'] %}
        {{ log("Result: " ~ result['data'][0][0] ~ " row(s) were added for " ~ year ~ "-" ~ month, info=True) }}
    {% else %}
        {{ log("Result: reprocess completed for " ~ year ~ "-" ~ month, info=True) }}
    {% endif %}

{% endmacro %}
