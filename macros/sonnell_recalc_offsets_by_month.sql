{#
  Macro: sonnell_recalc_offsets_by_month
  Equivalent to SP: Sonnell_CalculateOffsetsByMonth + Sonnell_FetchOffsetValuesByMonth

  Usage:
    dbt run-operation sonnell_recalc_offsets_by_month --args '{"year": 2026, "month": 1}'

  Note: ref() and source() are not available in run-operation context,
  so table names are constructed via api.Relation.create().
#}

{% macro sonnell_recalc_offsets_by_month(year, month) %}

    {% if year is none or month is none %}
        {{ exceptions.raise_compiler_error("Error: both 'year' and 'month' parameters are required. Example: --args '{\"year\": 2026, \"month\": 1}'") }}
    {% endif %}

    {% set target_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'dbt_SonnellSubsystemOffset'
    ) %}

    {% set checkpoints_rel = api.Relation.create(
        database = target.database,
        schema   = 'dbo',
        identifier = 'SonnellCheckpoints'
    ) %}

    {% set costs_rel = api.Relation.create(
        database = target.database,
        schema   = target.schema,
        identifier = 'dbt_SonnellSubsystemCost'
    ) %}

    {% set params_rel = api.Relation.create(
        database = target.database,
        schema   = 'dbo',
        identifier = 'SonnellParameters'
    ) %}

    {# Step 1: Delete existing records for the month #}
    {% call statement('delete_month', fetch_result=False) %}
        DELETE FROM {{ target_rel }}
        WHERE [Month] = {{ month }} AND [Year] = {{ year }}
    {% endcall %}
    {{ log("Deleted existing records for " ~ year ~ "-" ~ month, info=True) }}

    {# Step 2: Insert aggregated checkpoints (replicates SP2 INSERT) #}
    {% call statement('insert_month', fetch_result=False) %}
        INSERT INTO {{ target_rel }}
            ([Year], [Month], MonthName, Subsystem, TotalCheckpoints,
             TotalCompliant1, TotalCompliant0, OnTimePerformance,
             Version, CurrentVersion)
        SELECT
            YEAR(ServiceDate)                                                  AS [Year],
            MONTH(ServiceDate)                                                 AS [Month],
            DATENAME(MONTH, ServiceDate)                                       AS MonthName,
            Subsystem,
            COUNT(*)                                                           AS TotalCheckpoints,
            SUM(CASE WHEN Compliant = 't' THEN 1 ELSE 0 END)                  AS TotalCompliant1,
            SUM(CASE WHEN Compliant = 'f' THEN 1 ELSE 0 END)                  AS TotalCompliant0,
            CASE
                WHEN COUNT(*) > 0
                THEN ROUND(SUM(CASE WHEN Compliant = 't' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                ELSE 0
            END                                                                AS OnTimePerformance,
            Version,
            1                                                                  AS CurrentVersion
        FROM {{ checkpoints_rel }}
        WHERE YEAR(ServiceDate) = {{ year }}
            AND MONTH(ServiceDate) = {{ month }}
            AND NOT EXISTS (
                SELECT 1 FROM {{ target_rel }} AS O
                WHERE O.[Year] = YEAR(ServiceDate)
                AND O.[Month] = MONTH(ServiceDate)
                AND O.Subsystem = {{ checkpoints_rel }}.Subsystem
            )
        GROUP BY YEAR(ServiceDate), MONTH(ServiceDate), DATENAME(MONTH, ServiceDate),
                 Subsystem, Version
    {% endcall %}

    {# Step 3: Update TotalSubsystemCost — no CurrentVersion filter on costs (replicates SP2 CROSS APPLY) #}
    {% call statement('update_costs', fetch_result=False) %}
        UPDATE O
        SET O.TotalSubsystemCost = CostSummary.TotalCost
        FROM {{ target_rel }} AS O
        CROSS APPLY (
            SELECT SUM(S.RevenueMilesAmount + S.RevenueHoursAmount) AS TotalCost
            FROM {{ costs_rel }} AS S
            WHERE S.GroupId = O.Subsystem
                AND S.[Year] = O.[Year]
                AND S.[Month] = O.[Month]
        ) AS CostSummary
        WHERE O.[Year] = {{ year }}
            AND O.[Month] = {{ month }}
    {% endcall %}

    {# Step 4: Update EffectiveRate — no IS NULL filter (replicates SP3 child SP) #}
    {% call statement('update_rates', fetch_result=False) %}
        UPDATE O
        SET O.EffectiveRate = B.IncentivesOffsets
        FROM {{ target_rel }} AS O
        INNER JOIN {{ params_rel }} AS B
            ON O.Subsystem = B.GroupId
            AND (O.OnTimePerformance / 100.0) > B.CheckPointOTPMin
            AND (O.OnTimePerformance / 100.0) <= B.CheckPointOTPMax
        WHERE O.[Year] = {{ year }}
            AND O.[Month] = {{ month }}
    {% endcall %}

    {# Step 5: Compute TotalOffset (replicates SP2 final UPDATE) #}
    {% call statement('update_offset', fetch_result=False) %}
        UPDATE {{ target_rel }}
        SET TotalOffset = ROUND(TotalSubsystemCost * EffectiveRate, 2)
        WHERE [Year] = {{ year }}
            AND [Month] = {{ month }}
            AND TotalOffset IS NULL
    {% endcall %}

    {# Step 6: Count and log result (replicates SP2 final SELECT) #}
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
