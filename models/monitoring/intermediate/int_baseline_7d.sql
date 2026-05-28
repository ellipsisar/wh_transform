{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT
    event_date,
    entity_name,
    domain,
    entity_type,
    entity_base_name,
    snapshot_file_date,
    records_processed,
    records_failed,
    executions_total,
    executions_success,
    executions_failed,
    executions_skipped,
    last_success_datetime,
    last_error_datetime,
    last_error_message,

    CASE WHEN days_in_baseline >= 3 THEN baseline_7d_avg               ELSE NULL END AS baseline_7d_avg,
    CASE WHEN days_in_baseline >= 3 THEN ISNULL(baseline_7d_stddev, 0) ELSE NULL END AS baseline_7d_stddev,

    CASE
        WHEN days_in_baseline >= 3 AND baseline_7d_avg > 0
        THEN ((CAST(records_processed AS DECIMAL(18, 4)) - baseline_7d_avg) / baseline_7d_avg) * 100
        ELSE NULL
    END AS volume_variation_pct,

    CASE
        WHEN days_in_baseline >= 3 AND ISNULL(baseline_7d_stddev, 0) > 0
        THEN (CAST(records_processed AS DECIMAL(18, 4)) - baseline_7d_avg) / baseline_7d_stddev
        ELSE NULL
    END AS volume_zscore

FROM (
    SELECT
        event_date,
        entity_name,
        domain,
        entity_type,
        entity_base_name,
        snapshot_file_date,
        records_processed,
        records_failed,
        executions_total,
        executions_success,
        executions_failed,
        executions_skipped,
        last_success_datetime,
        last_error_datetime,
        last_error_message,

        AVG(CAST(records_processed AS DECIMAL(18, 2))) OVER (
            PARTITION BY entity_name, domain
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_7d_avg,

        STDEV(CAST(records_processed AS DECIMAL(18, 2))) OVER (
            PARTITION BY entity_name, domain
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_7d_stddev,

        COUNT(*) OVER (
            PARTITION BY entity_name, domain
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS days_in_baseline

    FROM {{ ref('int_daily_aggregates') }}
) AS window_calc
