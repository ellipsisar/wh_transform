{{
  config(
    materialized = 'view'
  )
}}

/*
  CAMBIO 1: Reclasificación de dominios
    snapshot_sonnell       → sonnel
    snapshot_transdev      → transdev
    transdev               → transdev
    ama_legacy             → data_legacy
    snapshot_ama_legacy    → data_legacy
    geotab                 → ama
    other                  → gtfs

  CAMBIO 2: Nueva columna source derivada del domain reclasificado
    sonnel, transdev, data_legacy → korbato
    ama                           → geotab
    gtfs                          → other
*/

WITH baseline AS (
    SELECT * FROM {{ ref('int_baseline_7d') }}
),

entity_config AS (
    SELECT * FROM {{ ref('stg_entity_config') }}
),

errors AS (
    SELECT * FROM {{ ref('int_latest_errors') }}
),

domain_reclassified AS (
    SELECT
        b.*,
        -- CAMBIO 1: Reclasificar domain
        CASE
            WHEN b.domain = 'snapshot_sonnell'       THEN 'sonnel'
            WHEN b.domain = 'snapshot_transdev'      THEN 'transdev'
            WHEN b.domain = 'transdev'               THEN 'transdev'
            WHEN b.domain = 'ama_legacy'             THEN 'data_legacy'
            WHEN b.domain = 'snapshot_ama_legacy'    THEN 'data_legacy'
            WHEN b.domain = 'geotab'                 THEN 'ama'
            WHEN b.domain = 'other'                  THEN 'gtfs'
            ELSE b.domain  -- fallback para valores no mapeados (sonnell, hms, gtfs_orig, snapshot_other)
        END AS domain_final
    FROM baseline b
),

with_source AS (
    SELECT
        d.*,
        -- CAMBIO 2: Derivar source del domain reclasificado
        CASE
            WHEN domain_final IN ('sonnel', 'transdev', 'data_legacy') THEN 'korbato'
            WHEN domain_final = 'ama'                                  THEN 'geotab'
            WHEN domain_final = 'gtfs'                                 THEN 'other'
            ELSE 'other'  -- fallback
        END AS source
    FROM domain_reclassified d
),

with_config AS (
    SELECT
        ws.*,
        -- CAMBIO 4: expected_frequency_hrs derivado de source (no de domain)
        CASE
            WHEN ws.source = 'korbato' THEN 24
            WHEN ws.source = 'geotab'  THEN 1
            ELSE NULL  -- source = 'other': no aplica SLA
        END AS expected_frequency_hrs,
        ISNULL(c.domain_description, 'Unknown') AS domain_description
    FROM with_source ws
    LEFT JOIN entity_config c ON ws.domain_final = c.domain
),

with_errors AS (
    SELECT
        w.*,
        ISNULL(e.latest_error_message,  w.last_error_message)  AS final_error_message,
        ISNULL(e.latest_error_datetime, w.last_error_datetime)  AS final_error_datetime
    FROM with_config w
    LEFT JOIN errors e
        ON  w.event_date   = e.event_date
        AND w.entity_name  = e.entity_name
        AND w.domain       = e.domain
),

with_derived AS (
    SELECT
        *,

        CASE
            WHEN executions_total > 0
            THEN (CAST(executions_failed AS DECIMAL(8, 4)) / CAST(executions_total AS DECIMAL(8, 4))) * 100
            ELSE 0
        END AS error_rate_pct,

        -- Hours elapsed from last_success to end-of-day (used for SLA, avoids GETDATE() drift)
        CASE
            WHEN last_success_datetime IS NOT NULL
            THEN DATEDIFF(HOUR, last_success_datetime, DATEADD(DAY, 1, event_date))
            ELSE NULL
        END AS hours_since_success_eod,

        -- Anomaly flags
        CASE WHEN ABS(ISNULL(volume_zscore, 0)) > 2.5                                THEN 1 ELSE 0 END AS is_volume_anomaly,
        CASE WHEN records_processed = 0 AND executions_success > 0                   THEN 1 ELSE 0 END AS is_zero_records,
        CASE
            WHEN executions_total > 0
             AND (CAST(executions_failed AS DECIMAL(8,4)) / CAST(executions_total AS DECIMAL(8,4))) > 0.20
            THEN 1 ELSE 0
        END AS is_high_failure_rate

    FROM with_errors
),

with_health AS (
    SELECT
        *,

        CAST(
            -- Success rate component (40 pts)
            (CASE
                WHEN executions_total > 0
                THEN (CAST(executions_success AS DECIMAL(8,4)) / CAST(executions_total AS DECIMAL(8,4))) * 40
                ELSE 0
            END)
            +
            -- Volume consistency component (30 pts)
            (CASE
                WHEN volume_variation_pct IS NULL        THEN 30
                WHEN ABS(volume_variation_pct) <= 10     THEN 30
                WHEN ABS(volume_variation_pct) <= 25     THEN 20
                WHEN ABS(volume_variation_pct) <= 50     THEN 10
                ELSE 0
            END)
            +
            -- No anomalies component (30 pts)
            (30 - (is_volume_anomaly * 10) - (is_zero_records * 10) - (is_high_failure_rate * 10))
        AS DECIMAL(5, 2)) AS health_score,

        -- SLA compliance (NULL for snapshots)
        CASE
            WHEN entity_type = 'snapshot'                                THEN CAST(NULL AS BIT)
            WHEN last_success_datetime IS NULL                           THEN CAST(0 AS BIT)
            WHEN hours_since_success_eod <= expected_frequency_hrs       THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
        END AS sla_met,

        CASE
            WHEN entity_type = 'snapshot'
                THEN 'N/A - snapshot file'
            WHEN last_success_datetime IS NULL
                THEN 'No successful execution recorded'
            WHEN hours_since_success_eod > expected_frequency_hrs
                THEN 'Last success was '  + CAST(hours_since_success_eod  AS VARCHAR(10))
                   + ' hours ago (expected: ' + CAST(expected_frequency_hrs AS VARCHAR(10)) + 'h)'
            ELSE NULL
        END AS sla_breach_reason

    FROM with_derived
)

SELECT
    event_date,
    entity_name,
    domain_final         AS domain,       -- CAMBIO 1: usar domain reclasificado
    source,                                -- CAMBIO 2: nueva columna source
    entity_type,
    entity_base_name,
    snapshot_file_date,
    records_processed,
    records_failed,
    executions_total,
    executions_success,
    executions_failed,
    executions_skipped,
    baseline_7d_avg,
    baseline_7d_stddev,
    volume_variation_pct,
    volume_zscore,
    health_score,

    CASE
        WHEN executions_total = 0                                  THEN 'NO_DATA'
        WHEN entity_type = 'snapshot' AND executions_success > 0   THEN 'HEALTHY'
        WHEN entity_type = 'snapshot' AND executions_failed > 0    THEN 'CRITICAL'
        WHEN entity_type = 'snapshot'                              THEN 'WARNING'
        WHEN health_score >= 80                                    THEN 'HEALTHY'
        WHEN health_score >= 50                                    THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS health_status,

    is_volume_anomaly,
    is_zero_records,
    is_high_failure_rate,
    executions_failed    AS error_count,
    error_rate_pct,
    final_error_message  AS last_error_message,
    final_error_datetime AS last_error_datetime,
    last_success_datetime,
    hours_since_success_eod AS hours_since_success,
    expected_frequency_hrs,
    sla_met,
    sla_breach_reason

FROM with_health
