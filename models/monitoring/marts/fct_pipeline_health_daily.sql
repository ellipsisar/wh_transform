{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    dist                 = 'ROUND_ROBIN',
    index                = 'CLUSTERED COLUMNSTORE INDEX',
    pre_hook             = [
      "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring') EXEC('CREATE SCHEMA [monitoring]')",
      "{% if is_incremental() %}DELETE FROM {{ this }} WHERE event_date >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)){% endif %}"
    ]
  )
}}

/*
  fct_pipeline_health_daily
  -------------------------
  Unica tabla materializada del modelo de monitoreo de ingesta.

  Granularidad : una fila por (event_date, entity_name, domain)
  Estrategia   : synapse_safe_incremental (custom), lookback 7 dias
  Distribucion : ROUND_ROBIN  (volumen bajo, queries analiticos)

  Todos los modelos staging e intermediate son ephemeral; se compilan
  como CTEs inline en este query por el compilador de dbt.

  La materialización custom resuelve la limitación de Synapse T-SQL donde
  INSERT INTO ... WITH cte es inválido. Usa CTAS para la tabla temporal
  (CTEs válidos en CTAS) y luego DELETE + INSERT sin CTEs.
  Ver macros/materializations/synapse_safe_incremental.sql.
*/

SELECT
    -- Dimensiones de granularidad
    event_date,
    CAST(entity_name             AS VARCHAR(255))  AS entity_name,
    CAST(domain                  AS VARCHAR(50))   AS domain,

    -- Metricas de volumen
    CAST(records_processed       AS BIGINT)        AS records_processed,
    CAST(records_failed          AS BIGINT)        AS records_failed,
    CAST(executions_total        AS INT)           AS executions_total,
    CAST(executions_success      AS INT)           AS executions_success,
    CAST(executions_failed       AS INT)           AS executions_failed,
    CAST(executions_skipped      AS INT)           AS executions_skipped,

    -- Baseline y variacion
    CAST(baseline_7d_avg         AS DECIMAL(18,2)) AS baseline_7d_avg,
    CAST(baseline_7d_stddev      AS DECIMAL(18,2)) AS baseline_7d_stddev,
    CAST(volume_variation_pct    AS DECIMAL(18,4)) AS volume_variation_pct,
    CAST(volume_zscore           AS DECIMAL(18,4)) AS volume_zscore,

    -- Salud y anomalias
    CAST(health_score            AS DECIMAL(5,2))  AS health_score,
    CAST(health_status           AS VARCHAR(20))   AS health_status,
    CAST(is_volume_anomaly       AS BIT)           AS is_volume_anomaly,
    CAST(is_zero_records         AS BIT)           AS is_zero_records,
    CAST(is_high_failure_rate    AS BIT)           AS is_high_failure_rate,

    -- Metricas de error
    CAST(error_count             AS INT)           AS error_count,
    CAST(error_rate_pct          AS DECIMAL(8,4))  AS error_rate_pct,
    CAST(last_error_message      AS VARCHAR(4000)) AS last_error_message,
    CAST(last_error_datetime     AS DATETIME2)     AS last_error_datetime,

    -- SLA y freshness
    CAST(last_success_datetime   AS DATETIME2)     AS last_success_datetime,
    CAST(hours_since_success     AS INT)           AS hours_since_success,
    CAST(expected_frequency_hrs  AS INT)           AS expected_frequency_hrs,
    CAST(sla_met                 AS BIT)           AS sla_met,
    CAST(sla_breach_reason       AS VARCHAR(255))  AS sla_breach_reason,

    -- Metadata
    CAST(GETDATE()               AS DATETIME2)     AS dbt_updated_at,
    CAST('{{ invocation_id }}'   AS VARCHAR(100))  AS dbt_run_id

FROM {{ ref('int_health_metrics') }}

{% if is_incremental() %}
WHERE event_date >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
{% endif %}
