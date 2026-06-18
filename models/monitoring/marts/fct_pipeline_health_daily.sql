{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    dist                 = 'ROUND_ROBIN',
    index                = 'CLUSTERED COLUMNSTORE INDEX',
    pre_hook             = [
      "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'monitoring') EXEC('CREATE SCHEMA [monitoring]')",
      "{% if is_incremental() %}DELETE FROM {{ this }} WHERE event_date >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)){% endif %}"
    ],
    post_hook            = [
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_event_date' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_event_date ON {{ this }} (event_date)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_domain' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_domain ON {{ this }} (domain)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_entity_name' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_entity_name ON {{ this }} (entity_name)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_entity_base_name' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_entity_base_name ON {{ this }} (entity_base_name)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_health_status' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_health_status ON {{ this }} (health_status)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_zscore' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_zscore ON {{ this }} (volume_zscore)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_sla_met' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_sla_met ON {{ this }} (sla_met)'",
      "IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = 'stat_phd_event_date_domain' AND object_id = OBJECT_ID('{{ this }}')) EXEC sp_executesql N'CREATE STATISTICS stat_phd_event_date_domain ON {{ this }} (event_date, domain)'"
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
    CAST(source                  AS VARCHAR(50))   AS source,  -- CAMBIO 2: nueva columna source
    CAST(entity_type             AS VARCHAR(20))   AS entity_type,
    CAST(entity_base_name        AS VARCHAR(255))  AS entity_base_name,
    CAST(snapshot_file_date      AS DATE)          AS snapshot_file_date,

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
    CAST(ABS(volume_zscore)      AS DECIMAL(18,4)) AS abs_volume_zscore,

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
    CASE WHEN expected_frequency_hrs IS NOT NULL THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_recurring,
    CAST(sla_met                 AS BIT)           AS sla_met,
    CAST(sla_breach_reason       AS VARCHAR(255))  AS sla_breach_reason,

    -- Metadata
    CAST(GETDATE()               AS DATETIME2)     AS dbt_updated_at,
    CAST('{{ invocation_id }}'   AS VARCHAR(100))  AS dbt_run_id

FROM {{ ref('int_health_metrics') }}

{% if is_incremental() %}
WHERE event_date >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
{% endif %}
