-- models/fct_operator_scd.sql
-- Propósito: SCD Type 2 manual para dimensión de operadores (reemplaza snap_dim_operators)
-- Fuente: ref('dimOperators')
-- Grain: (operator_id, valid_from) — una fila por versión de cada operador
-- Owner: analytics-team | Actualizado: 2026-05-21

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        dist='HASH(operator_id)',
        index='CLUSTERED COLUMNSTORE INDEX',
        tag='analytics',
        pre_hook="{% if is_incremental() %}
            UPDATE {{ this }}
            SET
                is_current   = CAST(0 AS BIT),
                valid_to     = GETDATE()
            WHERE is_current = CAST(1 AS BIT)
              AND operator_id IN (
                  SELECT src.operator_id
                  FROM {{ ref('dimOperators') }} src
                  INNER JOIN {{ this }} tgt
                    ON  src.operator_id   = tgt.operator_id
                    AND tgt.is_current    = CAST(1 AS BIT)
                  WHERE src.operator_name <> tgt.operator_name
              )
        {% endif %}"
    )
}}

WITH source AS (
    SELECT
        operator_id,
        operator_name
    FROM {{ ref('dimOperators') }}
),

{% if is_incremental() %}
changed AS (
    SELECT
        src.operator_id,
        src.operator_name
    FROM source src
    LEFT JOIN {{ this }} tgt
      ON  src.operator_id = tgt.operator_id
      AND tgt.is_current  = CAST(1 AS BIT)
    WHERE
        tgt.operator_id IS NULL              -- operador nuevo
        OR src.operator_name <> tgt.operator_name  -- cambió de nombre
),
{% endif %}

final AS (
    SELECT
        NEWID()                       AS scd_id,
        operator_id,
        operator_name,
        GETDATE()                     AS valid_from,
        CAST(NULL AS DATETIME2)       AS valid_to,
        CAST(1 AS BIT)                AS is_current,
        GETDATE()                     AS dbt_updated_at
    FROM {% if is_incremental() %} changed {% else %} source {% endif %}
)

SELECT * FROM final
