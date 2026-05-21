-- models/fct_route_scd.sql
-- Propósito: SCD Type 2 manual para dimensión de rutas (reemplaza snap_dim_routes)
-- Fuente: ref('dimRoutes')
-- Grain: (route_operator_key, valid_from) — una fila por versión de cada ruta+operador
-- Owner: analytics-team | Actualizado: 2026-05-21

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        dist='HASH(route_operator_key)',
        index='CLUSTERED COLUMNSTORE INDEX',
        tag='analytics',
        pre_hook="{% if is_incremental() %}
            UPDATE {{ this }}
            SET
                is_current   = CAST(0 AS BIT),
                valid_to     = GETDATE()
            WHERE is_current = CAST(1 AS BIT)
              AND route_operator_key IN (
                  SELECT CAST(src.operator_id AS VARCHAR(50)) + '_' + CAST(src.route_id AS VARCHAR(50))
                  FROM {{ ref('dimRoutes') }} src
                  INNER JOIN {{ this }} tgt
                    ON  CAST(src.operator_id AS VARCHAR(50)) + '_' + CAST(src.route_id AS VARCHAR(50)) = tgt.route_operator_key
                    AND tgt.is_current = CAST(1 AS BIT)
                  WHERE src.route_name <> tgt.route_name
              )
        {% endif %}"
    )
}}

WITH source AS (
    SELECT
        CAST(operator_id AS VARCHAR(50)) + '_' + CAST(route_id AS VARCHAR(50)) AS route_operator_key,
        operator_id,
        route_id,
        route_name
    FROM {{ ref('dimRoutes') }}
),

{% if is_incremental() %}
changed AS (
    SELECT
        src.route_operator_key,
        src.operator_id,
        src.route_id,
        src.route_name
    FROM source src
    LEFT JOIN {{ this }} tgt
      ON  src.route_operator_key = tgt.route_operator_key
      AND tgt.is_current         = CAST(1 AS BIT)
    WHERE
        tgt.route_operator_key IS NULL         -- ruta nueva
        OR src.route_name <> tgt.route_name    -- cambió de nombre
),
{% endif %}

final AS (
    SELECT
        NEWID()                       AS scd_id,
        route_operator_key,
        operator_id,
        route_id,
        route_name,
        GETDATE()                     AS valid_from,
        CAST(NULL AS DATETIME2)       AS valid_to,
        CAST(1 AS BIT)                AS is_current,
        GETDATE()                     AS dbt_updated_at
    FROM {% if is_incremental() %} changed {% else %} source {% endif %}
)

SELECT * FROM final
