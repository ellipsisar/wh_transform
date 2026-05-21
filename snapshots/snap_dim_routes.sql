-- snapshots/snap_dim_routes.sql
-- Propósito: SCD Type 2 snapshot de la dimensión de rutas
-- Fuente: ref('dimRoutes')
-- Owner: analytics-team | Actualizado: 2026-05-21

{% snapshot snap_dim_routes %}

{{
    config(
        target_schema='snapshots',
        unique_key='route_operator_key',
        strategy='check',
        check_cols=['route_name'],
        dist='ROUND_ROBIN',
        index='CLUSTERED COLUMNSTORE INDEX'
    )
}}

SELECT
    CAST(operator_id AS VARCHAR(50)) + '_' + CAST(route_id AS VARCHAR(50)) AS route_operator_key,
    operator_id,
    route_id,
    route_name
FROM {{ ref('dimRoutes') }}

{% endsnapshot %}
