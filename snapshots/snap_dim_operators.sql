-- snapshots/snap_dim_operators.sql
-- Propósito: SCD Type 2 snapshot de la dimensión de operadores
-- Fuente: ref('dimOperators')
-- Owner: analytics-team | Actualizado: 2026-05-21

{% snapshot snap_dim_operators %}

{{
    config(
        target_schema='snapshots',
        unique_key='operator_id',
        strategy='check',
        check_cols=['operator_name'],
        dist='ROUND_ROBIN',
        index='CLUSTERED COLUMNSTORE INDEX'
    )
}}

SELECT
    operator_id,
    operator_name
FROM {{ ref('dimOperators') }}

{% endsnapshot %}
