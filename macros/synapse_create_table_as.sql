{% macro synapse__create_table_as(temporary, relation, sql) %}
  {#
    Override del macro de dbt-synapse para usar CTAS directo en lugar del patrón
    EXEC(CREATE VIEW) + EXEC(CREATE TABLE AS SELECT FROM view).

    Problema raíz: el adapter crea un VIEW temporal del SQL del modelo via
    EXEC('CREATE VIEW ... AS [sql]') antes de hacer CTAS. Cuando el SQL contiene
    CTEs (por inlining de modelos ephemeral), Synapse falla con:
      "Incorrect syntax near 'WITH'" (error 103010)

    Synapse Dedicated SQL Pool sí soporta CTEs directamente en CTAS:
      CREATE TABLE ... WITH (...) AS WITH cte AS (...) SELECT ...

    Este override evita el VIEW intermedio y usa CTAS directo, resolviendo
    el error sin cambiar la arquitectura de modelos ephemeral.
  #}
  {%- set dist  = config.get('dist',  default='ROUND_ROBIN') -%}
  {%- set index = 'HEAP' if temporary else config.get('index', default='CLUSTERED COLUMNSTORE INDEX') -%}

  CREATE TABLE {{ relation.include(database=False) }}
  WITH (
    DISTRIBUTION = {{ dist }},
    {{ index }}
  )
  AS
  {{ sql }}
{% endmacro %}
