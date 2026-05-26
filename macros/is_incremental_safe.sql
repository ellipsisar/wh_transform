{#
  is_incremental_safe()
  =====================
  Equivalente a is_incremental() para la materialización synapse_safe_incremental.

  is_incremental() verifica que config.materialized == 'incremental', lo cual
  falla para materializaciones custom. Este macro omite ese chequeo y evalúa
  directamente si la tabla destino existe y no se está haciendo --full-refresh.

  Retorna True cuando:
    - La tabla destino ya existe en la base de datos
    - No se está ejecutando con --full-refresh

  Usar en modelos con materialized='synapse_safe_incremental' en lugar de
  is_incremental().
#}
{% macro is_incremental_safe() %}
  {%- set existing_relation = load_relation(this) -%}
  {{ (existing_relation is not none) and (not flags.FULL_REFRESH) }}
{% endmacro %}
