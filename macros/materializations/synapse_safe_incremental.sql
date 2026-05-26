{#
  synapse_safe_incremental
  ========================
  Materialización incremental custom para Azure Synapse Dedicated SQL Pool.

  PROBLEMA:
    Synapse T-SQL rechaza INSERT INTO ... WITH cte AS (...) SELECT, que es
    exactamente el SQL que dbt genera con estrategias estándar (append o
    delete+insert) cuando hay modelos ephemeral como dependencias: los CTEs
    se inyectan antes del SELECT pero después del INSERT INTO, produciendo
    sintaxis inválida.

  SOLUCIÓN:
    Usar CTAS (CREATE TABLE AS SELECT) para la staging temporal — los CTEs
    son válidos en Synapse CTAS porque aparecen como parte del query después
    del AS, no antes de un DML. Luego DELETE + INSERT desde la temporal no
    incluyen CTEs y son T-SQL válido.

  FLUJO POR EJECUCIÓN:
    1ª vez / --full-refresh:
      CREATE TABLE [target] WITH (...) AS [model SQL con CTEs]

    Ejecuciones incrementales:
      1. CREATE TABLE [target__inc_tmp] WITH (...) AS [model SQL con CTEs]
      2. DELETE target FROM target JOIN tmp ON unique_keys
      3. INSERT INTO target SELECT FROM tmp
      4. DROP TABLE tmp

  USO:
    config(
      materialized = 'synapse_safe_incremental',
      unique_key   = ['col1', 'col2'],
      dist         = 'ROUND_ROBIN',
      index        = 'CLUSTERED COLUMNSTORE INDEX'
    )

    Usar is_incremental_safe() en lugar de is_incremental() en el SQL del modelo.
#}

{% materialization synapse_safe_incremental, adapter='synapse' %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set tmp_relation    = target_relation.incorporate(
        identifier = target_relation.identifier ~ '__inc_tmp') -%}

  {%- set unique_key_raw  = config.require('unique_key') -%}
  {%- set unique_key_cols = [unique_key_raw] if unique_key_raw is string else unique_key_raw -%}

  {{ run_hooks(pre_hooks) }}

  {%- set existing_relation = load_relation(this) -%}
  {%- set is_first_run      = (existing_relation is none) or flags.FULL_REFRESH -%}

  {% if is_first_run %}

    {%- if existing_relation is not none %}
      {% do adapter.drop_relation(existing_relation) %}
    {%- endif %}

    {# CTAS directo — CTEs del model SQL son válidos en Synapse CTAS #}
    {% call statement('main') %}
      {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

  {% else %}

    {# Limpiar tabla temporal si quedó de una ejecución anterior fallida #}
    {%- set existing_tmp = load_relation(tmp_relation) -%}
    {%- if existing_tmp is not none %}
      {% do adapter.drop_relation(existing_tmp) %}
    {%- endif %}

    {# Paso 1: CTAS a tabla temporal — CTEs válidos en Synapse CTAS #}
    {% call statement('create_tmp') %}
      {{ create_table_as(False, tmp_relation, sql) }}
    {% endcall %}

    {# Paso 2: Borrar del destino las filas que serán reemplazadas — sin CTEs #}
    {% call statement('delete_target') %}
      DELETE target
      FROM {{ target_relation }} AS target
      INNER JOIN {{ tmp_relation }} AS tmp
          ON {% for col in unique_key_cols -%}
             target.{{ col }} = tmp.{{ col }}
             {%- if not loop.last %} AND {% endif %}
             {%- endfor %}
    {% endcall %}

    {# Paso 3: Insertar desde temporal — INSERT sin CTEs, T-SQL válido #}
    {%- set dest_columns  = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {% call statement('insert_target') %}
      INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
      SELECT {{ dest_cols_csv }} FROM {{ tmp_relation }}
    {% endcall %}

    {# Paso 4: Limpiar tabla temporal #}
    {% call statement('drop_tmp') %}
      DROP TABLE {{ tmp_relation }}
    {% endcall %}

  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
