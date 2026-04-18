{#
  Macro: ama_backfill_gps_comunicacion
  Rellena AMA_Geotab_GPS_Comunicacion para los últimos N días hacia atrás.
  Ejecuta DELETE + INSERT por cada día del rango, usando la lógica del modelo.

  Uso:
    dbt run-operation --profiles-dir . ama_backfill_gps_comunicacion --args '{days: 30}'

  Note: ref() y source() no están disponibles en run-operation; los nombres
  de tabla se construyen con api.Relation.create().
#}

{% macro ama_backfill_gps_comunicacion(days=30) %}

    {% set target_rel = api.Relation.create(
        database   = target.database,
        schema     = target.schema,
        identifier = 'AMA_Geotab_GPS_Comunicacion'
    ) %}

    {% set device_rel = api.Relation.create(
        database   = target.database,
        schema     = 'staging',
        identifier = 'geotab_device'
    ) %}

    {% set status_rel = api.Relation.create(
        database   = target.database,
        schema     = 'staging',
        identifier = 'geotab_device_status_info'
    ) %}

    {% set log_rel = api.Relation.create(
        database   = target.database,
        schema     = 'staging',
        identifier = 'geotab_log_record'
    ) %}

    {% set trip_rel = api.Relation.create(
        database   = target.database,
        schema     = 'staging',
        identifier = 'geotab_trip'
    ) %}

    {% for offset in range(days, 0, -1) %}
        {% set backfill_date = modules.datetime.date.today() - modules.datetime.timedelta(days=offset) %}
        {% set d = backfill_date.strftime('%Y-%m-%d') %}
        {% set d_end = d ~ ' 23:59:59' %}

        {{ log("==> Backfill GPS_Comunicacion: " ~ d, info=True) }}

        -- Eliminar snapshot existente para este día (idempotente)
        {% set delete_sql %}
            DELETE FROM {{ target_rel }}
            WHERE snapshot_date = CAST('{{ d }}' AS DATE)
        {% endset %}
        {% do run_query(delete_sql) %}

        -- Insertar snapshot del día
        {% set insert_sql %}
            WITH
            cte_ultimo_status AS (
                SELECT
                    device_id,
                    status_datetime,
                    is_device_communicating,
                    is_driving,
                    speed,
                    latitude,
                    longitude
                FROM (
                    SELECT
                        device_id,
                        status_datetime,
                        is_device_communicating,
                        is_driving,
                        speed,
                        latitude,
                        longitude,
                        ROW_NUMBER() OVER (
                            PARTITION BY device_id
                            ORDER BY status_datetime DESC
                        ) AS rn
                    FROM {{ status_rel }}
                    WHERE status_datetime <= CAST('{{ d_end }}' AS DATETIME)
                ) x
                WHERE rn = 1
            ),
            cte_ultimo_gps AS (
                SELECT
                    device_id,
                    MAX(log_datetime) AS ultima_posicion_gps,
                    COUNT(*)          AS total_pings_historico
                FROM {{ log_rel }}
                WHERE log_datetime <= CAST('{{ d_end }}' AS DATETIME)
                GROUP BY device_id
            ),
            cte_viajes AS (
                SELECT
                    device_id,
                    MAX(trip_start)                                                             AS ultimo_viaje,
                    SUM(CASE WHEN trip_start >= DATEADD(DAY, -30, CAST('{{ d_end }}' AS DATETIME))
                             THEN 1 ELSE 0 END)                                                AS viajes_ultimos_30_dias,
                    SUM(CASE WHEN trip_start >= DATEADD(DAY, -7,  CAST('{{ d_end }}' AS DATETIME))
                             THEN 1 ELSE 0 END)                                                AS viajes_ultimos_7_dias,
                    SUM(CASE WHEN trip_start >= DATEADD(DAY, -30, CAST('{{ d_end }}' AS DATETIME))
                             THEN distance_km ELSE 0.0 END)                                    AS km_ultimos_30_dias
                FROM {{ trip_rel }}
                WHERE trip_start <= CAST('{{ d_end }}' AS DATETIME)
                GROUP BY device_id
            )
            INSERT INTO {{ target_rel }} (
                snapshot_date,
                device_id,
                device_name,
                device_comment,
                ultima_fecha_de_comunicacion,
                is_device_communicating,
                dias_sin_comunicacion,
                ultima_posicion_gps,
                dias_sin_gps,
                esta_conduciendo,
                velocidad_actual_kmh,
                ultima_latitud,
                ultima_longitud,
                ultimo_viaje,
                dias_sin_viaje,
                viajes_ultimos_7_dias,
                viajes_ultimos_30_dias,
                km_ultimos_30_dias,
                total_pings_gps_historico,
                dispositivo_activo_desde,
                dispositivo_activo_hasta,
                es_dispositivo_vigente,
                categoria_comunicacion,
                categoria_gps
            )
            SELECT
                CAST('{{ d }}' AS DATE)                                                         AS snapshot_date,
                d.device_id,
                d.device_name,
                d.comment                                                                       AS device_comment,
                CONVERT(DATETIME, si.status_datetime)                                           AS ultima_fecha_de_comunicacion,
                CONVERT(BIT, ISNULL(si.is_device_communicating, 0))                             AS is_device_communicating,
                DATEDIFF(DAY, si.status_datetime, CAST('{{ d_end }}' AS DATETIME))              AS dias_sin_comunicacion,
                CONVERT(DATETIME, g.ultima_posicion_gps)                                        AS ultima_posicion_gps,
                DATEDIFF(DAY, g.ultima_posicion_gps, CAST('{{ d_end }}' AS DATETIME))           AS dias_sin_gps,
                CONVERT(BIT, ISNULL(si.is_driving, 0))                                          AS esta_conduciendo,
                si.speed                                                                        AS velocidad_actual_kmh,
                si.latitude                                                                     AS ultima_latitud,
                si.longitude                                                                    AS ultima_longitud,
                CONVERT(DATETIME, v.ultimo_viaje)                                               AS ultimo_viaje,
                DATEDIFF(DAY, v.ultimo_viaje, CAST('{{ d_end }}' AS DATETIME))                  AS dias_sin_viaje,
                ISNULL(v.viajes_ultimos_7_dias,  0)                                             AS viajes_ultimos_7_dias,
                ISNULL(v.viajes_ultimos_30_dias, 0)                                             AS viajes_ultimos_30_dias,
                ISNULL(v.km_ultimos_30_dias,     0.0)                                           AS km_ultimos_30_dias,
                ISNULL(g.total_pings_historico,  0)                                             AS total_pings_gps_historico,
                CONVERT(DATE, d.active_from)                                                    AS dispositivo_activo_desde,
                CONVERT(DATE, d.active_to)                                                      AS dispositivo_activo_hasta,
                CONVERT(BIT,
                    CASE WHEN d.active_to IS NULL OR d.active_to > CAST('{{ d_end }}' AS DATETIME)
                         THEN 1 ELSE 0 END
                )                                                                               AS es_dispositivo_vigente,
                CASE
                    WHEN si.device_id IS NULL                                                    THEN 'Sin registro de estado'
                    WHEN ISNULL(si.is_device_communicating, 0) = 1                              THEN 'Comunicando'
                    WHEN DATEDIFF(DAY, si.status_datetime, CAST('{{ d_end }}' AS DATETIME)) = 0 THEN 'Sin comunicacion (hoy)'
                    WHEN DATEDIFF(DAY, si.status_datetime, CAST('{{ d_end }}' AS DATETIME)) BETWEEN 1 AND 1
                                                                                                THEN 'Sin comunicacion (1 dia)'
                    WHEN DATEDIFF(DAY, si.status_datetime, CAST('{{ d_end }}' AS DATETIME)) BETWEEN 2 AND 7
                                                                                                THEN 'Sin comunicacion (2-7 dias)'
                    WHEN DATEDIFF(DAY, si.status_datetime, CAST('{{ d_end }}' AS DATETIME)) BETWEEN 8 AND 30
                                                                                                THEN 'Sin comunicacion (8-30 dias)'
                    ELSE 'Sin comunicacion (mas de 30 dias)'
                END                                                                             AS categoria_comunicacion,
                CASE
                    WHEN g.device_id IS NULL                                                     THEN 'Sin registros GPS'
                    WHEN DATEDIFF(DAY, g.ultima_posicion_gps, CAST('{{ d_end }}' AS DATETIME)) = 0
                                                                                                THEN 'GPS activo (hoy)'
                    WHEN DATEDIFF(DAY, g.ultima_posicion_gps, CAST('{{ d_end }}' AS DATETIME)) BETWEEN 1 AND 7
                                                                                                THEN 'GPS inactivo (1-7 dias)'
                    WHEN DATEDIFF(DAY, g.ultima_posicion_gps, CAST('{{ d_end }}' AS DATETIME)) BETWEEN 8 AND 30
                                                                                                THEN 'GPS inactivo (8-30 dias)'
                    ELSE 'GPS inactivo (mas de 30 dias)'
                END                                                                             AS categoria_gps
            FROM {{ device_rel }} d
                LEFT JOIN cte_ultimo_status si ON d.device_id = si.device_id
                LEFT JOIN cte_ultimo_gps     g ON d.device_id = g.device_id
                LEFT JOIN cte_viajes         v ON d.device_id = v.device_id
            WHERE
                (d.active_to IS NULL OR d.active_to > CAST('{{ d_end }}' AS DATETIME))
        {% endset %}
        {% do run_query(insert_sql) %}

    {% endfor %}

    {{ log("Backfill completado para los ultimos " ~ days ~ " dias.", info=True) }}

{% endmacro %}
