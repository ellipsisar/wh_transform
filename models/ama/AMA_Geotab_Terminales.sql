{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(zone_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['dashboard_AMA','terminales_AMA'],
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE event_date >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

/*
  Fuente principal: staging.geotab_planned_vs_actual
  - Cada fila = una parada planificada en una ruta para un vehículo.
  - actual_arrival_utc IS NOT NULL → el vehículo visitó físicamente la zona.
  - La tabla se actualiza en múltiples snapshots diarios; se deduplica por
    (device_id, route_id, stop_sequence, actual_arrival_utc) usando _md_filename DESC.
  - Solo se incluyen filas con visita real (actual_arrival_utc no nulo).

  Integración con AMA_Itinerario:
  - Se cruza tren = device_name + orden_parada = stop_sequence + tipo de día.
  - Para cada visita se evalúan candidatos IDA y VUELTA; se elige el de menor
    diferencia absoluta entre hora programada y hora real:
      IDA   → compara hora itinerario vs. actual_departure_local
      VUELTA → compara hora itinerario vs. actual_arrival_local
  - departure_status/delay_minutes (IDA) y arrival_status/arrival_delay_minutes (VUELTA)
    se calculan independientemente: cte_itinerario_best particiona por (visita, direccion)
    y en el SELECT final se une dos veces (ib_dep para IDA, ib_arr para VUELTA).
  - Zona horaria: se usan directamente actual_arrival_local / actual_departure_local
    (ya expresadas en America/Puerto_Rico). Las columnas UTC se conservan solo para
    terminal_entry_datetime / terminal_exit_datetime (output) y el join con geotab_trip.
*/

WITH cte_deduped AS (
    -- Eliminar duplicados por snapshot: conservar una fila por visita única
    SELECT
        route_id,
        route_name,
        device_id,
        device_name,
        stop_sequence,
        stop_name,
        arrival_zone_id,
        arrival_zone_name,
        CAST(actual_arrival_utc   AS DATETIME)    AS actual_arrival_utc,
        CAST(actual_departure_utc AS DATETIME)    AS actual_departure_utc,
        CAST(LEFT(REPLACE(actual_arrival_local,'T',' '),19) AS DATETIME) AS actual_arrival_local,
        CAST(LEFT(REPLACE(actual_departure_local,'T',' '),19) AS DATETIME) AS actual_departure_local,
        status,
        note,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, route_id, stop_sequence, actual_arrival_utc
            ORDER BY _md_filename DESC
        ) AS rn
    FROM {{ source('geotab', 'geotab_planned_vs_actual') }}
    WHERE actual_arrival_local IS NOT NULL
    {% if is_incremental() %}
      AND CAST(actual_arrival_local AS DATE) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

cte_visits AS (
    SELECT *
    FROM cte_deduped
    WHERE rn = 1
),

cte_device AS (
    SELECT device_id, device_type
    FROM {{ source('geotab', 'geotab_device') }}
),

cte_itinerario AS (
    -- Snapshot más reciente de AMA_Itinerario: todas las paradas y direcciones.
    -- tren cast a VARCHAR para que coincida con device_name.
    SELECT
        CAST(tren AS VARCHAR(50)) AS tren,
        orden_parada,
        hora,
        direccion,
        servicio
    FROM {{ source('ama', 'AMA_Itinerario') }}
    WHERE _md_snapshot_date = (
        SELECT MAX(_md_snapshot_date)
        FROM {{ source('ama', 'AMA_Itinerario') }}
    )
),

cte_itinerario_candidatos AS (
    -- Para cada visita real se generan candidatos de itinerario (IDA y VUELTA)
    -- que coincidan en tren + parada + tipo de día.
    -- La hora programada se construye combinando la fecha LOCAL del evento con
    -- el campo hora del itinerario (formato H:MM, 24 h, sin ceros a la izquierda).
    -- diff_minutos: varianza en minutos entre programado y real:
    --   IDA   → real = actual_departure_local  (el vehículo debe salir de la parada)
    --   VUELTA → real = actual_arrival_local    (el vehículo debe llegar a la parada)
    SELECT
        v.device_id,
        v.route_id,
        v.stop_sequence,
        v.actual_arrival_utc,
        i.direccion,
        i.hora                                          AS scheduled_hora,

        -- Hora programada en tiempo local (fecha local del evento + hora del itinerario)
        DATEADD(MINUTE,
            CAST(SUBSTRING(i.hora, CHARINDEX(':', i.hora) + 1, 2) AS INT),
            DATEADD(HOUR,
                CAST(LEFT(i.hora, CHARINDEX(':', i.hora) - 1) AS INT),
                CAST(CAST(v.actual_arrival_local AS DATE) AS DATETIME)
            )
        )                                               AS scheduled_datetime_local,

        -- Varianza: positivo = tarde, negativo = adelantado
        DATEDIFF(MINUTE,
            DATEADD(MINUTE,
                CAST(SUBSTRING(i.hora, CHARINDEX(':', i.hora) + 1, 2) AS INT),
                DATEADD(HOUR,
                    CAST(LEFT(i.hora, CHARINDEX(':', i.hora) - 1) AS INT),
                    CAST(CAST(v.actual_arrival_local AS DATE) AS DATETIME)
                )
            ),
            CASE i.direccion
                WHEN 'IDA'    THEN v.actual_departure_local
                WHEN 'VUELTA' THEN v.actual_arrival_local
            END
        )                                               AS diff_minutos

    FROM cte_visits v
    JOIN cte_itinerario i
        ON  i.tren        = v.device_name
        AND i.orden_parada = v.stop_sequence
        AND i.servicio    = CASE
            WHEN DATEPART(dw, v.actual_arrival_local) = 1 THEN 'DOMINGO - DO'
            WHEN DATEPART(dw, v.actual_arrival_local) = 7 THEN 'SABADO - SA'
            ELSE 'LUNES-VIERNES - LV'
          END
    -- Excluir candidatos donde el tiempo de referencia sea nulo
    WHERE (
        (i.direccion = 'IDA'    AND v.actual_departure_local IS NOT NULL)
        OR
        (i.direccion = 'VUELTA' AND v.actual_arrival_local IS NOT NULL)
    )
),

cte_itinerario_best AS (
    -- Conservar el mejor candidato POR DIRECCIÓN (IDA y VUELTA por separado).
    -- Esto permite evaluar independientemente:
    --   ib_dep (IDA)    → departure_status / delay_minutes
    --   ib_arr (VUELTA) → arrival_status   / arrival_delay_minutes
    -- trip_direction se resuelve en el SELECT final comparando ABS(diff_minutos) de ambas.
    SELECT
        device_id,
        route_id,
        stop_sequence,
        actual_arrival_utc,
        direccion,
        scheduled_hora,
        scheduled_datetime_local,
        diff_minutos
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY device_id, route_id, stop_sequence, actual_arrival_utc, direccion
                ORDER BY ABS(diff_minutos) ASC
            ) AS rn
        FROM cte_itinerario_candidatos
    ) ranked
    WHERE rn = 1
),


SELECT
    -- Evento
    CAST(v.actual_arrival_local AS DATE)                    AS event_date,
    DATEPART(HOUR,    v.actual_arrival_local)               AS entry_hour,
    DATEPART(WEEKDAY, v.actual_arrival_local)               AS entry_weekday,

    -- Zona / Terminal (resueltos directamente desde planned_vs_actual)
    v.arrival_zone_id                                       AS zone_id,
    v.arrival_zone_name                                     AS terminal_name,

    -- Indicador de terminal central (Estacion Martinez Nadal)
    CAST(
        CASE
            WHEN v.arrival_zone_name = 'Estacion Martinez Nadal (Abordaje/Descenso), San Juan, 00921, Puerto Rico'
            THEN 1 ELSE 0
        END
    AS BIT)                                                 AS is_central,

    -- Vehículo
    v.device_id                                             AS vehicle_id,
    v.device_name                                           AS vehicle_name,
    d.device_type                                           AS vehicle_type,

    -- Ruta (resuelta directamente desde planned_vs_actual)
    v.route_id,
    v.route_name,

    -- Parada en la ruta
    v.stop_sequence,
    v.stop_name,

    -- Dirección del servicio según itinerario (IDA / VUELTA / NULL si sin match en ninguna).
    -- Se elige la dirección cuyo candidato tenga menor varianza absoluta.
    CASE
        WHEN ib_dep.diff_minutos IS NULL AND ib_arr.diff_minutos IS NULL THEN NULL
        WHEN ib_dep.diff_minutos IS NULL                                 THEN 'VUELTA'
        WHEN ib_arr.diff_minutos IS NULL                                 THEN 'IDA'
        WHEN ABS(ib_dep.diff_minutos) <= ABS(ib_arr.diff_minutos)        THEN 'IDA'
        ELSE                                                                  'VUELTA'
    END                                                     AS trip_direction,

    -- Tiempos del evento en terminal
    v.actual_arrival_utc                                    AS terminal_entry_datetime,
    v.actual_departure_utc                                  AS terminal_exit_datetime,
    CASE
        WHEN v.actual_arrival_utc IS NOT NULL
         AND v.actual_departure_utc IS NOT NULL
        THEN CAST(
            DATEDIFF(SECOND, v.actual_arrival_utc, v.actual_departure_utc) / 60.0
            AS DECIMAL(10, 2))
        ELSE NULL
    END                                                     AS dwell_time_minutes,

    -- Clasificación de permanencia
    CASE
        WHEN v.actual_departure_utc IS NULL
            THEN 'SIN_DATOS'
        WHEN DATEDIFF(SECOND, v.actual_arrival_utc, v.actual_departure_utc) / 60.0 < 5
            THEN 'RAPIDO'
        WHEN DATEDIFF(SECOND, v.actual_arrival_utc, v.actual_departure_utc) / 60.0 BETWEEN 5  AND 15
            THEN 'NORMAL'
        WHEN DATEDIFF(SECOND, v.actual_arrival_utc, v.actual_departure_utc) / 60.0 BETWEEN 15 AND 30
            THEN 'DEMORADO'
        ELSE
            'CUELLO_DE_BOTELLA'
    END                                                     AS dwell_categoria,
    CAST(
        CASE
            WHEN v.actual_departure_utc IS NOT NULL
             AND DATEDIFF(SECOND, v.actual_arrival_utc, v.actual_departure_utc) / 60.0 > 30
            THEN 1 ELSE 0
        END
    AS BIT)                                                 AS es_cuello_de_botella,

    -- Franja horaria basada en hora local (America/Puerto_Rico)
    CASE
        WHEN DATEPART(HOUR, v.actual_arrival_local) BETWEEN 5  AND 8  THEN 'MANANA_TEMPRANA'
        WHEN DATEPART(HOUR, v.actual_arrival_local) BETWEEN 9  AND 11 THEN 'MANANA'
        WHEN DATEPART(HOUR, v.actual_arrival_local) BETWEEN 12 AND 14 THEN 'MEDIODIA'
        WHEN DATEPART(HOUR, v.actual_arrival_local) BETWEEN 15 AND 18 THEN 'TARDE'
        WHEN DATEPART(HOUR, v.actual_arrival_local) BETWEEN 19 AND 22 THEN 'NOCHE'
        ELSE                                                                'MADRUGADA'
    END                                                     AS franja_horaria,

    -- Hora programada de salida según itinerario IDA (en hora local Puerto Rico)
    ib_dep.scheduled_datetime_local                         AS scheduled_departure,

    -- Varianza de salida (IDA):
    --   positivo = tarde, negativo = adelantado, NULL = sin match en itinerario
    CAST(ib_dep.diff_minutos AS DECIMAL(10, 2))             AS delay_minutes,

    -- Estado de cumplimiento de salida (IDA):
    --   ON_TIME    : varianza dentro de ±5 min
    --   EARLY      : salió más de 5 min antes del horario
    --   LATE       : salió más de 5 min después del horario
    --   NO_SCHEDULE: no existe entrada IDA en el itinerario para este vehículo/parada/día
    CASE
        WHEN ib_dep.scheduled_hora IS NULL                THEN 'NO_SCHEDULE'
        WHEN ib_dep.diff_minutos BETWEEN -5 AND 5         THEN 'ON_TIME'
        WHEN ib_dep.diff_minutos < -5                     THEN 'EARLY'
        WHEN ib_dep.diff_minutos > 5                      THEN 'LATE'
        ELSE                                                   'ON_TIME'
    END                                                     AS departure_status,

    -- Hora programada de llegada según itinerario VUELTA (en hora local Puerto Rico)
    ib_arr.scheduled_datetime_local                         AS scheduled_arrival,

    -- Varianza de llegada (VUELTA):
    --   positivo = tarde, negativo = adelantado, NULL = sin match en itinerario
    CAST(ib_arr.diff_minutos AS DECIMAL(10, 2))             AS arrival_delay_minutes,

    -- Estado de cumplimiento de llegada (VUELTA):
    --   ON_TIME    : varianza dentro de ±2 min
    --   EARLY      : llegó más de 2 min antes del horario
    --   LATE       : llegó más de 2 min después del horario
    --   NO_SCHEDULE: no existe entrada VUELTA en el itinerario para este vehículo/parada/día
    CASE
        WHEN ib_arr.scheduled_hora IS NULL                THEN 'NO_SCHEDULE'
        WHEN ib_arr.diff_minutos BETWEEN -5 AND 5         THEN 'ON_TIME'
        WHEN ib_arr.diff_minutos < -5                     THEN 'EARLY'
        WHEN ib_arr.diff_minutos > 5                      THEN 'LATE'
        ELSE                                                   'ON_TIME'
    END                                                     AS arrival_status,

    -- Nota del proveedor sobre el evento
    v.note                                                  AS event_note

FROM cte_visits v
LEFT JOIN cte_device d
    ON d.device_id = v.device_id
LEFT JOIN cte_itinerario_best ib_dep
    ON  ib_dep.device_id          = v.device_id
    AND ib_dep.route_id           = v.route_id
    AND ib_dep.stop_sequence      = v.stop_sequence
    AND ib_dep.actual_arrival_utc = v.actual_arrival_utc
    AND ib_dep.direccion          = 'IDA'
LEFT JOIN cte_itinerario_best ib_arr
    ON  ib_arr.device_id          = v.device_id
    AND ib_arr.route_id           = v.route_id
    AND ib_arr.stop_sequence      = v.stop_sequence
    AND ib_arr.actual_arrival_utc = v.actual_arrival_utc
    AND ib_arr.direccion          = 'VUELTA'
