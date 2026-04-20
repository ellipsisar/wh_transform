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
  - status = 'ON_TIME' → el vehículo visitó físicamente la zona (tiene actual_arrival/departure).
  - La tabla se actualiza en múltiples snapshots diarios; se deduplica por
    (device_id, route_id, stop_sequence, actual_arrival_utc) usando _md_filename DESC.
  - Solo se incluyen filas ON_TIME (visitas reales al terminal).
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
        status,
        CAST(ISNULL(variance_minutes, 0) AS DECIMAL(10, 2)) AS variance_minutes,
        note,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, route_id, stop_sequence, actual_arrival_utc
            ORDER BY _md_filename DESC
        ) AS rn
    FROM {{ source('geotab', 'geotab_planned_vs_actual') }}
    WHERE status = 'ON_TIME'
    {% if is_incremental() %}
      AND CAST(actual_arrival_utc AS DATE) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
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

-- Siguiente viaje del vehículo dentro de las 4 horas posteriores a la salida del terminal
cte_next_trip AS (
    SELECT
        v.device_id,
        v.route_id,
        v.stop_sequence,
        v.actual_arrival_utc,
        MIN(t.trip_start) AS next_trip_start,
        MIN(t.trip_id)    AS next_trip_id
    FROM cte_visits v
    JOIN {{ source('geotab', 'geotab_trip') }} t
        ON  t.device_id  = v.device_id
        AND t.trip_start >= v.actual_departure_utc
        AND t.trip_start <  DATEADD(HOUR, 4, v.actual_departure_utc)
    WHERE v.actual_departure_utc IS NOT NULL
    GROUP BY v.device_id, v.route_id, v.stop_sequence, v.actual_arrival_utc
)

SELECT
    -- Evento
    CAST(v.actual_arrival_utc AS DATE)                      AS event_date,
    DATEPART(HOUR,    v.actual_arrival_utc)                 AS entry_hour,
    DATEPART(WEEKDAY, v.actual_arrival_utc)                 AS entry_weekday,

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

    -- Franja horaria
    CASE
        WHEN DATEPART(HOUR, v.actual_arrival_utc) BETWEEN 5  AND 8  THEN 'MANANA_TEMPRANA'
        WHEN DATEPART(HOUR, v.actual_arrival_utc) BETWEEN 9  AND 11 THEN 'MANANA'
        WHEN DATEPART(HOUR, v.actual_arrival_utc) BETWEEN 12 AND 14 THEN 'MEDIODIA'
        WHEN DATEPART(HOUR, v.actual_arrival_utc) BETWEEN 15 AND 18 THEN 'TARDE'
        WHEN DATEPART(HOUR, v.actual_arrival_utc) BETWEEN 19 AND 22 THEN 'NOCHE'
        ELSE                                                              'MADRUGADA'
    END                                                     AS franja_horaria,

    -- Viaje posterior al evento en terminal
    nt.next_trip_id,
    nt.next_trip_start,
    DATEDIFF(MINUTE, v.actual_departure_utc, nt.next_trip_start) AS minutos_hasta_viaje,

    -- Estado e itinerario (ahora poblados desde planned_vs_actual)
    v.status                                                AS departure_status,
    v.variance_minutes                                      AS delay_minutes,
    -- Los tiempos planificados en esta fuente son valores centinela (1986/2050); se mantiene NULL
    CAST(NULL AS DATETIME)                                  AS scheduled_departure,

    -- Nota del proveedor sobre el evento
    v.note                                                  AS event_note

FROM cte_visits v
LEFT JOIN cte_device d
    ON d.device_id = v.device_id
LEFT JOIN cte_next_trip nt
    ON  nt.device_id          = v.device_id
    AND nt.route_id           = v.route_id
    AND nt.stop_sequence      = v.stop_sequence
    AND nt.actual_arrival_utc = v.actual_arrival_utc
