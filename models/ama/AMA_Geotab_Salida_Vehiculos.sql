{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(route_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['dashboard_AMA'],
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

/*
  Fuente principal: staging.geotab_planned_vs_actual
  - Grain: stop_sequence = 0 (primera parada = salida del vehículo desde terminal).
  - La tabla se actualiza en múltiples snapshots diarios; se deduplica por
    (device_id, route_id, stop_sequence, actual_arrival_utc) usando _md_filename DESC.
  - departure_status se deriva de variance_minutes (regla ±2 min).
  - status <> 'ON_TIME' → 'MISSED' (parada planificada no visitada).
*/

WITH cte_deduped AS (
    -- Una fila por salida real: deduplica snapshots y filtra la parada de inicio de recorrido
    SELECT
        route_id,
        route_name,
        device_id,
        device_name,
        arrival_zone_name,
        CAST(actual_departure_utc AS DATETIME)                   AS actual_departure_utc,
        status,
        CAST(variance_minutes AS DECIMAL(10, 2))                 AS variance_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, route_id, stop_sequence, actual_arrival_utc
            ORDER BY _md_filename DESC
        ) AS rn
    FROM {{ source('geotab', 'geotab_planned_vs_actual') }}
    WHERE stop_sequence = 0
    {% if is_incremental() %}
      AND CAST(actual_arrival_utc AS DATE) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

cte_departures AS (
    SELECT
        -- Fecha de salida truncada a medianoche para agregaciones diarias
        CAST(CAST(actual_departure_utc AS DATE) AS DATETIME)     AS [date],
        route_id,
        route_name,
        device_id,
        device_name,
        variance_minutes,
        -- Terminal central: Estacion Martinez Nadal
        CAST(
            CASE
                WHEN arrival_zone_name = 'Estacion Martinez Nadal (Abordaje/Descenso), San Juan, 00921, Puerto Rico'
                THEN 1 ELSE 0
            END
        AS BIT)                                                  AS is_central_departure,
        -- Clasificación con margen ±2 min; MISSED = parada planificada no visitada
        CASE
            WHEN status <> 'ON_TIME'                   THEN 'MISSED'
            WHEN variance_minutes BETWEEN -2 AND 2     THEN 'ON_SCHEDULE'
            WHEN variance_minutes > 2                  THEN 'DELAYED'
            WHEN variance_minutes < -2                 THEN 'EARLY'
        END                                                      AS departure_status
    FROM cte_deduped
    WHERE rn = 1
)

SELECT
    ISNULL([date], CAST('1900-01-01' AS DATETIME))               AS [date],
    CAST(route_id   AS NVARCHAR(100))                            AS route_id,
    CAST(route_name AS NVARCHAR(100))                            AS route_name,
    COUNT(CASE WHEN departure_status = 'ON_SCHEDULE' THEN 1 END) AS cantidad_on_schedule_departure,
    COUNT(CASE WHEN departure_status = 'DELAYED'     THEN 1 END) AS cantidad_delayed_departure,
    COUNT(*)                                                     AS total_departures,
    COUNT(CASE WHEN departure_status = 'EARLY'       THEN 1 END) AS cantidad_early_departure,
    -- MISSED: parada planificada que el vehículo no visitó (equivalente funcional al anterior SIN_ITINERARIO)
    COUNT(CASE WHEN departure_status = 'MISSED'      THEN 1 END) AS cantidad_sin_itinerario,
    -- pct sobre viajes completados (excluye MISSED del denominador)
    CAST(
        CASE
            WHEN COUNT(CASE WHEN departure_status <> 'MISSED' THEN 1 END) = 0
            THEN NULL
            ELSE 100.0
                 * COUNT(CASE WHEN departure_status = 'ON_SCHEDULE' THEN 1 END)
                 / COUNT(CASE WHEN departure_status <> 'MISSED'     THEN 1 END)
        END
    AS DECIMAL(5, 2))                                            AS pct_on_schedule,
    CAST(
        CASE
            WHEN COUNT(CASE WHEN departure_status <> 'MISSED' THEN 1 END) = 0
            THEN NULL
            ELSE 100.0
                 * COUNT(CASE WHEN departure_status = 'DELAYED' THEN 1 END)
                 / COUNT(CASE WHEN departure_status <> 'MISSED' THEN 1 END)
        END
    AS DECIMAL(5, 2))                                            AS pct_delayed,
    CAST(
        AVG(
            CASE WHEN departure_status = 'DELAYED'
                 THEN CAST(variance_minutes AS FLOAT)
                 ELSE NULL
            END
        )
    AS DECIMAL(8, 2))                                            AS avg_delay_minutes,
    MAX(
        CASE WHEN departure_status = 'DELAYED'
             THEN variance_minutes
             ELSE NULL
        END
    )                                                            AS max_delay_minutes,
    COUNT(DISTINCT device_id)                                    AS cantidad_vehicles_operated,
    -- Eliminadas: no disponibles en geotab_planned_vs_actual sin join adicional
    CAST(NULL AS DECIMAL(8, 2))                                  AS avg_trip_duration_minutes,
    CAST(NULL AS DECIMAL(8, 2))                                  AS avg_distance_km,
    COUNT(CASE WHEN is_central_departure = CAST(1 AS BIT)
               THEN 1 END)                                       AS cantidad_central_departures

FROM cte_departures
GROUP BY [date], route_id, route_name
