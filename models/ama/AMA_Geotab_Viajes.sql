{{ config(
    materialized='view',
    tag='dashboard_AMA'
) }}

WITH trip_base AS (
    SELECT
        t.trip_id,
        CAST(t.trip_start AS DATETIME)              AS [date],
        t.device_id,
        t.driver_id,
        t.trip_start                                AS actual_departure,
        t.trip_stop                                 AS actual_arrival,
        DATEDIFF(MINUTE, t.trip_start, t.trip_stop) AS trip_duration_minutes,
        t.distance_km,
        t.average_speed_kmh,
        t.stop_point_lat,
        t.stop_point_lon,
        d.device_name                               AS vehicle_name,
        d.device_type                               AS vehicle_type,
        -- --------------------------------------------------------
        -- ITINERARIO: pendiente de dataset — valores NULL hasta
        -- recibir el dataset con horarios por ruta y viaje de AMA.
        -- Reemplazar los tres campos siguientes con el JOIN
        -- correspondiente a la tabla de itinerario cuando esté disponible.
        -- --------------------------------------------------------
        CAST(NULL AS NVARCHAR(100))                 AS route_id,
        CAST(NULL AS NVARCHAR(100))                 AS route_name,
        CAST(NULL AS NVARCHAR(100))                 AS terminal_name,
        CAST(NULL AS DATETIME)                      AS scheduled_departure
    FROM {{ source('geotab', 'geotab_trip') }} t
    LEFT JOIN {{ source('geotab', 'geotab_device') }} d
        ON d.device_id = t.device_id
),

delay_calc AS (
    SELECT
        tb.*,
        -- Diferencia en minutos: positivo = tarde, negativo = temprano
        -- NULL cuando no hay itinerario disponible
        CASE
            WHEN tb.scheduled_departure IS NOT NULL
            THEN DATEDIFF(MINUTE, tb.scheduled_departure, tb.actual_departure)
            ELSE NULL
        END AS delay_minutes
    FROM trip_base tb
),

classified AS (
    SELECT
        dc.*,
        -- Clasificación con margen ±2 minutos según definición de AMA
        CASE
            WHEN dc.scheduled_departure IS NULL     THEN 'SIN_ITINERARIO'
            WHEN dc.delay_minutes BETWEEN -2 AND 2  THEN 'ON_SCHEDULE'
            WHEN dc.delay_minutes > 2               THEN 'DELAYED'
            WHEN dc.delay_minutes < -2              THEN 'EARLY'
        END AS departure_status,
        -- --------------------------------------------------------
        -- CENTRAL: TODO — reemplazar 'CENTRAL_DUMMY' con el
        -- zone_name o zone_id real una vez que AMA confirme cuál
        -- es la estación Central. Mientras tanto devuelve 0 (false).
        -- --------------------------------------------------------
        CASE
            WHEN dc.terminal_name = 'CENTRAL_DUMMY' THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
        END AS is_central_departure
    FROM delay_calc dc
)

SELECT
    trip_id,
    [date],
    route_id,
    route_name,
    terminal_name,
    device_id                               AS vehicle_id,
    vehicle_name,
    vehicle_type,
    driver_id,
    actual_departure,
    actual_arrival,
    trip_duration_minutes,
    scheduled_departure,
    delay_minutes,
    departure_status,
    distance_km,
    average_speed_kmh,
    is_central_departure,
    stop_point_lat,
    stop_point_lon
FROM classified
