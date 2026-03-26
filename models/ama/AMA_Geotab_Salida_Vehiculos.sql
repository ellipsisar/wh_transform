{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='ROUND_ROBIN',
    index='CLUSTERED COLUMNSTORE INDEX',
    tag='dashboard_AMA',
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

-- Nota: dist='ROUND_ROBIN' mientras route_id sea NULL (sin dataset de itinerario).
-- Cambiar a dist='HASH(route_id)' cuando el dataset de AMA esté disponible.

WITH base AS (
    SELECT
        [date],
        route_id,
        route_name,
        trip_id,
        vehicle_id,
        departure_status,
        delay_minutes,
        trip_duration_minutes,
        distance_km,
        is_central_departure
    FROM {{ ref('AMA_Geotab_Viajes') }}
    {% if is_incremental() %}
    WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
)

SELECT
    ISNULL([date], CAST('1900-01-01' AS DATETIME))        AS [date],          -- datetime not null
    CAST(route_id   AS NVARCHAR(100))                     AS route_id,        -- nvarchar (NULL hasta itinerario)
    CAST(route_name AS NVARCHAR(100))                     AS route_name,      -- nvarchar (NULL hasta itinerario)
    ISNULL(SUM(CASE WHEN departure_status = 'ON_SCHEDULE'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_on_schedule_departure,
    ISNULL(SUM(CASE WHEN departure_status = 'DELAYED'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_delayed_departure,
    COUNT(trip_id)                                        AS total_departures,
    ISNULL(SUM(CASE WHEN departure_status = 'EARLY'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_early_departure,
    ISNULL(SUM(CASE WHEN departure_status = 'SIN_ITINERARIO'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_sin_itinerario,
    CAST(
        CASE
            WHEN SUM(CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END) = 0
            THEN NULL
            ELSE 100.0
                 * SUM(CASE WHEN departure_status = 'ON_SCHEDULE' THEN 1 ELSE 0 END)
                 / SUM(CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END)
        END
    AS DECIMAL(5, 2))                                     AS pct_on_schedule,
    CAST(
        CASE
            WHEN SUM(CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END) = 0
            THEN NULL
            ELSE 100.0
                 * SUM(CASE WHEN departure_status = 'DELAYED' THEN 1 ELSE 0 END)
                 / SUM(CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END)
        END
    AS DECIMAL(5, 2))                                     AS pct_delayed,
    CAST(
        AVG(
            CASE WHEN departure_status = 'DELAYED'
                 THEN CAST(delay_minutes AS FLOAT)
                 ELSE NULL
            END
        )
    AS DECIMAL(8, 2))                                     AS avg_delay_minutes,
    MAX(
        CASE WHEN departure_status = 'DELAYED'
             THEN delay_minutes
             ELSE NULL
        END
    )                                                     AS max_delay_minutes,
    COUNT(DISTINCT vehicle_id)                            AS cantidad_vehicles_operated,
    CAST(AVG(CAST(trip_duration_minutes AS FLOAT))
    AS DECIMAL(8, 2))                                     AS avg_trip_duration_minutes,
    CAST(AVG(CAST(distance_km AS FLOAT))
    AS DECIMAL(8, 2))                                     AS avg_distance_km,
    ISNULL(SUM(CASE WHEN is_central_departure = 1
                    THEN 1 ELSE 0 END), 0)                AS cantidad_central_departures

FROM base
GROUP BY [date], route_id, route_name
