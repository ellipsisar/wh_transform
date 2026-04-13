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
    CAST(route_id   AS NVARCHAR(100))                     AS route_id,
    CAST(route_name AS NVARCHAR(100))                     AS route_name,
    COUNT(DISTINCT CASE WHEN departure_status = 'ON_SCHEDULE'
                        THEN vehicle_id END)              AS cantidad_on_schedule_departure,
    COUNT(DISTINCT CASE WHEN departure_status = 'DELAYED'
                        THEN vehicle_id END)              AS cantidad_delayed_departure,
    COUNT(trip_id)                                        AS total_departures,
    COUNT(DISTINCT CASE WHEN departure_status = 'EARLY'
                        THEN vehicle_id END)              AS cantidad_early_departure,
    COUNT(DISTINCT CASE WHEN departure_status = 'SIN_ITINERARIO'
                        THEN vehicle_id END)              AS cantidad_sin_itinerario,
    CAST(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN vehicle_id END) = 0
            THEN NULL
            ELSE 100.0
                 * COUNT(DISTINCT CASE WHEN departure_status = 'ON_SCHEDULE' THEN vehicle_id END)
                 / COUNT(DISTINCT CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN vehicle_id END)
        END
    AS DECIMAL(5, 2))                                     AS pct_on_schedule,
    CAST(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN vehicle_id END) = 0
            THEN NULL
            ELSE 100.0
                 * COUNT(DISTINCT CASE WHEN departure_status = 'DELAYED' THEN vehicle_id END)
                 / COUNT(DISTINCT CASE WHEN departure_status <> 'SIN_ITINERARIO' THEN vehicle_id END)
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
    COUNT(DISTINCT CASE WHEN is_central_departure = 1
                        THEN vehicle_id END)              AS cantidad_central_departures

FROM base
GROUP BY [date], route_id, route_name
