{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(zone_id)',
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
        zone_id,
        zone_name,
        trip_id,
        vehicle_id,
        arrival_status,
        delay_minutes,
        dwell_minutes,
        is_central_zone
    FROM {{ ref('AMA_Geotab_Viajes') }}
    {% if is_incremental() %}
    WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
)

SELECT
    ISNULL([date], CAST('1900-01-01' AS DATETIME))        AS [date],          -- datetime not null
    CAST(zone_id   AS NVARCHAR(100))                      AS zone_id,
    CAST(zone_name AS NVARCHAR(500))                      AS zone_name,
    ISNULL(SUM(CASE WHEN arrival_status = 'ON_SCHEDULE'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_on_schedule,
    ISNULL(SUM(CASE WHEN arrival_status = 'DELAYED'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_delayed,
    COUNT(trip_id)                                        AS total_zone_passages,
    ISNULL(SUM(CASE WHEN arrival_status = 'EARLY'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_early,
    ISNULL(SUM(CASE WHEN arrival_status = 'SIN_ITINERARIO'
                    THEN 1 ELSE 0 END), 0)                AS cantidad_sin_itinerario,
    CAST(
        CASE
            WHEN SUM(CASE WHEN arrival_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END) = 0
            THEN NULL
            ELSE 100.0
                 * SUM(CASE WHEN arrival_status = 'ON_SCHEDULE' THEN 1 ELSE 0 END)
                 / SUM(CASE WHEN arrival_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END)
        END
    AS DECIMAL(5, 2))                                     AS pct_on_schedule,
    CAST(
        CASE
            WHEN SUM(CASE WHEN arrival_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END) = 0
            THEN NULL
            ELSE 100.0
                 * SUM(CASE WHEN arrival_status = 'DELAYED' THEN 1 ELSE 0 END)
                 / SUM(CASE WHEN arrival_status <> 'SIN_ITINERARIO' THEN 1 ELSE 0 END)
        END
    AS DECIMAL(5, 2))                                     AS pct_delayed,
    CAST(
        AVG(
            CASE WHEN arrival_status = 'DELAYED'
                 THEN CAST(delay_minutes AS FLOAT)
                 ELSE NULL
            END
        )
    AS DECIMAL(8, 2))                                     AS avg_delay_minutes,
    MAX(
        CASE WHEN arrival_status = 'DELAYED'
             THEN delay_minutes
             ELSE NULL
        END
    )                                                     AS max_delay_minutes,
    CAST(AVG(CAST(dwell_minutes AS FLOAT))
    AS DECIMAL(8, 2))                                     AS avg_dwell_minutes,
    COUNT(DISTINCT vehicle_id)                            AS cantidad_vehicles_operated,
    ISNULL(SUM(CASE WHEN is_central_zone = 1
                    THEN 1 ELSE 0 END), 0)                AS cantidad_central_zone_passages

FROM base
GROUP BY [date], zone_id, zone_name
