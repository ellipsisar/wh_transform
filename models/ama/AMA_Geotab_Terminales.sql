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

WITH cte_deduped AS (
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
        CASE WHEN DATEPART(weekday,actual_arrival_local) BETWEEN 1 AND 5 THEN 'LUNES-VIERNES - LV' ELSE 'SABADO - SA' END AS service_day_type,
        CAST(actual_arrival_local   AS TIME) AS arrival_time,
        CAST(actual_departure_local AS TIME) AS departure_time,
        status,
        note,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, route_id, stop_sequence, actual_arrival_utc
            ORDER BY _md_filename DESC
        ) AS rn
    FROM {{ source('geotab', 'geotab_planned_vs_actual') }}
    {% if is_incremental() %}
      WHERE CAST(actual_arrival_local AS DATE) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

pln AS (
    SELECT
        CAST(tren AS VARCHAR(50)) AS train_id,
        orden_parada              AS stop_order,
        CASE
            WHEN turno BETWEEN 201 AND 208
                THEN CAST(hora AS TIME)
            WHEN turno BETWEEN 209 AND 216
                THEN CAST(DATEADD(HOUR, 12, CAST(hora AS TIME)) AS TIME)
            ELSE CAST(hora AS TIME)
        END                       AS planned_time,
        direccion                 AS direction,
        servicio                  AS service_day_type,
        turno                     AS shift,
        parada                    AS stop_name
    FROM dbo.AMA_Itinerario
    WHERE _md_snapshot_date = (
        SELECT MAX(_md_snapshot_date)
        FROM dbo.AMA_Itinerario)
    AND ruta_nombre = 'METROBUS II'
),

act AS (
    SELECT
        route_name,
        device_name,
        stop_sequence,
        arrival_zone_name,
        actual_arrival_local,
        actual_departure_local,
        arrival_time,
        departure_time,
        service_day_type,
        device_id
    FROM cte_deduped
    WHERE rn = 1
),

cte_joined AS (
    SELECT
        act.route_name,
        act.device_name,
        act.stop_sequence,
        act.arrival_zone_name,
        act.actual_arrival_local,
        act.actual_departure_local,
        act.arrival_time,
        act.departure_time,
        act.service_day_type,
        pln.direction,
        pln.planned_time,
        pln.shift,
        pln.stop_name,
        d.device_type
    FROM act
    LEFT JOIN pln
           ON  pln.stop_order       = act.stop_sequence
           AND pln.train_id         = act.device_name
           AND pln.service_day_type = act.service_day_type
    LEFT JOIN staging.geotab_device d
           ON d.device_id = act.device_id
),

final AS (
    SELECT
        *,
        CASE
            WHEN direction = 'IDA'
            THEN DATEDIFF(MINUTE, planned_time, departure_time)
            ELSE DATEDIFF(MINUTE, planned_time, arrival_time)
        END AS delay_minutes
    FROM cte_joined
)

SELECT
    route_name,
    CAST(actual_arrival_local AS DATE)                    AS event_date,
    actual_arrival_local,
    actual_departure_local,
    DATEPART(HOUR,    actual_arrival_local)               AS arrival_hour,
    DATEPART(WEEKDAY, actual_arrival_local)               AS arrival_weekday,
    arrival_zone_name                                     AS terminal_name,

    device_name                                           AS vehicle_name,
    device_type                                           AS vehicle_type,

    CAST(
        CASE
            WHEN arrival_zone_name = 'Estacion Martinez Nadal (Abordaje/Descenso), San Juan, 00921, Puerto Rico'
            THEN 1 ELSE 0
        END
    AS BIT)                                               AS is_central_terminal,

    stop_sequence,
    stop_name,
    shift,

    CASE
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN  0 AND  4 THEN 'OVERNIGHT'
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN  5 AND  8 THEN 'EARLY_MORNING'
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN  9 AND 11 THEN 'MORNING'
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN 12 AND 14 THEN 'MIDDAY'
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN 15 AND 18 THEN 'AFTERNOON'
        WHEN DATEPART(HOUR, actual_departure_local) BETWEEN 19 AND 23 THEN 'EVENING'
    END                                                   AS time_slot,

    arrival_time,
    departure_time,
    planned_time,
    ABS(delay_minutes)                                    AS delay_minutes,

    CASE
        WHEN delay_minutes BETWEEN -5 AND 5 THEN 'ON_TIME'
        WHEN delay_minutes < -5             THEN 'EARLY'
        WHEN delay_minutes > 5              THEN 'LATE'
        ELSE                                     'NO_SCHEDULE'
    END                                                   AS status

FROM final
ORDER BY actual_arrival_local DESC, device_name, stop_sequence, shift
