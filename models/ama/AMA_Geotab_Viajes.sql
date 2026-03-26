{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(vehicle_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tag='dashboard_AMA',
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
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
        d.device_type                               AS vehicle_type
    FROM {{ source('geotab', 'geotab_trip') }} t
    LEFT JOIN {{ source('geotab', 'geotab_device') }} d
        ON d.device_id = t.device_id
    {% if is_incremental() %}
    WHERE t.trip_start >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

cte_regla_zona AS (
    -- Resuelve rule_id → zone_id + route vía coincidencia de nombres.
    -- ROW_NUMBER deduplica en caso de que una zona pertenezca a múltiples rutas.
    SELECT
        ru.rule_id,
        z.zone_id,
        z.zone_name,
        r.route_id,
        r.route_name,
        ROW_NUMBER() OVER (PARTITION BY ru.rule_id ORDER BY r.route_id) AS rn
    FROM {{ source('geotab', 'geotab_rule') }} ru
    JOIN {{ source('geotab', 'geotab_zone') }} z
        ON z.zone_name = ru.rule_name
    LEFT JOIN {{ source('geotab', 'geotab_route_plan_item') }} rpi
        ON rpi.zone_id = z.zone_id
    LEFT JOIN {{ source('geotab', 'geotab_route') }} r
        ON r.route_id = rpi.route_id
),

cte_terminal_pre_viaje AS (
    -- Para cada viaje: busca el evento de zona (terminal) más reciente
    -- que haya terminado hasta 4 horas antes de la salida del vehículo.
    SELECT
        tb.trip_id,
        rz.zone_id,
        rz.zone_name                                AS terminal_name,
        rz.route_id,
        rz.route_name,
        ROW_NUMBER() OVER (PARTITION BY tb.trip_id ORDER BY ee.active_to DESC) AS rn
    FROM trip_base tb
    JOIN {{ source('geotab', 'geotab_exception_event') }} ee
        ON  ee.device_id  = tb.device_id
        AND ee.active_to <= tb.actual_departure
        AND ee.active_to >= DATEADD(HOUR, -4, tb.actual_departure)
    JOIN cte_regla_zona rz
        ON  rz.rule_id = ee.rule_id
        AND rz.rn      = 1
),

trip_con_terminal AS (
    SELECT
        tb.*,
        tpt.terminal_name,
        tpt.route_id,
        tpt.route_name
    FROM trip_base tb
    LEFT JOIN cte_terminal_pre_viaje tpt
        ON  tpt.trip_id = tb.trip_id
        AND tpt.rn      = 1
),

delay_calc AS (
    SELECT
        tc.*,
        -- ITINERARIO: pendiente dataset AMA
        CAST(NULL AS DATETIME)                      AS scheduled_departure,
        -- Diferencia en minutos: positivo = tarde, negativo = temprano
        -- NULL hasta que haya itinerario disponible
        CAST(NULL AS INT)                           AS delay_minutes
    FROM trip_con_terminal tc
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
        -- zone_name real una vez que AMA confirme cuál es la
        -- estación Central (el terminal_name ya viene de geotab_zone).
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
