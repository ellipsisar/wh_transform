{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(vehicle_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['dashboard_AMA'],
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

WITH itinerario_salida AS (
    -- Primera parada (orden_parada = 0) del snapshot más reciente.
    -- Cada fila representa la salida programada de un servicio:
    --   (tren, servicio, direccion, turno) → terminal de origen + hora programada.
    SELECT
        i.tren,
        i.ruta              AS route_id,
        i.ruta_nombre       AS route_name,
        i.servicio,
        i.direccion,
        i.turno,
        i.parada            AS terminal_salida,
        i.hora              AS scheduled_hora
    FROM {{ source('ama', 'AMA_Itinerario') }} i
    WHERE i.orden_parada = 0
      AND i._md_snapshot_date = (
          SELECT MAX(_md_snapshot_date)
          FROM {{ source('ama', 'AMA_Itinerario') }}
      )
),

trip_base AS (
    SELECT
        t.trip_id,
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
        -- Tipo de servicio según día de semana (DATEFIRST=7 default Synapse)
        -- 1=Domingo, 2=Lunes … 6=Viernes, 7=Sábado
        CASE
            WHEN DATEPART(dw, t.trip_start) = 1             THEN 'DOMINGO - DO'
            WHEN DATEPART(dw, t.trip_start) = 7             THEN 'SABADO - SA'
            ELSE                                                  'LUNES-VIERNES - LV'
        END                                         AS servicio_dia
    FROM {{ source('geotab', 'geotab_trip') }} t
    LEFT JOIN {{ source('geotab', 'geotab_device') }} d
        ON d.device_id = t.device_id
    {% if is_incremental() %}
    WHERE t.trip_start >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

trip_itinerario_candidatos AS (
    -- Todos los servicios programados que coinciden en tren + día de semana.
    -- Construye la hora programada de salida = fecha del viaje real + hora del itinerario.
    -- diff_minutos: distancia temporal entre la salida programada y la real.
    SELECT
        tb.trip_id,
        ipp.route_id,
        ipp.route_name,
        ipp.terminal_salida,
        ipp.direccion,
        DATEADD(MINUTE,
            CAST(SUBSTRING(ipp.scheduled_hora,
                           CHARINDEX(':', ipp.scheduled_hora) + 1, 2) AS INT),
            DATEADD(HOUR,
                CAST(LEFT(ipp.scheduled_hora,
                          CHARINDEX(':', ipp.scheduled_hora) - 1) AS INT),
                CAST(CAST(tb.actual_departure AS DATE) AS DATETIME)
            )
        )                                           AS scheduled_departure,
        ABS(DATEDIFF(MINUTE,
            DATEADD(MINUTE,
                CAST(SUBSTRING(ipp.scheduled_hora,
                               CHARINDEX(':', ipp.scheduled_hora) + 1, 2) AS INT),
                DATEADD(HOUR,
                    CAST(LEFT(ipp.scheduled_hora,
                              CHARINDEX(':', ipp.scheduled_hora) - 1) AS INT),
                    CAST(CAST(tb.actual_departure AS DATE) AS DATETIME)
                )
            ),
            tb.actual_departure
        ))                                          AS diff_minutos
    FROM trip_base tb
    JOIN itinerario_salida ipp
        ON  ipp.tren     = tb.vehicle_name
        AND ipp.servicio = tb.servicio_dia
),

trip_itinerario AS (
    -- Servicio programado más cercano temporalmente al viaje real.
    -- Un mismo tren puede tener varios turnos al día; se elige el más próximo.
    SELECT
        trip_id,
        route_id,
        route_name,
        terminal_salida,
        direccion,
        scheduled_departure
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY trip_id
                ORDER BY diff_minutos ASC
            ) AS rn
        FROM trip_itinerario_candidatos
    ) ranked
    WHERE rn = 1
),

cte_regla_zona AS (
    -- Resuelve rule_id → zone_id + route vía coincidencia de nombres.
    -- ROW_NUMBER deduplica si una zona pertenece a múltiples rutas.
    SELECT
        ru.rule_id,
        z.zone_id,
        z.zone_name,
        r.route_id,
        r.name                                      AS route_name,
        ROW_NUMBER() OVER (PARTITION BY ru.rule_id ORDER BY r.route_id) AS rn
    FROM {{ source('geotab', 'geotab_rule') }} ru
    JOIN {{ source('geotab', 'geotab_zone') }} z
        ON z.zone_name = ru.name
    LEFT JOIN {{ source('geotab', 'geotab_route_plan_item') }} rpi
        ON rpi.zone_id = z.zone_id
    LEFT JOIN {{ source('geotab', 'geotab_route') }} r
        ON r.route_id = rpi.route_id
),

cte_terminal_pre_viaje AS (
    -- Para cada viaje: zona (terminal) de geotab más reciente hasta 4 h antes
    -- de la salida. Se usa como fallback cuando el tren no está en el itinerario.
    SELECT
        tb.trip_id,
        rz.zone_name                                AS terminal_name_geotab,
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

trip_enriquecido AS (
    SELECT
        tb.*,
        -- Itinerario: fuente primaria para ruta, terminal y hora programada
        ti.route_id,
        ti.route_name,
        -- terminal_name: itinerario (parada) preferido sobre zona geotab
        ISNULL(ti.terminal_salida, tpt.terminal_name_geotab) AS terminal_name,
        ti.scheduled_departure,
        -- delay_minutes: positivo = tarde, negativo = adelantado
        CASE
            WHEN ti.scheduled_departure IS NOT NULL
            THEN DATEDIFF(MINUTE, ti.scheduled_departure, tb.actual_departure)
            ELSE NULL
        END                                         AS delay_minutes
    FROM trip_base tb
    LEFT JOIN trip_itinerario ti
        ON ti.trip_id = tb.trip_id
    LEFT JOIN cte_terminal_pre_viaje tpt
        ON  tpt.trip_id = tb.trip_id
        AND tpt.rn      = 1
),

classified AS (
    SELECT
        te.*,
        -- Clasificación con margen ±2 minutos (definición AMA)
        CASE
            WHEN te.scheduled_departure IS NULL    THEN 'SIN_ITINERARIO'
            WHEN te.delay_minutes BETWEEN -2 AND 2 THEN 'ON_SCHEDULE'
            WHEN te.delay_minutes > 2              THEN 'DELAYED'
            WHEN te.delay_minutes < -2             THEN 'EARLY'
        END                                        AS departure_status,
        -- is_central_departure: marca salidas desde la terminal Central de AMA.
        -- TODO: confirmar con AMA el código exacto de la terminal Central.
        --       Se usa 'SGDO' como referencia provisional basada en el itinerario.
        CASE
            WHEN te.terminal_name = 'SGDO'         THEN CAST(1 AS BIT)
            ELSE                                        CAST(0 AS BIT)
        END                                        AS is_central_departure
    FROM trip_enriquecido te
)

SELECT
    trip_id,
    -- Fecha del viaje truncada a medianoche para agregaciones diarias
    CAST(CAST(actual_departure AS DATE) AS DATETIME) AS [date],
    route_id,
    route_name,
    terminal_name,
    device_id                                        AS vehicle_id,
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
