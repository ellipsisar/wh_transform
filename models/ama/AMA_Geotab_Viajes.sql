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

WITH itinerario_paradas AS (
    -- Todas las paradas del snapshot más reciente del itinerario.
    -- Se usa en dos roles:
    --   1. Matching viaje → servicio: filtrando orden_parada = 0 (terminal de origen).
    --   2. Lookup de hora programada por zona: todas las paradas.
    SELECT
        i.tren,
        i.ruta              AS route_id,
        i.ruta_nombre       AS route_name,
        i.servicio,
        i.direccion,
        i.turno,
        i.orden_parada,
        i.parada            AS zone_name_itinerario,
        i.hora              AS scheduled_hora
    FROM {{ source('geotab', 'ama_itinerario') }} i
    WHERE i._md_snapshot_date = (
        SELECT MAX(_md_snapshot_date)
        FROM {{ source('geotab', 'ama_itinerario') }}
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

cte_regla_zona AS (
    -- Resuelve rule_id → zone_id + zone_name.
    -- ROW_NUMBER deduplica si una regla apunta a múltiples zonas con el mismo nombre.
    SELECT
        ru.rule_id,
        z.zone_id,
        z.zone_name,
        ROW_NUMBER() OVER (PARTITION BY ru.rule_id ORDER BY z.zone_id) AS rn
    FROM {{ source('geotab', 'geotab_rule') }} ru
    JOIN {{ source('geotab', 'geotab_zone') }} z
        ON z.zone_name = ru.name
),

trip_servicio_candidatos AS (
    -- Todos los servicios programados que coinciden en tren + día de semana,
    -- usando solo la parada de origen (orden_parada = 0) para el matching.
    -- diff_minutos: distancia temporal entre salida programada y salida real.
    SELECT
        tb.trip_id,
        ipp.route_id,
        ipp.route_name,
        ipp.servicio,
        ipp.direccion,
        ipp.turno,
        ipp.zone_name_itinerario                    AS terminal_salida,
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
    JOIN itinerario_paradas ipp
        ON  ipp.tren         = tb.vehicle_name
        AND ipp.servicio     = tb.servicio_dia
        AND ipp.orden_parada = 0
),

trip_servicio AS (
    -- Servicio programado más cercano temporalmente al viaje real.
    -- Determina (route_id, direccion, turno, scheduled_departure) para todo el viaje.
    SELECT
        trip_id,
        route_id,
        route_name,
        direccion,
        turno,
        terminal_salida,
        scheduled_departure
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY trip_id
                ORDER BY diff_minutos ASC
            ) AS rn
        FROM trip_servicio_candidatos
    ) ranked
    WHERE rn = 1
),

trip_zone_events AS (
    -- Todos los eventos de zona ocurridos DURANTE cada viaje
    -- (zone_entry_time entre trip_start y trip_stop).
    -- Cada fila es una zona por la que pasó el vehículo en ese viaje.
    SELECT
        tb.trip_id,
        tb.device_id,
        tb.vehicle_name,
        tb.vehicle_type,
        tb.driver_id,
        tb.servicio_dia,
        tb.actual_departure,
        tb.actual_arrival,
        tb.trip_duration_minutes,
        tb.distance_km,
        tb.average_speed_kmh,
        tb.stop_point_lat,
        tb.stop_point_lon,
        rz.zone_id,
        rz.zone_name,
        ee.active_from                                  AS zone_entry_time,
        ee.active_to                                    AS zone_exit_time,
        DATEDIFF(MINUTE, ee.active_from, ee.active_to)  AS dwell_minutes
    FROM trip_base tb
    JOIN {{ source('geotab', 'geotab_exception_event') }} ee
        ON  ee.device_id   = tb.device_id
        AND ee.active_from >= tb.actual_departure
        AND ee.active_from <= tb.actual_arrival
    JOIN cte_regla_zona rz
        ON  rz.rule_id = ee.rule_id
        AND rz.rn      = 1
),

trip_zone_scheduled AS (
    -- Enriquece cada evento de zona con:
    --   - metadata del viaje (route, turno, scheduled_departure)
    --   - hora programada para esa zona específica (del itinerario)
    SELECT
        tze.*,
        ts.route_id,
        ts.route_name,
        ts.direccion,
        ts.turno,
        ISNULL(ts.terminal_salida, tze.zone_name)   AS terminal_name,
        ts.scheduled_departure,
        -- Hora programada de llegada a esta zona.
        -- NULL si la zona no figura en el itinerario del servicio asignado.
        CASE
            WHEN ipa.scheduled_hora IS NOT NULL
            THEN DATEADD(MINUTE,
                    CAST(SUBSTRING(ipa.scheduled_hora,
                                   CHARINDEX(':', ipa.scheduled_hora) + 1, 2) AS INT),
                    DATEADD(HOUR,
                        CAST(LEFT(ipa.scheduled_hora,
                                  CHARINDEX(':', ipa.scheduled_hora) - 1) AS INT),
                        CAST(CAST(tze.zone_entry_time AS DATE) AS DATETIME)
                    )
                )
            ELSE NULL
        END                                         AS scheduled_arrival,
        ipa.orden_parada
    FROM trip_zone_events tze
    LEFT JOIN trip_servicio ts
        ON ts.trip_id = tze.trip_id
    LEFT JOIN itinerario_paradas ipa
        ON  ipa.tren                 = tze.vehicle_name
        AND ipa.servicio             = tze.servicio_dia
        AND ipa.turno                = ts.turno
        AND ipa.zone_name_itinerario = tze.zone_name
),

classified AS (
    SELECT
        tzs.*,
        -- delay_minutes: positivo = tarde, negativo = adelantado
        CASE
            WHEN tzs.scheduled_arrival IS NOT NULL
            THEN DATEDIFF(MINUTE, tzs.scheduled_arrival, tzs.zone_entry_time)
            ELSE NULL
        END                                         AS delay_minutes,
        CASE
            WHEN tzs.scheduled_arrival IS NULL          THEN 'SIN_ITINERARIO'
            WHEN DATEDIFF(MINUTE, tzs.scheduled_arrival,
                          tzs.zone_entry_time) BETWEEN -2 AND 2 THEN 'ON_SCHEDULE'
            WHEN DATEDIFF(MINUTE, tzs.scheduled_arrival,
                          tzs.zone_entry_time) > 2              THEN 'DELAYED'
            ELSE                                                      'EARLY'
        END                                         AS arrival_status,
        -- is_central_zone: identifica la Estacion Martinez Nadal
        -- (Abordaje/Descenso), San Juan, 00921, Puerto Rico — zone_id 'b12'.
        CASE
            WHEN tzs.zone_id = 'b12'                THEN CAST(1 AS BIT)
            ELSE                                         CAST(0 AS BIT)
        END                                         AS is_central_zone
    FROM trip_zone_scheduled tzs
)

SELECT
    trip_id,
    CAST(CAST(actual_departure AS DATE) AS DATETIME) AS [date],
    zone_id,
    zone_name,
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
    zone_entry_time,
    zone_exit_time,
    dwell_minutes,
    orden_parada,
    scheduled_departure,
    scheduled_arrival,
    delay_minutes,
    arrival_status,
    is_central_zone,
    distance_km,
    average_speed_kmh,
    stop_point_lat,
    stop_point_lon
FROM classified
