{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(zone_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tag=['dashboard_AMA','terminales_AMA'],
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE event_date >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

WITH pos AS (
    SELECT
        zone_id,
        zone_name,
        zone_types_json,
        external_reference,
        must_identify_stops,
        active_from,
        active_to,
        -- posición del token "id" dentro del JSON
        CHARINDEX('"id"', zone_types_json)          AS id_key_pos
    FROM {{ source('geotab', 'geotab_zone') }}
),

val_pos AS (
    SELECT
        *,
        -- posición del primer " que abre el valor del id (sólo relevante si id_key_pos > 0)
        CASE
            WHEN id_key_pos > 0
            THEN CHARINDEX('"', zone_types_json,
                     CHARINDEX(':', zone_types_json, id_key_pos + 4) + 1)
            ELSE 0
        END                                         AS val_start_pos
    FROM pos
),

stg_geotab__zone AS (
SELECT
    z.zone_id,
    z.zone_name,
    z.external_reference,
    z.must_identify_stops,
    z.active_from,
    z.active_to,

    -- Referencia al tipo: ID custom o nombre built-in
    CASE
        WHEN z.id_key_pos > 0
            -- Custom: [{"id": "b22"}] → extraer valor entre las comillas del id
            THEN SUBSTRING(
                z.zone_types_json,
                z.val_start_pos + 1,
                CHARINDEX('"', z.zone_types_json, z.val_start_pos + 1) - z.val_start_pos - 1
            )
        ELSE
            -- Built-in: ["ZoneTypeCustomerId"] → extraer el nombre entre [" y "]
            SUBSTRING(z.zone_types_json, 3, LEN(z.zone_types_json) - 4)
    END                                             AS zone_type_ref,

    CAST(
        CASE WHEN z.id_key_pos > 0 THEN 1 ELSE 0 END
    AS BIT)                                         AS is_custom_type,

    -- Nombre del tipo resuelto (solo para tipos custom con registro en geotab_zone_type)
    zt.zone_type_name

FROM val_pos z
LEFT JOIN {{ source('geotab', 'geotab_zone_type') }} zt
    ON zt.zone_type_id = CASE
        WHEN z.id_key_pos > 0
        THEN SUBSTRING(
            z.zone_types_json,
            z.val_start_pos + 1,
            CHARINDEX('"', z.zone_types_json, z.val_start_pos + 1) - z.val_start_pos - 1
        )
        ELSE NULL
    END),


cte_zonas AS (
    SELECT
        z.zone_id,
        z.zone_name,
        z.zone_type_name                AS zone_type,
        z.external_reference            AS terminal_codigo_externo,
        z.active_from                   AS zona_activa_desde,
        z.active_to                     AS zona_activa_hasta
    FROM stg_geotab__zone z
    WHERE (z.active_to IS NULL OR z.active_to > GETDATE())
      -- AND z.zone_type_name = 'Terminal'
),

cte_zona_ruta AS (
    -- Resuelve zone_id → route vía route_plan_item.
    -- ROW_NUMBER deduplica en caso de que una zona pertenezca a múltiples rutas.
    SELECT
        rpi.zone_id,
        r.route_id,
        r.name as route_name,
        ROW_NUMBER() OVER (PARTITION BY rpi.zone_id ORDER BY r.route_id) AS rn
    FROM {{ source('geotab', 'geotab_route_plan_item') }} rpi
    INNER JOIN {{ source('geotab', 'geotab_route') }} r ON r.route_id = rpi.route_id
),

cte_rules AS (
    SELECT
        r.rule_id,
        r.name as rule_name,
        z.zone_id,
        z.zone_name,
        z.zone_type,
        z.terminal_codigo_externo,
        zr.route_id,
        zr.route_name
    FROM {{ source('geotab', 'geotab_rule') }} r
    LEFT JOIN cte_zonas z
        ON z.zone_name = r.name
    LEFT JOIN cte_zona_ruta zr
        ON zr.zone_id = z.zone_id AND zr.rn = 1
    -- Filtrar solo reglas de tipo ZoneStop
    -- WHERE r.base_type = 'ZoneStop'
),


cte_eventos AS (
    SELECT
        ee.exception_event_id,
        ee.device_id,
        ee.driver_id,
        ee.rule_id,                                 -- puente hacia la zona
        cr.zone_id,                                 -- ← resuelto via Rule
        cr.zone_name,
        cr.zone_type,
        cr.terminal_codigo_externo,
        cr.route_id,
        cr.route_name,
        CONVERT(DATETIME, ee.active_from)           AS active_from,
        CONVERT(DATETIME, ee.active_to)             AS active_to,
        CASE
            WHEN ee.active_from IS NOT NULL
             AND ee.active_to   IS NOT NULL
            THEN CAST(
                DATEDIFF(SECOND, ee.active_from, ee.active_to) / 60.0
                AS DECIMAL(10, 2))
            ELSE NULL
        END                                         AS dwell_time_minutes,
        CONVERT(DATE,     ee.active_from)           AS event_date,
        DATEPART(HOUR,    ee.active_from)           AS entry_hour,
        DATEPART(WEEKDAY, ee.active_from)           AS entry_weekday
    FROM {{ source('geotab', 'geotab_exception_event') }} ee
    -- Join hacia la zona via Rule
    INNER JOIN cte_rules cr
        ON cr.rule_id = ee.rule_id
    {% if is_incremental() %}
    WHERE ee.active_from >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
    {% endif %}
),

cte_viaje_siguiente AS (
    SELECT
        ev.exception_event_id,
        MIN(t.trip_start)   AS next_trip_start,
        MIN(t.trip_id)      AS next_trip_id
    FROM cte_eventos ev
    JOIN {{ source('geotab', 'geotab_trip') }} t
        ON  t.device_id  = ev.device_id
        AND t.trip_start >= ev.active_to
        AND t.trip_start <  DATEADD(HOUR, 4, ev.active_to)
    WHERE ev.active_to IS NOT NULL
    GROUP BY ev.exception_event_id
)

SELECT
    -- Identificadores
    ev.exception_event_id,
    ev.event_date,
    ev.entry_hour,
    ev.entry_weekday,

    -- Terminal (zone_id ahora resuelto via Rule)
    ev.zone_id,
    ev.zone_name                                            AS terminal_name,
    ev.zone_type                                            AS terminal_type,
    ev.terminal_codigo_externo,

    -- Vehículo
    ev.device_id                                            AS vehicle_id,
    d.device_name                                           AS vehicle_name,
    d.device_type                                           AS vehicle_type,

    -- Conductor
    ev.driver_id,

    -- Tiempos del evento en terminal
    ev.active_from                                          AS terminal_entry_datetime,
    ev.active_to                                            AS terminal_exit_datetime,
    ev.dwell_time_minutes,

    -- Clasificación de permanencia
    CASE
        WHEN ev.dwell_time_minutes IS NULL              THEN 'SIN_DATOS'
        WHEN ev.dwell_time_minutes < 5                  THEN 'RAPIDO'
        WHEN ev.dwell_time_minutes BETWEEN 5  AND 15    THEN 'NORMAL'
        WHEN ev.dwell_time_minutes BETWEEN 15 AND 30    THEN 'DEMORADO'
        ELSE                                                 'CUELLO_DE_BOTELLA'
    END                                                     AS dwell_categoria,
    CAST(
        CASE WHEN ev.dwell_time_minutes > 30 THEN 1 ELSE 0 END
    AS BIT)                                                 AS es_cuello_de_botella,

    -- Franja horaria
    CASE
        WHEN ev.entry_hour BETWEEN 5  AND 8  THEN 'MANANA_TEMPRANA'
        WHEN ev.entry_hour BETWEEN 9  AND 11 THEN 'MANANA'
        WHEN ev.entry_hour BETWEEN 12 AND 14 THEN 'MEDIODIA'
        WHEN ev.entry_hour BETWEEN 15 AND 18 THEN 'TARDE'
        WHEN ev.entry_hour BETWEEN 19 AND 22 THEN 'NOCHE'
        ELSE                                     'MADRUGADA'
    END                                                     AS franja_horaria,

    -- Viaje posterior
    vs.next_trip_id,
    vs.next_trip_start,
    DATEDIFF(MINUTE, ev.active_to, vs.next_trip_start)     AS minutos_hasta_viaje,

    -- Ruta asociada al terminal (vía geotab_route_plan_item → geotab_route)
    ev.route_id,
    ev.route_name,

    -- ITINERARIO: pendiente dataset AMA
    CAST(NULL AS DATETIME)                                  AS scheduled_departure,
    CAST(NULL AS INT)                                       AS delay_minutes,
    CAST(NULL AS NVARCHAR(50))                              AS departure_status

FROM cte_eventos ev
LEFT JOIN {{ source('geotab', 'geotab_device') }} d
    ON d.device_id = ev.device_id
LEFT JOIN cte_viaje_siguiente vs
    ON vs.exception_event_id = ev.exception_event_id