{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(device_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tag='dashboard_AMA',
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE snapshot_date = CAST(GETDATE() AS DATE)\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

-- Modelo de snapshot diario: una fila por dispositivo activo por día.
-- Los campos de ventana rolling (viajes_ultimos_30_dias, etc.) se calculan
-- siempre desde el histórico completo de geotab_trip y se congelan al momento
-- de la carga, permitiendo tracking histórico de estado de flota.

WITH
cte_ultimo_gps AS (
    SELECT
        device_id,
        MAX(log_datetime)           AS ultima_posicion_gps,
        COUNT(*)                    AS total_pings_historico
    FROM {{ source('geotab', 'geotab_log_record') }}
    GROUP BY device_id
),

cte_viajes AS (
    SELECT
        device_id,
        MAX(trip_start)             AS ultimo_viaje,
        SUM(CASE WHEN trip_start >= DATEADD(DAY, -30, GETDATE())
                 THEN 1 ELSE 0 END) AS viajes_ultimos_30_dias,
        SUM(CASE WHEN trip_start >= DATEADD(DAY, -7,  GETDATE())
                 THEN 1 ELSE 0 END) AS viajes_ultimos_7_dias,
        SUM(CASE WHEN trip_start >= DATEADD(DAY, -30, GETDATE())
                 THEN distance_km ELSE 0.0 END) AS km_ultimos_30_dias
    FROM {{ source('geotab', 'geotab_trip') }}
    GROUP BY device_id
)

SELECT
    CAST(GETDATE() AS DATE)                             AS snapshot_date,
    d.device_id                                         AS device_id,
    d.device_name                                       AS device_name,
    d.comment                                           AS device_comment,
    CONVERT(DATETIME, si.status_datetime)               AS ultima_fecha_de_comunicacion,
    CONVERT(BIT, ISNULL(si.is_device_communicating, 0)) AS is_device_communicating,
    DATEDIFF(DAY, si.status_datetime, GETDATE())        AS dias_sin_comunicacion,
    CONVERT(DATETIME, g.ultima_posicion_gps)            AS ultima_posicion_gps,
    DATEDIFF(DAY, g.ultima_posicion_gps, GETDATE())     AS dias_sin_gps,
    CONVERT(BIT, ISNULL(si.is_driving, 0))              AS esta_conduciendo,
    si.speed                                            AS velocidad_actual_kmh,
    si.latitude                                         AS ultima_latitud,
    si.longitude                                        AS ultima_longitud,
    CONVERT(DATETIME, v.ultimo_viaje)                   AS ultimo_viaje,
    DATEDIFF(DAY, v.ultimo_viaje, GETDATE())            AS dias_sin_viaje,
    ISNULL(v.viajes_ultimos_7_dias,  0)                 AS viajes_ultimos_7_dias,
    ISNULL(v.viajes_ultimos_30_dias, 0)                 AS viajes_ultimos_30_dias,
    ISNULL(v.km_ultimos_30_dias,     0.0)               AS km_ultimos_30_dias,
    ISNULL(g.total_pings_historico,  0)                 AS total_pings_gps_historico,
    CONVERT(DATE, d.active_from)                        AS dispositivo_activo_desde,
    CONVERT(DATE, d.active_to)                          AS dispositivo_activo_hasta,
    CONVERT(BIT,
        CASE WHEN d.active_to IS NULL OR d.active_to > GETDATE()
             THEN 1 ELSE 0 END
    )                                                   AS es_dispositivo_vigente,
    CASE
        WHEN si.device_id IS NULL
            THEN 'Sin registro de estado'
        WHEN ISNULL(si.is_device_communicating, 0) = 1
            THEN 'Comunicando'
        WHEN DATEDIFF(DAY, si.status_datetime, GETDATE()) = 0
            THEN 'Sin comunicacion (hoy)'
        WHEN DATEDIFF(DAY, si.status_datetime, GETDATE()) BETWEEN 1 AND 1
            THEN 'Sin comunicacion (1 dia)'
        WHEN DATEDIFF(DAY, si.status_datetime, GETDATE()) BETWEEN 2 AND 7
            THEN 'Sin comunicacion (2-7 dias)'
        WHEN DATEDIFF(DAY, si.status_datetime, GETDATE()) BETWEEN 8 AND 30
            THEN 'Sin comunicacion (8-30 dias)'
        ELSE 'Sin comunicacion (mas de 30 dias)'
    END                                                 AS categoria_comunicacion,
    CASE
        WHEN g.device_id IS NULL
            THEN 'Sin registros GPS'
        WHEN DATEDIFF(DAY, g.ultima_posicion_gps, GETDATE()) = 0
            THEN 'GPS activo (hoy)'
        WHEN DATEDIFF(DAY, g.ultima_posicion_gps, GETDATE()) BETWEEN 1 AND 7
            THEN 'GPS inactivo (1-7 dias)'
        WHEN DATEDIFF(DAY, g.ultima_posicion_gps, GETDATE()) BETWEEN 8 AND 30
            THEN 'GPS inactivo (8-30 dias)'
        ELSE 'GPS inactivo (mas de 30 dias)'
    END                                                 AS categoria_gps
FROM {{ source('geotab', 'geotab_device') }} d
    LEFT JOIN {{ source('geotab', 'geotab_device_status_info') }} si
        ON d.device_id = si.device_id
    LEFT JOIN cte_ultimo_gps g
        ON d.device_id = g.device_id
    LEFT JOIN cte_viajes v
        ON d.device_id = v.device_id
WHERE
    (d.active_to IS NULL OR d.active_to > GETDATE())
