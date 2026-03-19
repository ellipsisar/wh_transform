{{ config(
    materialized='view',
    tag='dashboard_AMA'
) }}

WITH cte_incidentes AS (
    SELECT
        device_id,
        log_datetime,
        CONVERT(DATE,  log_datetime)    AS fecha,
        DATEPART(HOUR, log_datetime)    AS hora_del_dia,
        CONVERT(FLOAT, latitude)        AS latitud_incidente,
        CONVERT(FLOAT, longitude)       AS longitud_incidente,
        CONVERT(FLOAT, speed_kmh)       AS velocidad_kmh,
        CONVERT(FLOAT, speed_kmh) - 80  AS exceso_kmh
    FROM {{ source('geotab', 'geotab_log_record') }}
    WHERE
        speed_kmh    >  80
        AND log_datetime IS NOT NULL
        AND latitude     IS NOT NULL
        AND longitude    IS NOT NULL
),

cte_device AS (
    SELECT
        device_id,
        device_name,
        comment     AS device_comment,
        active_from,
        active_to
    FROM {{ source('geotab', 'geotab_device') }} 
)

SELECT
    CONVERT(DATETIME, i.log_datetime)   AS fecha_incidente,
    d.device_id                         AS device_id,
    d.device_name                       AS device_name,
    d.device_comment                    AS device_comment,
    i.latitud_incidente,
    i.longitud_incidente,
    i.velocidad_kmh,
    i.exceso_kmh,
    i.fecha,
    i.hora_del_dia,
    CASE
        WHEN i.velocidad_kmh BETWEEN 80  AND  99 THEN 'Leve (80-99 km/h)'
        WHEN i.velocidad_kmh BETWEEN 100 AND 119 THEN 'Moderado (100-119 km/h)'
        WHEN i.velocidad_kmh >= 120              THEN 'Severo (120+ km/h)'
    END                                         AS severidad,
    CASE
        WHEN i.velocidad_kmh BETWEEN 80  AND  99 THEN '#FFC107'  -- amarillo
        WHEN i.velocidad_kmh BETWEEN 100 AND 119 THEN '#FF7043'  -- naranja
        WHEN i.velocidad_kmh >= 120              THEN '#D32F2F'  -- rojo
    END                                         AS color_mapa
FROM cte_incidentes i
    INNER JOIN cte_device d
        ON i.device_id = d.device_id
        AND (d.active_to IS NULL OR d.active_to > i.log_datetime)
