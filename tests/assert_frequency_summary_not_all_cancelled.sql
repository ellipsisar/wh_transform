-- Devuelve filas si, para alguna ServiceDate reciente, el 100% de las rutas tienen
-- OperatedTripsCount = CancelledTripsCount. Un match legítimo del itinerario GTFS
-- debería dejar la gran mayoría de los viajes como no-cancelados; que TODAS las
-- rutas de un día aparezcan como "totalmente canceladas" es la firma del bug de
-- 2026-07 donde el join a trip_gtfs_match rompía silenciosamente (ver
-- models/temp/schema.yml).

SELECT
    ServiceDate,
    COUNT(*) AS route_rows,
    SUM(CASE WHEN OperatedTripsCount = CancelledTripsCount THEN 1 ELSE 0 END) AS all_cancelled_rows
FROM {{ ref('FrequencyDailySummary') }}
WHERE ServiceDate >= DATEADD(day, -7, CAST(GETDATE() AS date))
GROUP BY ServiceDate
HAVING COUNT(*) > 0
   AND COUNT(*) = SUM(CASE WHEN OperatedTripsCount = CancelledTripsCount THEN 1 ELSE 0 END)
