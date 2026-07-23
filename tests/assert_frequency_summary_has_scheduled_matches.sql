-- Devuelve una fila si, en los últimos 7 días de servicio, no hay NINGÚN viaje
-- matcheado contra el itinerario GTFS (PlannedTripsCount = 0 en todas las filas).
-- Esto es la segunda firma del bug de 2026-07: cuando el join a trip_gtfs_match
-- rompe, PlannedTripsCount/DelayedTripsCount/OnTimeDepartureCount/OnTimeArrivalCount/
-- DelayedDepartureCount/DelayedArrivalCount/ExecutedPlannedTripsCount quedan todos en 0.

SELECT
    MIN(ServiceDate) AS from_date,
    MAX(ServiceDate) AS to_date,
    SUM(PlannedTripsCount) AS total_planned_trips
FROM {{ ref('FrequencyDailySummary') }}
WHERE ServiceDate >= DATEADD(day, -7, CAST(GETDATE() AS date))
HAVING SUM(PlannedTripsCount) = 0
