-- Devuelve filas si algún viaje tiene actual_departure en el futuro.
-- Un timestamp futuro indica corrupción en geotab_trip (zona horaria mal
-- configurada en el dispositivo o error de ingesta).

SELECT
    trip_id,
    vehicle_name,
    actual_departure,
    GETDATE()                                              AS current_datetime,
    DATEDIFF(MINUTE, GETDATE(), actual_departure)          AS minutes_ahead
FROM {{ ref('AMA_Geotab_Viajes') }}
WHERE actual_departure > GETDATE()
