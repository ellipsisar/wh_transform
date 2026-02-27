{{ config(materialized='ephemeral') }}

SELECT
    ServiceDate,
    TripKey,
    ScheduleTrip,
    VehicleId,
    Subsystem,
    VisitKey,
    SequenceInDay,
    ArrivalTime,
    DepartureTime,
    ScheduledArrivaltime,
    ScheduleDepartureTime,
    SequenceInTrip,
    StopId,
    Compliant,
    GeneratedAt,
    CreatedAt,
    Version
FROM {{ source('sonnell', 'SonnellCheckpoints') }}
