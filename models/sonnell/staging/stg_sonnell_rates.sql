{{ config(materialized='ephemeral') }}

SELECT
    Id,
    GroupId,
    StartDate,
    EndDate,
    Currency,
    RateMiles,
    RateHours,
    IsActive
FROM {{ source('sonnell', 'SonnellRates') }}
