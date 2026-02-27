{{ config(materialized='ephemeral') }}

SELECT
    Id,
    GroupId,
    CheckPointOTP,
    IncentivesOffsets,
    CheckPointOTPMax,
    CheckPointOTPMin
FROM {{ source('sonnell', 'SonnellParameters') }}
