{{ config(
    materialized='view'
) }}

select  SUM(CASE WHEN Compliant = 'f' THEN 1 ELSE 0 END) as TotalCompliant0,
        SUM(CASE WHEN Compliant = 't' THEN 1 ELSE 0 END) as TotalCompliant1,
        CASE WHEN COUNT(*) > 0 THEN ROUND(SUM(CASE WHEN Compliant = 't' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) ELSE 0 END as OnTimePerformance
FROM {{ source('tdev_dev', 'TDEVCheckpoints') }} T1