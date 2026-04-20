{{
    config(
    materialized='ephemeral'    
)}}


    SELECT
        device_id,
        device_name,
        comment,
        active_from,
        active_to
    FROM (
        SELECT
            device_id,
            device_name,
            comment,
            active_from,
            active_to,
            ROW_NUMBER() OVER (
                PARTITION BY device_id
                ORDER BY _md_processed_at DESC
            ) AS rn
        FROM {{ source('geotab', 'geotab_device') }}
    ) x
    WHERE rn = 1