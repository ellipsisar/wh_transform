{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']    
)}}


SELECT *models/ati_datawarehouse/deduplicated/staging_fleet.sql$0models/ati_datawarehouse/deduplicated/staging_fleet.sql$0
FROM {{ ref('intermediate_trip')