{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']    
)}}


SELECT 	fleet_key,
		fleet_name,
		operator_id,
		_md_filename,
		_md_processed_at,
		ROW_NUMBER() OVER (PARTITION BY fleet_key ORDER BY _md_processed_at DESC) as rn 
FROM {{ ref('intermediate_fleet') }}