{{
    config(
    materialized='table',
    dist='HASH(fleet_key)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['ati_datawarehouse']    
)}}

SELECT 	fleet_key,
		fleet_name,
		operator_id,
		_md_filename,
		_md_processed_at
FROM {{ ref('staging_fleet') }}
WHERE rn=1