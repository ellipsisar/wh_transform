{{
    config(
    materialized='ephemeral',
    tags=['ati_datawarehouse']    
)}}

SELECT 	CAST(fleet_key as nvarchar(4000)) as fleet_key,
		CAST(fleet_name as nvarchar(4000)) as fleet_name,
		CAST(operator_id as nvarchar(4000)) as operator_id,
		CAST(_md_filename as nvarchar(4000)) as _md_filename,
		CAST(_md_processed_at as datetime2(7)) as _md_processed_at	
FROM {{ source('external', 'fleet') }}
