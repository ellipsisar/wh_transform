{{ config(
    materialized='incremental',
    incremental_strategy='append',
    dist='HASH(route_id)',
    index='CLUSTERED COLUMNSTORE INDEX',
    tags=['dashboard_AMA'],
    pre_hook="{% if is_incremental() %}\
        DELETE FROM {{ this }}\
        WHERE [date] >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))\
    {% else %} SELECT 1 AS noop {% endif %}"
) }}

SELECT 	CAST(_md_filename AS DATE)  AS [date],
		route_id,
    	route_name,
    	COUNT(DISTINCT CASE WHEN status = 'ON_TIME' THEN device_id END) AS cantidad_on_schedule_departure,
    	COUNT(DISTINCT CASE WHEN status = 'MISSED'  THEN device_id END) AS cantidad_sin_itinerario,
        COUNT(DISTINCT CASE WHEN status = 'LATE'    THEN device_id END) AS cantidad_delayed_departure,
        COUNT(DISTINCT CASE WHEN status = 'EARLY'   THEN device_id END) AS cantidad_early_departure,
    	COUNT(DISTINCT device_id)                                   	AS cantidad_vehicles,
    	100.0 * COUNT(DISTINCT CASE WHEN status = 'ON_TIME' THEN device_id END) / 
    		COUNT(DISTINCT device_id) 									AS pct_on_schedule,
		AVG(variance_minutes)										AS avg_delay_minutes,
    	MAX(variance_minutes)										AS max_delay_minutes	
FROM {{ source('geotab', 'geotab_planned_vs_actual') }}
WHERE stop_sequence = 0
{% if is_incremental() %}
      AND CAST(_md_filename AS DATE) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
{% endif %}
GROUP BY CAST(_md_filename AS DATE), route_id, route_name

