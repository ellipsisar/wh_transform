{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT 'geotab'     AS domain, 24  AS expected_frequency_hrs, 'AMA Geotab API'              AS domain_description
UNION ALL SELECT 'transdev',   24,                             'Transdev bus data'
UNION ALL SELECT 'sonnell',    24,                             'Sonnell bus data'
UNION ALL SELECT 'hms',        168,                            'HMS monthly trips (weekly tolerance)'
UNION ALL SELECT 'gtfs',       168,                            'GTFS feed updates'
UNION ALL SELECT 'ama_legacy', 24,                             'AMA legacy data'
UNION ALL SELECT 'other',      24,                             'Other sources'
