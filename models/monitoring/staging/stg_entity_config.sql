{{
  config(
    materialized = 'ephemeral'
  )
}}

/*
  Domain to source mapping (CAMBIO 2):
  - sonnel, transdev, data_legacy → korbato
  - ama → geotab
  - gtfs → other

  Expected frequency (CAMBIO 4):
  - korbato sources: 24 hrs
  - geotab sources: 1 hr
  - other sources: NULL (no SLA)
*/

SELECT 'sonnel'       AS domain, 'korbato' AS source, 24   AS expected_frequency_hrs, 'Sonnell bus data'              AS domain_description
UNION ALL SELECT 'transdev',     'korbato',           24,                              'Transdev bus data'
UNION ALL SELECT 'data_legacy',  'korbato',           24,                              'AMA legacy data'
UNION ALL SELECT 'ama',          'geotab',            1,                               'AMA Geotab API'
UNION ALL SELECT 'gtfs',         'other',             NULL,                            'GTFS feed updates'
