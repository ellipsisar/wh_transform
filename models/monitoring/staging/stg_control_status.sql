{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    SELECT
        filename,
        status,
        process_datetime,
        error_message,
        record_count
    FROM {{ source('ati_lakehouse', 'control_raw_file_status') }}
    WHERE filename IS NOT NULL
),

parsed AS (
    SELECT
        filename,
        status,
        process_datetime,
        error_message,
        ISNULL(record_count, 0) AS record_count,

        CAST(process_datetime AS DATE) AS event_date,

        CASE
            WHEN filename LIKE 'nb_geotab_%' THEN
                SUBSTRING(filename, 4, ISNULL(NULLIF(CHARINDEX('_2', filename, 4), 0) - 4, LEN(filename)))
            WHEN filename LIKE 'geotab_%' THEN
                LEFT(filename,
                    LEN(filename)
                    - CHARINDEX('_', REVERSE(filename))
                    - CHARINDEX('_', REVERSE(SUBSTRING(filename, 1, LEN(filename) - CHARINDEX('_', REVERSE(filename)))))
                )
            ELSE
                LEFT(filename, ISNULL(NULLIF(CHARINDEX('_2', filename), 0) - 1, LEN(filename) - 5))
        END AS entity_name_raw,

        CASE
            WHEN filename LIKE 'geotab_%' OR filename LIKE 'nb_geotab_%' THEN 'geotab'
            WHEN filename LIKE 'tdev_%'                                   THEN 'transdev'
            WHEN filename LIKE 'sonnell_%'                                THEN 'sonnell'
            WHEN filename LIKE 'hms_%' OR filename LIKE 'HMS_%'           THEN 'hms'
            WHEN filename LIKE 'gtfs_%' OR filename LIKE 'GTFS_%'         THEN 'gtfs'
            WHEN filename LIKE 'fleet%'
              OR filename LIKE 'pattern%'
              OR filename LIKE 'trip%'
              OR filename LIKE 'visit%'
              OR filename LIKE 'vehicle_day%'                             THEN 'ama_legacy'
            ELSE 'other'
        END AS domain

    FROM source
),

cleaned AS (
    SELECT
        filename,
        status,
        process_datetime,
        error_message,
        record_count,
        event_date,
        LOWER(REPLACE(REPLACE(entity_name_raw, '.json', ''), '.csv', '')) AS entity_name,
        LOWER(domain) AS domain
    FROM parsed
)

SELECT
    filename,
    UPPER(ISNULL(status, 'unknown'))  AS status,
    process_datetime,
    error_message,
    record_count,
    event_date,
    ISNULL(entity_name, 'unknown')    AS entity_name,
    domain
FROM cleaned
