{{
  config(
    materialized = 'ephemeral'
  )
}}

SELECT
    filename,
    UPPER(ISNULL(status, 'unknown'))  AS status,
    process_datetime,
    error_message,
    record_count,
    event_date,
    ISNULL(entity_name, 'unknown')    AS entity_name,
    domain,
    entity_type,
    entity_base_name,
    snapshot_file_date

FROM (
    SELECT
        filename,
        status,
        process_datetime,
        error_message,
        record_count,
        event_date,
        entity_name,

        -- Domain classification: recurring files by prefix, snapshots by entity_base_name
        CASE
            -- Recurring files: classify by filename prefix
            WHEN entity_type = 'recurring' THEN
                CASE
                    WHEN filename LIKE 'geotab_%' OR filename LIKE 'nb_geotab_%'   THEN 'geotab'
                    WHEN filename LIKE 'tdev_%'                                     THEN 'transdev'
                    WHEN filename LIKE 'sonnell_%'                                  THEN 'sonnell'
                    WHEN filename LIKE 'hms_%' OR filename LIKE 'HMS_%'             THEN 'hms'
                    WHEN filename LIKE 'gtfs_%' OR filename LIKE 'GTFS_%'           THEN 'gtfs'
                    WHEN filename LIKE 'fleet%'
                      OR filename LIKE 'pattern%'
                      OR filename LIKE 'trip%'
                      OR filename LIKE 'visit%'
                      OR filename LIKE 'vehicle_day%'                               THEN 'ama_legacy'
                    ELSE 'other'
                END

            -- Snapshot files: classify by entity_base_name
            WHEN entity_type = 'snapshot' THEN
                CASE
                    WHEN entity_base_name LIKE 'tdev_%'                                                                 THEN 'snapshot_transdev'
                    WHEN entity_base_name LIKE 'sonnell_%'                                                              THEN 'snapshot_sonnell'
                    WHEN entity_base_name IN ('visit', 'trip', 'vehicle_day', 'trip_sch_match')                         THEN 'snapshot_ama_legacy'
                    ELSE 'snapshot_other'
                END

            ELSE 'other'
        END AS domain,

        entity_type,
        entity_base_name,
        snapshot_file_date

    FROM (
        SELECT
            filename,
            status,
            process_datetime,
            error_message,
            ISNULL(record_count, 0)        AS record_count,
            CAST(process_datetime AS DATE) AS event_date,

            LOWER(REPLACE(REPLACE(
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
                END
            , '.json', ''), '.csv', ''))   AS entity_name,

            -- Entity type detection: snapshot if YYYYMMDD_ prefix
            CASE
                WHEN filename LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_%' THEN 'snapshot'
                ELSE 'recurring'
            END AS entity_type,

            -- Entity base name: strip YYYYMMDD_ from snapshots, keep entity_name for recurring
            CASE
                WHEN filename LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_%'
                THEN LOWER(REPLACE(REPLACE(
                    LEFT(
                        SUBSTRING(filename, 10, LEN(filename)),
                        ISNULL(NULLIF(CHARINDEX('_2', SUBSTRING(filename, 10, LEN(filename))), 0) - 1, LEN(SUBSTRING(filename, 10, LEN(filename))) - 5)
                    )
                , '.json', ''), '.csv', ''))
                ELSE LOWER(REPLACE(REPLACE(
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
                    END
                , '.json', ''), '.csv', ''))
            END AS entity_base_name,

            -- Snapshot file date: parse YYYYMMDD from filename prefix
            CASE
                WHEN filename LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_%'
                THEN TRY_CAST(
                    SUBSTRING(filename, 1, 4) + '-' + SUBSTRING(filename, 5, 2) + '-' + SUBSTRING(filename, 7, 2)
                    AS DATE
                )
                ELSE NULL
            END AS snapshot_file_date

        FROM {{ source('ati_lakehouse', 'control_raw_file_status') }}
        WHERE filename IS NOT NULL
    ) AS entity_parsed
) AS domain_parsed
