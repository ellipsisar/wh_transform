{{
    config(materialized='ephemeral')
}}

-- Parseo de zone_types_json → zone_type_ref + join a geotab_zone_type.
--
-- geotab_zone no tiene columna zone_type_id: el tipo se almacena como JSON.
-- Dos formatos posibles:
--   - Built-in : ["ZoneTypeCustomerId"]  → zone_type_ref = "ZoneTypeCustomerId", sin FK
--   - Custom   : [{"id": "b22"}]         → zone_type_ref = "b22", FK a geotab_zone_type
--
-- is_custom_type = 1  →  zone_type_name resuelto desde geotab_zone_type
-- is_custom_type = 0  →  zone_type_name es NULL (tipo built-in de Geotab, sin registro propio)

WITH pos AS (
    SELECT
        zone_id,
        zone_name,
        zone_types_json,
        external_reference,
        must_identify_stops,
        active_from,
        active_to,
        -- posición del token "id" dentro del JSON
        CHARINDEX('"id"', zone_types_json)          AS id_key_pos
    FROM {{ source('geotab', 'geotab_zone') }}
),

val_pos AS (
    SELECT
        *,
        -- posición del primer " que abre el valor del id (sólo relevante si id_key_pos > 0)
        CASE
            WHEN id_key_pos > 0
            THEN CHARINDEX('"', zone_types_json,
                     CHARINDEX(':', zone_types_json, id_key_pos + 4) + 1)
            ELSE 0
        END                                         AS val_start_pos
    FROM pos
)

SELECT
    z.zone_id,
    z.zone_name,
    z.external_reference,
    z.must_identify_stops,
    z.active_from,
    z.active_to,

    -- Referencia al tipo: ID custom o nombre built-in
    CASE
        WHEN z.id_key_pos > 0
            -- Custom: [{"id": "b22"}] → extraer valor entre las comillas del id
            THEN SUBSTRING(
                z.zone_types_json,
                z.val_start_pos + 1,
                CHARINDEX('"', z.zone_types_json, z.val_start_pos + 1) - z.val_start_pos - 1
            )
        ELSE
            -- Built-in: ["ZoneTypeCustomerId"] → extraer el nombre entre [" y "]
            SUBSTRING(z.zone_types_json, 3, LEN(z.zone_types_json) - 4)
    END                                             AS zone_type_ref,

    CAST(
        CASE WHEN z.id_key_pos > 0 THEN 1 ELSE 0 END
    AS BIT)                                         AS is_custom_type,

    -- Nombre del tipo resuelto (solo para tipos custom con registro en geotab_zone_type)
    zt.zone_type_name

FROM val_pos z
LEFT JOIN {{ source('geotab', 'geotab_zone_type') }} zt
    ON zt.zone_type_id = CASE
        WHEN z.id_key_pos > 0
        THEN SUBSTRING(
            z.zone_types_json,
            z.val_start_pos + 1,
            CHARINDEX('"', z.zone_types_json, z.val_start_pos + 1) - z.val_start_pos - 1
        )
        ELSE NULL
    END
