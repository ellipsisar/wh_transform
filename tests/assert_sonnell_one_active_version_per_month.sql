-- Devuelve filas si un mismo (Year, Month) tiene más de una Version con
-- CurrentVersion = 1 en fct_sonnell_invoice_totals.
-- Más de una versión activa indica que el pre_hook de SCD Type 2 falló
-- y no marcó la versión anterior como CurrentVersion = 0.

SELECT
    [Year],
    [Month],
    COUNT(DISTINCT Version) AS versiones_activas
FROM {{ ref('fct_sonnell_invoice_totals') }}
WHERE CurrentVersion = CAST(1 AS BIT)
GROUP BY [Year], [Month]
HAVING COUNT(DISTINCT Version) > 1
