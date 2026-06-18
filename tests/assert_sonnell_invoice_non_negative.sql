-- Devuelve filas si alguna línea Regular de factura tiene Total negativo.
-- Un Total < 0 en tipo 'Regular' indica error de cálculo — los créditos
-- van exclusivamente por líneas de tipo 'Offset'.

SELECT
    [Year],
    [Month],
    Subsystem,
    [Type],
    Total,
    Version
FROM {{ ref('fct_sonnell_invoice_totals') }}
WHERE CurrentVersion = CAST(1 AS BIT)
  AND [Type] = 'Regular'
  AND Total < 0
