-- Devuelve filas cuando el offset de un subsistema supera el 4% del costo total.
-- Según la regla de negocio, el cap del 4% es informacional: no bloquea la factura
-- pero debe auditarse. Este test alerta cuando se supera el umbral.

SELECT
    [Year],
    [Month],
    Subsystem,
    TotalSubsystemCost,
    TotalOffset,
    ROUND(ABS(TotalOffset) / NULLIF(TotalSubsystemCost, 0) * 100, 2) AS offset_pct
FROM {{ ref('fct_sonnell_subsystem_offset') }}
WHERE CurrentVersion = CAST(1 AS BIT)
  AND TotalSubsystemCost > 0
  AND ABS(TotalOffset) / NULLIF(TotalSubsystemCost, 0) > 0.04
