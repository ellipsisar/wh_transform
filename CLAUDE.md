# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

This is a **dbt Core** project (`wh_transform`) that transforms data in **Azure Synapse Analytics** (Dedicated SQL Pool, T-SQL dialect). The primary focus is migrating the Sonnell invoice pipeline from SQL Server stored procedures to dbt incremental models.

- **Target DWH**: `ati_datawarehouse` on `ati-cdw-synapse.sql.azuresynapse.net`
- **Schemas in use**: `dbo` (production tables), `raw` (external tables from data lake), `dev` (development target)
- **`profiles.yml`** is at the **repo root** (not in a `profiles/` subdirectory). It contains plaintext credentials — never commit changes to it.

---

## Architecture

### Model Layers

```
models/
├── sonnell/               # Main Sonnell invoice pipeline (see below)
│   ├── staging/           # Views: stg_sonnell_* (normalize raw sources)
│   ├── fct_sonnell_subsystem_cost.sql
│   ├── fct_sonnell_subsystem_offset.sql
│   └── fct_sonnell_invoice_totals.sql
├── ama/                   # AMA dashboard views (tag: dashboard_AMA)
│   ├── AMA_Geotab_Viajes.sql           # Trip detail with delay classification
│   ├── AMA_Geotab_Salida_Vehiculos.sql # Daily departure KPIs (refs AMA_Geotab_Viajes)
│   ├── AMA_Geotab_Exceso_Velocidad.sql # Speed violations (>80 km/h)
│   ├── AMA_Geotab_GPS_Comunicacion.sql # Device communication & GPS status
│   ├── AMA_Geotab_Terminales.sql       # Terminal dwell events (exception events)
│   └── intermediate/
│       └── stg_geotab__zone.sql        # Ephemeral: parses zone_types_json
├── sources/               # Source YAML definitions (sonnell.yml, gtfs.yml, korbato.yml, tdev.yml, geotab.yml)
├── dimOperators.sql       # Legacy model (root level, no schema file)
├── dimRoutes.sql          # Legacy model (root level, no schema file)
├── FrequencyDailySummary.sql  # Legacy model (root level)
└── temp/                  # Ad-hoc/exploratory models
macros/
├── sonnell_recalc_subsystem_costs.sql
├── sonnell_recalc_offsets_by_month.sql
└── sonnell_recalc_invoice_by_month.sql
```

### AMA/Geotab Pipeline

Dashboard views for AMA fleet monitoring, tagged `dashboard_AMA`. Source schema: `dbo` (source name: `geotab`).

**Geotab source tables registered** (`models/sources/geotab.yml`): `geotab_trip`, `geotab_device`, `geotab_device_status_info`, `geotab_log_record`, `geotab_exception_event`, `geotab_zone`, `geotab_zone_type`, `geotab_rule`, `geotab_route`, `geotab_route_plan_item`, `geotab_fault_data`, `geotab_group`, `geotab_status_data`, `ama_itinerario`

**Model dependencies**:
```
geotab_trip + geotab_device → AMA_Geotab_Viajes → AMA_Geotab_Salida_Vehiculos
geotab_log_record + geotab_device → AMA_Geotab_Exceso_Velocidad
geotab_device + geotab_device_status_info + geotab_log_record + geotab_trip → AMA_Geotab_GPS_Comunicacion
geotab_exception_event + geotab_rule + geotab_zone + geotab_zone_type + geotab_route_plan_item + geotab_route + geotab_device + geotab_trip
  → stg_geotab__zone (ephemeral) → AMA_Geotab_Terminales
```

**Pending work**: `is_central_departure` usa `'SGDO'` como placeholder — confirmar con AMA el código exacto de la terminal Central. Speed violation threshold is hardcoded at 80 km/h.

**Itinerario AMA** (`dbo.ama_itinerario`): disponible desde 2026-03-26. Join: `ama_itinerario.tren = geotab_device.device_name`. Se filtra `orden_parada = 0` para obtener la terminal y hora de salida programada. Se matchea por día de semana (`servicio`: LUNES-VIERNES / SABADO / DOMINGO). Para viajes con múltiples turnos posibles, se elige el turno cuya hora programada sea más cercana a la salida real.

**Synapse config per model**:
| Model | dist | index | Incremental window | Tags |
|---|---|---|---|---|
| `AMA_Geotab_Viajes` | `HASH(vehicle_id)` | CCI | last 1 day on `trip_start` | `dashboard_AMA` |
| `AMA_Geotab_Salida_Vehiculos` | `HASH(route_id)` | CCI | last 1 day on `[date]` | `dashboard_AMA` |
| `AMA_Geotab_Exceso_Velocidad` | `HASH(device_id)` | CCI | last 1 day on `log_datetime` | `dashboard_AMA` |
| `AMA_Geotab_GPS_Comunicacion` | `HASH(device_id)` | CCI | daily snapshot by `snapshot_date` | `dashboard_AMA` |
| `AMA_Geotab_Terminales` | `HASH(zone_id)` | CCI | last 1 day on `event_date` | `dashboard_AMA`, `terminales_AMA` |
| `stg_geotab__zone` (ephemeral) | — | — | — | `dashboard_AMA`, `terminales_AMA` |

**GPS_Comunicacion es un snapshot diario**: grain `(device_id, snapshot_date)`. `pre_hook` elimina el snapshot de hoy antes de re-insertar — idempotente. Los campos rolling (viajes_ultimos_30_dias, etc.) siempre se calculan desde el histórico completo de la fuente.

**AMA_Geotab_Terminales**: tracks dwell events at terminals (exception events). Resolution chain: `geotab_exception_event` → `geotab_rule` → zone (via `stg_geotab__zone` ephemeral) → `geotab_route_plan_item` → `geotab_route`. Joins next trip start within 4 hours of terminal exit. `stg_geotab__zone` (ephemeral, `models/ama/intermediate/`) parses `zone_types_json` via `CHARINDEX`/`SUBSTRING` since Geotab stores zone type as JSON (two formats: built-in `["ZoneTypeCustomerId"]` vs custom `[{"id": "b22"}]`). Itinerario fields (`scheduled_departure`, `delay_minutes`, `departure_status`) are placeholder NULLs pending AMA dataset integration.

**Incremental pattern** (todos los modelos): `pre_hook DELETE` del día anterior + `append`. Nunca MERGE. Idempotente ante re-ejecuciones del mismo día.

**Run AMA models**:
```bash
dbt run --profiles-dir . -s tag:dashboard_AMA

# Full refresh (reconstruye desde cero)
dbt run --profiles-dir . --full-refresh -s tag:dashboard_AMA
```

---

### Sonnell Pipeline

Migration of 3 stored procedure phases to dbt. All fact models use `materialized='incremental'`, `incremental_strategy='append'`, `dist='ROUND_ROBIN'`, `index='CLUSTERED COLUMNSTORE INDEX'`, and SCD Type 2 via a `CurrentVersion` flag.

**SP-to-dbt mapping**:
| Phase | Stored Procedure | dbt Equivalent |
|---|---|---|
| Fase I (daily) | `Sonnell_UpdateSubsystemCostsVersions` | `fct_sonnell_subsystem_cost` (default run) |
| Fase I (reprocess) | `Sonnell_CalculateSubsystemCostsByMonth` | model with `sonnell_reprocess` vars or macro |
| Fase II (daily) | `Sonnell_UpdateOffsetVersions` | `fct_sonnell_subsystem_offset` (default run) |
| Fase II (reprocess) | `Sonnell_CalculateOffsetsByMonth` + `Sonnell_FetchOffsetValuesByMonth` | model with `sonnell_offset_reprocess` vars or macro |
| Fase III SP1 (daily) | `Sonnell_UpdateInvoiceVersions` | `fct_sonnell_invoice_totals` (default run) |
| Fase III SP2 (daily stats) | `Sonnell_CalculateInvoiceTotalsStatColumns` | `fct_sonnell_invoice_totals` post_hooks |
| Fase III SP3 (reprocess) | `Sonnell_CalculateInvoiceTotalsByMonth` | model with `sonnell_invoice_reprocess` vars or macro |

**Staging model transforms**:
| Model | Transforms |
|---|---|
| `stg_sonnell_daily_summary` | Renames `Subsystem`→`GroupId`, `TripCount`→`NumTrips`; converts `RevenueSeconds`→`RevenueHours` (÷3600) and `RevenueMeters`→`RevenueMiles` (÷1609.34) |
| `stg_sonnell_rates` | Pass-through |
| `stg_sonnell_checkpoints` | Pass-through |
| `stg_sonnell_parameters` | Pass-through |

**Sources** (dbo schema): `SonnellDailySummary`, `SonnellRates`, `SonnellCheckpoints`, `SonnellParameters`, `SonnellOffsetApplied`

**Raw sources** (raw schema): `sonnell_trip`, `sonnell_subsystem`, `sonnell_checkpoin` — external tables from data lake (Parquet)

**Data flow**:
```
SonnellDailySummary → stg_sonnell_daily_summary ──┐
SonnellRates        → stg_sonnell_rates ───────────┼──► fct_sonnell_subsystem_cost ──┐
SonnellCheckpoints  → stg_sonnell_checkpoints ─────┤                                 ├──► fct_sonnell_invoice_totals
SonnellParameters   → stg_sonnell_parameters ───────┴──► fct_sonnell_subsystem_offset ┘
```

**Target tables** (dbt aliases):
| Model | Alias |
|---|---|
| `fct_sonnell_subsystem_cost` | `dbt_SonnellSubsystemCost` |
| `fct_sonnell_subsystem_offset` | `dbt_SonnellSubsystemOffset` |
| `fct_sonnell_invoice_totals` | `dbt_SonnellInvoiceTotals` |

### Execution Modes (all 3 fact models)

Each fact model supports 3 modes controlled by dbt vars and `is_incremental()`:

1. **Full refresh** (`--full-refresh`): rebuilds from scratch, no `pre_hook` version management
2. **Daily incremental** (default): `pre_hook` marks old versions `CurrentVersion=0`; inserts new rows with `NOT EXISTS` dedup; only processes closed months
3. **Monthly reprocess**: `pre_hook` DELETEs rows for specified month; inserts fresh data for that month

**Reprocess vars by model**:
| Model | Flag var | Month var | Year var |
|---|---|---|---|
| `fct_sonnell_subsystem_cost` | `sonnell_reprocess` | `sonnell_reprocess_month` | `sonnell_reprocess_year` |
| `fct_sonnell_subsystem_offset` | `sonnell_offset_reprocess` | `sonnell_offset_reprocess_month` | `sonnell_offset_reprocess_year` |
| `fct_sonnell_invoice_totals` | `sonnell_invoice_reprocess` | `sonnell_invoice_reprocess_month` | `sonnell_invoice_reprocess_year` |

### Daily vs Reprocess Behavioral Differences

| Aspect | Daily incremental | Monthly reprocess |
|---|---|---|
| Version handling | `pre_hook` marks old rows `CurrentVersion=0` | `pre_hook` DELETEs rows for the month |
| `CurrentVersion` filter on sources | `= 1` (current data only) | No filter (all versions) |
| `IsActive` on `SonnellRates` (MU) | Not applied | `IsActive = 1` |
| InvoiceID generation | One `NEWID()` per `(Year, Month)` via CTE | Single `NEWID()` for entire month via `CROSS JOIN` |
| InvoiceID propagation | JOIN by `(Year, Month, Subsystem)` | `CROSS JOIN` to all rows in the month |
| `SonnellOffsetApplied` | INSERT with NOT EXISTS guard | DELETE first, then INSERT |
| Stats (Trips, Revenue, Checkpoints) | `CurrentVersion = 1` filter on cost/offset | No `CurrentVersion` filter |
| Scope | All closed months with new data | Single specified month |

### Invoice Totals Model Details

`fct_sonnell_invoice_totals` grain is `(Year, Month, Subsystem, Type, Version)` and produces 3 `UNION ALL` blocks:
- **Block 1 — Regular MB/TC**: from `fct_sonnell_subsystem_offset` × `fct_sonnell_subsystem_cost`
- **Block 2 — Regular MU**: recalculates from raw `SonnellDailySummary × SonnellRates` (not from cost table, to match SP rounding with `ROUND(SUM(meters)/1609.34, 2)`)
- **Block 3 — Offset**: from `fct_sonnell_subsystem_offset` directly

Post-hooks (10 sequential): InvoiceID generation → SonnellOffsetApplied DELETE (reprocess) → SonnellOffsetApplied INSERT (4% cap audit) → InvoiceID propagation to offset/cost tables → Trips (MB/TC) → Trips (MU) → RevenueMiles/Hours → Checkpoints → Timestamps.

**Key design decisions**:
- `append` strategy (not `merge`) to replicate SP INSERT+UPDATE patterns; idempotent via `NOT EXISTS` guards
- `post_hook` array for cross-table writes (InvoiceID propagation, SonnellOffsetApplied)
- MU block uses `source()` not `ref(fct_sonnell_subsystem_cost)` — raw rounding from SP must be preserved
- MU route stats update (RevenueMiles/Hours) is a no-op in daily mode — intentional SP behavior replication
- `IsActive` filter on `SonnellRates` for MU: absent in daily mode, present in reprocess mode (matches SP difference)
- Reprocess macros use `api.Relation.create()` since `ref()` and `source()` are unavailable in `run-operation` context

---

## Common Commands

All commands require `--profiles-dir .` unless `profiles.yml` is symlinked to `~/.dbt/`.

```bash
# Run all Sonnell models (tagged)
dbt run --profiles-dir . -s tag:sonnell

# Full refresh
dbt run --profiles-dir . --full-refresh -s tag:sonnell

# Monthly reprocess (example: January 2026)
dbt run --profiles-dir . -s fct_sonnell_invoice_totals \
  --vars '{sonnell_invoice_reprocess: true, sonnell_invoice_reprocess_month: 1, sonnell_invoice_reprocess_year: 2026}'

# Reprocess via macro (alternative)
dbt run-operation --profiles-dir . sonnell_recalc_invoice_by_month --args '{"year": 2026, "month": 1}'

# Run/test a single model
dbt run --profiles-dir . --select <model_name>
dbt test --profiles-dir . --select <model_name>

# Compile without executing
dbt compile --profiles-dir . --select <model_name>
```

---

## Coding Rules

### Naming Conventions
- Staging: `stg_<source>__<entity>` (double underscore)
- Facts: `fct_<entity>`
- Dimensions: `dim_<entity>`
- Sources YAML: define both `sonnell` (dbo) and `sonnell_raw` (raw schema) separately

### T-SQL / Synapse Rules
- Use `CAST()` / `CONVERT()` — never `::` PostgreSQL casting
- Use `ISNULL()` for null coalescing where performance matters
- Use `GETDATE()` for current timestamp
- Avoid `MERGE` — use `DELETE + INSERT` for incremental models (Synapse MERGE has known issues)
- New tables: always specify `dist` and `index` in model config
- Prefer CTEs over nested subqueries

### dbt Rules
- Every model must have a `.yml` schema entry with `description`, `columns`, and `not_null`/`unique` tests on PKs
- Use `ref()` and `source()` — never hardcode schema/table names
- `ref()` cannot be used in `run-operation` macros — use `api.Relation.create(database, schema, identifier)`
- `is_incremental()` blocks in `pre_hook`/`post_hook` use Jinja strings — test both incremental and full-refresh paths
- Avoid `SELECT *` in staging models that pass through columns — be explicit

### dbt_project.yml Materialization Defaults
```yaml
models:
  wh_transform:
    sonnell:
      staging:
        +materialized: view   # all stg_* models
    # fact models set materialized='incremental' inline via config()
```

---

## Known Gotchas

- **`profiles.yml` is at repo root**, not `~/.dbt/` — pass `--profiles-dir .` or symlink if needed
- **Post-hooks are skipped on full refresh** — InvoiceID, Trips, Timestamps remain NULL until next incremental run
- **4% offset cap rule is informational only** — it does NOT modify invoice line Totals; it only writes to `SonnellOffsetApplied`
- **Columns that are never populated**: `ReenueMiles` (typo column), `ApprovalStatus`, `PaymentStatus` in the original SonnellInvoiceTotals table
- **`CurrentVersion` is `BIT` (0/1) not INT** — use `CAST(1 AS BIT)` in SELECT lists
- **AMA models have no `schema.yml`** — the `models/ama/` directory has no schema file, violating the project's coding rule. Adding one is pending work.
- **`dbt_project.yml` has no AMA section** — AMA models are not listed under `models.wh_transform` in `dbt_project.yml`; they rely entirely on inline `{{ config() }}` blocks for materialization and distribution settings.
