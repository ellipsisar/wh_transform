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
│   └── AMA_Geotab_GPS_Comunicacion.sql # Device communication & GPS status
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

Dashboard views for AMA fleet monitoring, all `materialized='view'`, tagged `dashboard_AMA`. Source schema: `dbo` (source name: `geotab`).

**Geotab source tables used**: `geotab_trip`, `geotab_device`, `geotab_device_status_info`, `geotab_log_record`

**Model dependencies**:
```
geotab_trip + geotab_device → AMA_Geotab_Viajes → AMA_Geotab_Salida_Vehiculos
geotab_log_record + geotab_device → AMA_Geotab_Exceso_Velocidad
geotab_device + geotab_device_status_info + geotab_log_record + geotab_trip → AMA_Geotab_GPS_Comunicacion
```

**Pending work**: `AMA_Geotab_Viajes` has `route_id`, `route_name`, `terminal_name`, and `scheduled_departure` hardcoded as NULL — awaiting AMA itinerary dataset. `is_central_departure` uses a `'CENTRAL_DUMMY'` placeholder. Speed violation threshold is hardcoded at 80 km/h.

**Synapse config per model**:
| Model | dist | index | Incremental window |
|---|---|---|---|
| `AMA_Geotab_Viajes` | `HASH(vehicle_id)` | CCI | last 1 day on `trip_start` |
| `AMA_Geotab_Salida_Vehiculos` | `ROUND_ROBIN`* | CCI | last 1 day on `[date]` |
| `AMA_Geotab_Exceso_Velocidad` | `HASH(device_id)` | CCI | last 1 day on `log_datetime` |
| `AMA_Geotab_GPS_Comunicacion` | `HASH(device_id)` | CCI | daily snapshot by `snapshot_date` |

*`AMA_Geotab_Salida_Vehiculos`: cambiar a `HASH(route_id)` cuando el dataset de itinerario de AMA esté disponible.

**GPS_Comunicacion es un snapshot diario**: grain `(device_id, snapshot_date)`. `pre_hook` elimina el snapshot de hoy antes de re-insertar — idempotente. Los campos rolling (viajes_ultimos_30_dias, etc.) siempre se calculan desde el histórico completo de la fuente.

**Incremental pattern** (todos los modelos): `pre_hook DELETE` del día anterior + `append`. Nunca MERGE. Idempotente ante re-ejecuciones del mismo día.

**Run AMA models**:
```bash
dbt run -s tag:dashboard_AMA

# Full refresh (reconstruye desde cero)
dbt run --full-refresh -s tag:dashboard_AMA
```

---

### Sonnell Pipeline

Migration of 3 stored procedure phases to dbt. All fact models use `materialized='incremental'`, `incremental_strategy='append'`, `dist='ROUND_ROBIN'`, `index='CLUSTERED COLUMNSTORE INDEX'`, and SCD Type 2 via a `CurrentVersion` flag.

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

```bash
# Run all Sonnell models (tagged)
dbt run -s tag:sonnell

# Full refresh
dbt run --full-refresh -s tag:sonnell

# Monthly reprocess (example: January 2026)
dbt run -s fct_sonnell_invoice_totals \
  --vars '{sonnell_invoice_reprocess: true, sonnell_invoice_reprocess_month: 1, sonnell_invoice_reprocess_year: 2026}'

# Reprocess via macro (alternative)
dbt run-operation sonnell_recalc_invoice_by_month --args '{"year": 2026, "month": 1}'

# Run/test a single model
dbt run --select <model_name>
dbt test --select <model_name>

# Compile without executing
dbt compile --select <model_name>
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
- **`stg_SonnellDailySummary` vs `stg_sonnell_daily_summary`**: two staging files exist for the same source (PascalCase legacy + snake_case new). The invoice model currently refs `stg_SonnellDailySummary`.
- **`stg_SonnellCheckpoints`**: similarly has a PascalCase legacy file alongside the snake_case version
- **Post-hooks are skipped on full refresh** — InvoiceID, Trips, Timestamps remain NULL until next incremental run
- **4% offset cap rule is informational only** — it does NOT modify invoice line Totals; it only writes to `SonnellOffsetApplied`
- **Columns that are never populated**: `ReenueMiles` (typo column), `ApprovalStatus`, `PaymentStatus` in the original SonnellInvoiceTotals table
- **`CurrentVersion` is `BIT` (0/1) not INT** — use `CAST(1 AS BIT)` in SELECT lists
