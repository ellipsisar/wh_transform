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
├── sonnell/               # Sonnell invoice pipeline (tag: sonnell)
│   ├── staging/           # Views: stg_sonnell_*
│   ├── fct_sonnell_subsystem_cost.sql
│   ├── fct_sonnell_subsystem_offset.sql
│   └── fct_sonnell_invoice_totals.sql
├── ama/                   # AMA fleet dashboard views (tag: dashboard_AMA)
│   ├── AMA_Geotab_Viajes.sql
│   ├── AMA_Geotab_Salida_Vehiculos.sql
│   ├── AMA_Geotab_Exceso_Velocidad.sql
│   ├── AMA_Geotab_GPS_Comunicacion.sql
│   ├── AMA_Geotab_Terminales.sql
│   └── intermediate/
│       ├── stg_geotab__zone.sql        # Ephemeral: parses zone_types_json
│       └── stg_geotab_device.sql       # Ephemeral: normalizes geotab_device
├── hms/                   # HMS ferry pipeline (tag: hms)
│   ├── staging/
│   │   └── stg_hms__trips.sql          # Ephemeral: cast + clean + dedup
│   ├── fct_hms_monthly_trips.sql       # table alias: HMS_MonthlyDataTrip
│   └── fct_hms_ntd_data.sql            # incremental alias: HmsNtdData
├── monitoring/            # Pipeline health monitoring (tag: monitoring)
│   ├── staging/
│   │   ├── stg_control_status.sql      # Ephemeral: parses control_raw_file_status
│   │   └── stg_entity_config.sql       # Ephemeral: inline expected-frequency config
│   ├── intermediate/
│   │   ├── int_daily_aggregates.sql    # Ephemeral: counts/sums by (date, entity, domain)
│   │   ├── int_baseline_7d.sql         # Ephemeral: 7-day avg, stddev, z-score
│   │   ├── int_latest_errors.sql       # Ephemeral: last error per (date, entity, domain)
│   │   └── int_health_metrics.sql      # Ephemeral: health_score, SLA flags
│   └── marts/
│       └── fct_pipeline_health_daily.sql  # Incremental (delete+insert, 7-day lookback)
├── ati_datawarehouse/     # Korbato ATI DWH migration (tag: ati_datawarehouse)
│   ├── deduplicated/      # Ephemeral dedup CTEs per source entity
│   ├── intermediate/      # Intermediate transformation models
│   ├── fleet.sql / trip.sql / vehicle_day.sql / visit.sql / pattern.sql / pattern_stop.sql / Trip_Sch_Match.sql
├── sources/               # Source YAML definitions
├── fct_operator_scd.sql   # SCD Type 2 for operators (root level, schema.yml)
├── fct_route_scd.sql      # SCD Type 2 for routes (root level, schema.yml)
├── dimOperators.sql       # Legacy (root level, no schema file)
├── dimRoutes.sql          # Legacy (root level, no schema file)
├── FrequencyDailySummary.sql  # Legacy (root level)
└── temp/                  # Ad-hoc/exploratory models
macros/
├── sonnell_recalc_subsystem_costs.sql
├── sonnell_recalc_offsets_by_month.sql
├── sonnell_recalc_invoice_by_month.sql
└── ama_backfill_gps_comunicacion.sql   # Backfills GPS_Comunicacion for N past days
```

### AMA/Geotab Pipeline

Dashboard views for AMA fleet monitoring, tagged `dashboard_AMA`. Source schema: `staging` (source name: `geotab`).

**Geotab source tables registered** (`models/sources/geotab.yml`): `geotab_trip`, `geotab_device`, `geotab_device_status_info`, `geotab_log_record`, `geotab_exception_event`, `geotab_zone`, `geotab_zone_type`, `geotab_rule`, `geotab_route`, `geotab_route_plan_item`, `geotab_fault_data`, `geotab_group`, `geotab_status_data`, `geotab_planned_vs_actual`

**AMA Itinerario** (`models/sources/ama.yml`): source name `ama`, table `AMA_Itinerario`, schema `dbo`. Used via `source('ama', 'AMA_Itinerario')`.

**Model dependencies**:
```
geotab_trip + geotab_device + AMA_Itinerario → AMA_Geotab_Viajes → AMA_Geotab_Salida_Vehiculos
geotab_log_record + geotab_device → AMA_Geotab_Exceso_Velocidad
geotab_device + geotab_device_status_info + geotab_log_record + geotab_trip → AMA_Geotab_GPS_Comunicacion
geotab_exception_event + geotab_rule + geotab_zone + geotab_zone_type + geotab_route_plan_item + geotab_route + geotab_device + geotab_trip + geotab_planned_vs_actual
  → AMA_Geotab_Terminales (zone parsing inlined as CTEs)
stg_geotab__zone (ephemeral, models/ama/intermediate/) — standalone ephemeral; not currently ref()d by any model
stg_geotab_device (ephemeral, models/ama/intermediate/) — standalone ephemeral; not currently ref()d by any model
```

**Pending work**: `is_central_departure` usa `'SGDO'` como placeholder — confirmar con AMA el código exacto de la terminal Central. Speed violation threshold is hardcoded at 80 km/h.

**Itinerario AMA** (`dbo.AMA_Itinerario`, source name `ama`): disponible desde 2026-03-26. Join: `ama_itinerario.tren = geotab_device.device_name`. Se filtra `orden_parada = 0` para obtener la terminal y hora de salida programada. Se matchea por día de semana (`servicio`: LUNES-VIERNES / SABADO / DOMINGO). Para viajes con múltiples turnos posibles, se elige el turno cuya hora programada sea más cercana a la salida real. **Esta integración está implementada en `AMA_Geotab_Viajes`** (CTEs `itinerario_salida` + `trip_itinerario_candidatos` + `trip_itinerario`). El campo `_md_snapshot_date` en `AMA_Itinerario` identifica el snapshot más reciente.

**Synapse config per model**:
| Model | dist | index | Incremental window | Tags |
|---|---|---|---|---|
| `AMA_Geotab_Viajes` | `HASH(vehicle_id)` | CCI | last 1 day on `trip_start` | `dashboard_AMA` |
| `AMA_Geotab_Salida_Vehiculos` | `HASH(route_id)` | CCI | last 1 day on `[date]` | `dashboard_AMA` |
| `AMA_Geotab_Exceso_Velocidad` | `HASH(device_id)` | CCI | last 1 day on `log_datetime` | `dashboard_AMA` |
| `AMA_Geotab_GPS_Comunicacion` | `HASH(device_id)` | CCI | daily snapshot by `snapshot_date` | `dashboard_AMA` |
| `AMA_Geotab_Terminales` | `HASH(zone_id)` | CCI | last 1 day on `event_date` | `dashboard_AMA`, `terminales_AMA` |
| `stg_geotab__zone` (ephemeral) | — | — | — | `dashboard_AMA`, `terminales_AMA` |
| `stg_geotab_device` (ephemeral) | — | — | — | `dashboard_AMA` |

**GPS_Comunicacion es un snapshot diario**: grain `(device_id, snapshot_date)`. `pre_hook` elimina el snapshot de hoy antes de re-insertar — idempotente. Los campos rolling (viajes_ultimos_30_dias, etc.) siempre se calculan desde el histórico completo de la fuente.

**AMA_Geotab_Terminales**: tracks dwell events at terminals (exception events). Resolution chain: `geotab_exception_event` → `geotab_rule` → zone → `geotab_route_plan_item` → `geotab_route`. Joins next trip start within 4 hours of terminal exit. Zone type parsing (`pos` + `val_pos` + `stg_geotab__zone` CTEs) is inlined directly in the model — parses `zone_types_json` via `CHARINDEX`/`SUBSTRING` (two formats: built-in `["ZoneTypeCustomerId"]` vs custom `[{"id": "b22"}]`). **Itinerario fields (`scheduled_departure`, `delay_minutes`, `departure_status`) are `CAST(NULL ...)` placeholders** — itinerario integration is not yet done for this model (unlike `AMA_Geotab_Viajes` where it is fully implemented).

**Incremental pattern** (todos los modelos): `pre_hook DELETE` del día anterior + `append`. Nunca MERGE. Idempotente ante re-ejecuciones del mismo día.

**Run AMA models**:
```bash
dbt run --profiles-dir . -s tag:dashboard_AMA

# Full refresh (reconstruye desde cero)
dbt run --profiles-dir . --full-refresh -s tag:dashboard_AMA
```

---

### HMS Pipeline

Migration of two HMS ferry stored procedures to dbt. Source: `raw.hms_trips` (source name `hms`, registered in `models/sources/hms.yml`). Output tables registered as source `hms_output` (schema `dbo`) for post-hook references.

**Models**:
| Model | Alias | Materialization | Notes |
|---|---|---|---|
| `stg_hms__trips` | — | ephemeral | Cast + clean + dedup (rank=1 per date+vessel+route+scheduled_departure) |
| `fct_hms_monthly_trips` | `HMS_MonthlyDataTrip` | table (`HASH(Id)`, CCI) | Full reload every run; synthetic `Id` via `ROW_NUMBER()` |
| `fct_hms_ntd_data` | `HmsNtdData` | incremental (append, `HASH(Date)`, CCI) | Daily: current month window; reprocess via vars |

**HMS NTD reprocess vars**:
```bash
dbt run --profiles-dir . -s fct_hms_ntd_data \
  --vars '{hms_ntd_reprocess: true, hms_ntd_reprocess_month: 1, hms_ntd_reprocess_year: 2026}'
```

**Key design**: `IsOutbound = 1` when origin is NOT vieques/culebra. `IsMissed = 1` when `Trip_Status` is NULL, empty, or `'MISSED TRIP'`. `DayOfWeek` uses formula `((WEEKDAY+5)%7)` (Monday=0 … Sunday=6).

---

### Monitoring Pipeline

Pipeline health monitoring model. Source: `ati_lakehouse.dbo.control_raw_file_status` (source name `ati_lakehouse`, registered in `_fct_monitoring__models.yml`). All staging and intermediate models are ephemeral; only `fct_pipeline_health_daily` is materialized.

**Architecture**: all CTEs compile inline into a single `fct_pipeline_health_daily` query.

```
control_raw_file_status
  → stg_control_status (ephemeral, parses filename → entity_name + domain)
  → stg_entity_config  (ephemeral, inline expected_frequency_hrs per domain)
  → int_daily_aggregates → int_baseline_7d → int_latest_errors → int_health_metrics
  → fct_pipeline_health_daily (incremental, delete+insert, 7-day lookback)
```

**Grain**: `(event_date, entity_name, domain)`. Unique key enforced via `dbt_utils.unique_combination_of_columns`.

**Domain classification** (from filename prefix in `stg_control_status`):
| Prefix | Domain |
|---|---|
| `geotab_*` / `nb_geotab_*` | `geotab` |
| `tdev_*` | `transdev` |
| `sonnell_*` | `sonnell` |
| `hms_*` / `HMS_*` | `hms` |
| `gtfs_*` / `GTFS_*` | `gtfs` |
| `fleet*`, `pattern*`, `trip*`, `visit*`, `vehicle_day*` | `ama_legacy` |
| other | `other` |

**Health score** (0–100): success_rate (40 pts) + volume_consistency (30 pts) + no_anomalies (30 pts). Status thresholds: `HEALTHY ≥ 80`, `WARNING ≥ 50`, `CRITICAL < 50`, `NO_DATA` if no executions.

**Incremental window**: 7-day lookback (`DATEADD(DAY, -7, GETDATE())`) to correctly recalculate 7-day baselines on re-run.

**`fct_pipeline_health_daily` uses `dbt_utils` package** — `packages.yml` must declare it.

```bash
dbt run --profiles-dir . -s tag:monitoring
dbt run --profiles-dir . --full-refresh -s fct_pipeline_health_daily
```

---

### Sonnell Pipeline

Migration of 3 stored procedure phases to dbt. All fact models use `materialized='incremental'`, `incremental_strategy='append'`, `dist='ROUND_ROBIN'`, `index='CLUSTERED COLUMNSTORE INDEX'`, and SCD Type 2 via a `CurrentVersion` flag.

**Sources** (dbo schema): `SonnellDailySummary`, `SonnellRates`, `SonnellCheckpoints`, `SonnellParameters`, `SonnellOffsetApplied`

**Raw sources** (raw schema, source name `external` in `models/sources/external.yml`): `sonnell_trip`, `sonnell_subsystem`, `Sonnell_checkpoin` — external tables from data lake (Parquet)

**Post-hook target tables also registered as sources** in `sonnell.yml`: `SonnellSubsystemCost`, `SonnellInvoiceTotals`, `SonnellOffsetApplied` — registered so post-hooks can reference them as `source()` relations when needed.

**Data flow**:
```
SonnellDailySummary → stg_sonnell_daily_summary ──┐
SonnellRates        → stg_sonnell_rates ───────────┼──► fct_sonnell_subsystem_cost ──┐
SonnellCheckpoints  → stg_sonnell_checkpoints ─────┤                                 ├──► fct_sonnell_invoice_totals
SonnellParameters   → stg_sonnell_parameters ───────┴──► fct_sonnell_subsystem_offset ┘
sonnell_trip (raw)  → stg_sonnell_trip              # staging view; not yet consumed by a fact model
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

All commands require `--profiles-dir .` unless `profiles.yml` is symlinked to `~/.dbt/`.

```bash
# --- Sonnell ---
dbt run --profiles-dir . -s tag:sonnell
dbt run --profiles-dir . --full-refresh -s tag:sonnell
# Monthly reprocess (example: January 2026)
dbt run --profiles-dir . -s fct_sonnell_invoice_totals \
  --vars '{sonnell_invoice_reprocess: true, sonnell_invoice_reprocess_month: 1, sonnell_invoice_reprocess_year: 2026}'
dbt run-operation --profiles-dir . sonnell_recalc_invoice_by_month --args '{"year": 2026, "month": 1}'

# --- AMA ---
dbt run --profiles-dir . -s tag:dashboard_AMA
dbt run --profiles-dir . --full-refresh -s tag:dashboard_AMA
# GPS backfill (last N days)
dbt run-operation --profiles-dir . ama_backfill_gps_comunicacion --args '{"days": 30}'

# --- HMS ---
dbt run --profiles-dir . -s tag:hms
# NTD monthly reprocess
dbt run --profiles-dir . -s fct_hms_ntd_data \
  --vars '{hms_ntd_reprocess: true, hms_ntd_reprocess_month: 1, hms_ntd_reprocess_year: 2026}'

# --- Monitoring ---
dbt run --profiles-dir . -s tag:monitoring
dbt run --profiles-dir . --full-refresh -s fct_pipeline_health_daily

# --- Generic ---
dbt run --profiles-dir . --select <model_name>
dbt test --profiles-dir . --select <model_name>
dbt compile --profiles-dir . --select <model_name>
```

---

## Coding Rules

### Naming Conventions
- Staging: `stg_<source>__<entity>` (double underscore)
- Facts: `fct_<entity>`
- Dimensions: `dim_<entity>`
- Sources YAML: define both `sonnell` (dbo) and `external` (raw schema, in `external.yml`) separately; AMA itinerario uses a separate `ama` source (`models/sources/ama.yml`); other registered sources (`korbato`, `gtfs`, `tdev`/`tdev_dev`) are in `models/sources/` but not yet wired to any active model
- AMA models have no `schema.yml` — model-level tests and descriptions are absent for the AMA pipeline

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
        +materialized: view     # stg_sonnell_* views
    hms:
      staging:
        +materialized: ephemeral  # stg_hms__trips
      # fct_hms_monthly_trips: table (inline config)
      # fct_hms_ntd_data: incremental (inline config)
    monitoring:
      staging:
        +materialized: ephemeral
      intermediate:
        +materialized: ephemeral
      marts:
        +materialized: incremental  # fct_pipeline_health_daily
    # All other fact/dim models set materialized inline via config()
```

---

## Known Gotchas

- **`profiles.yml` is at repo root**, not `~/.dbt/` — pass `--profiles-dir .` or symlink if needed
- **Post-hooks are skipped on full refresh** — InvoiceID, Trips, Timestamps remain NULL until next incremental run (Sonnell)
- **4% offset cap rule is informational only** — it does NOT modify invoice line Totals; it only writes to `SonnellOffsetApplied`
- **Columns that are never populated**: `ReenueMiles` (typo column), `ApprovalStatus`, `PaymentStatus` in the original SonnellInvoiceTotals table
- **`CurrentVersion` is `BIT` (0/1) not INT** — use `CAST(1 AS BIT)` in SELECT lists
- **`fct_pipeline_health_daily` uses `dbt_utils`** — `packages.yml` must declare `dbt-labs/dbt_utils`; tests like `dbt_utils.unique_combination_of_columns` and `dbt_utils.expression_is_true` will fail without it
- **Monitoring baseline requires 3+ days of history** — `baseline_7d_avg` / `baseline_7d_stddev` return NULL when fewer than 3 days exist for an entity
- **HMS `fct_hms_monthly_trips` is a full table rebuild** — it drops and recreates on every `dbt run`; no incremental logic, no `--full-refresh` needed
- **`ati_lakehouse` is a separate database** — the monitoring source lives in `ati_lakehouse.dbo.control_raw_file_status`, not in `ati_datawarehouse`; ensure the Synapse user has cross-database read access
- **AMA_Geotab_Terminales itinerario fields are NULL placeholders** — `scheduled_departure`, `delay_minutes`, `departure_status` are `CAST(NULL ...)` pending itinerario integration (unlike `AMA_Geotab_Viajes` where it is fully implemented)
