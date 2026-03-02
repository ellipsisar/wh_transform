# Sonnell dbt Models

Migration of the Sonnell invoice pipeline from SQL Server stored procedures to dbt incremental models.

## Overview

The Sonnell system calculates monthly invoices for transit subsystems (MB, TC, MU) based on revenue metrics (miles, hours, trips) and on-time performance (OTP) checkpoints. The pipeline produces:

- **Subsystem costs** per service date (revenue miles/hours x effective rates)
- **OTP offsets** per month (incentive/penalty based on checkpoint compliance)
- **Invoice totals** per month (regular lines + offset lines, with a 4% cap audit)

## Architecture

### Source Tables (dbo schema)

| Source | Description |
|---|---|
| `SonnellDailySummary` | Daily revenue metrics (RevenueMeters, RevenueSeconds, TripCount) by route and subsystem |
| `SonnellRates` | Rate cards with effective date ranges (RateMiles, RateHours, IsActive) |
| `SonnellCheckpoints` | Checkpoint-level OTP compliance data (Compliant = 't'/'f') |
| `SonnellParameters` | OTP threshold ranges and incentive/offset rates per subsystem |
| `SonnellOffsetApplied` | Audit table for the 4% offset cap rule (written to by invoice post_hooks) |

### Model Layers

```
Sources (dbo)                  Staging (views)                 Fact (incremental)
─────────────                  ───────────────                 ──────────────────
SonnellDailySummary ──────► stg_sonnell_daily_summary ──┐
                                                        ├──► fct_sonnell_subsystem_cost ──┐
SonnellRates ─────────────► stg_sonnell_rates ──────────┘                                 │
                                                                                          ├──► fct_sonnell_invoice_totals
SonnellCheckpoints ───────► stg_sonnell_checkpoints ──┐                                   │
                                                      ├──► fct_sonnell_subsystem_offset ──┘
SonnellParameters ────────► stg_sonnell_parameters ───┘
```

The invoice model also reads directly from `SonnellDailySummary` and `SonnellRates` (via `source()`) for the MU block, which recalculates amounts from raw data with `ROUND(SUM(RevenueMeters)/1609.34, 2)` to match SP rounding exactly.

### Staging Models

Views (`materialized='view'`) that normalize raw source columns without business logic:

| Model | Transforms |
|---|---|
| `stg_sonnell_daily_summary` | Renames `Subsystem`->`GroupId`, `TripCount`->`NumTrips`; converts `RevenueSeconds`->`RevenueHours` (/ 3600) and `RevenueMeters`->`RevenueMiles` (/ 1609.34) |
| `stg_sonnell_rates` | Pass-through |
| `stg_sonnell_checkpoints` | Pass-through |
| `stg_sonnell_parameters` | Pass-through |

### Fact Models

All three fact models share the same architecture:

- `materialized='incremental'` with `incremental_strategy='append'`
- SCD Type 2 versioning via `CurrentVersion` flag (1 = active, 0 = superseded)
- `pre_hook` for version management (mark old / delete for reprocess)
- `post_hook` array for computed columns and cross-table updates
- 3 execution modes: full refresh, daily incremental, monthly reprocess

## SP-to-dbt Mapping

### Fase I: Subsystem Costs

| SP | dbt Equivalent |
|---|---|
| `Sonnell_UpdateSubsystemCostsVersions` (daily) | `fct_sonnell_subsystem_cost` (default incremental run) |
| `Sonnell_CalculateSubsystemCostsByMonth` (reprocess) | `sonnell_recalc_subsystem_costs` macro or model with `sonnell_reprocess` vars |

### Fase II: OTP Offsets

| SP | dbt Equivalent |
|---|---|
| `Sonnell_UpdateOffsetVersions` (daily) | `fct_sonnell_subsystem_offset` (default incremental run) |
| `Sonnell_CalculateOffsetsByMonth` + `Sonnell_FetchOffsetValuesByMonth` (reprocess) | `sonnell_recalc_offsets_by_month` macro or model with `sonnell_offset_reprocess` vars |

### Fase III: Invoice Totals

| SP | dbt Equivalent |
|---|---|
| `Sonnell_UpdateInvoiceVersions` (daily, SP1) | `fct_sonnell_invoice_totals` (default incremental run) |
| `Sonnell_CalculateInvoiceTotalsStatColumns` (daily stats, SP2) | `fct_sonnell_invoice_totals` post_hooks (Trips, RevenueMiles/Hours, Checkpoints, Timestamps) |
| `Sonnell_CalculateInvoiceTotalsByMonth` (reprocess, SP3) | `sonnell_recalc_invoice_by_month` macro or model with `sonnell_invoice_reprocess` vars |

## Execution Modes

### 1. Full Refresh

Rebuilds all tables from scratch. Used for initial deployment or recovery.

```bash
dbt run --full-refresh -s tag:sonnell
```

- No date or version filters on source data
- `CurrentVersion` determined by `MAX(Version) OVER (PARTITION BY ...)` 
- Post_hooks are skipped (InvoiceID, stats, timestamps remain NULL until next incremental run)

### 2. Daily Incremental (default)

Standard daily execution. Processes new data for closed months only.

```bash
dbt run -s tag:sonnell
```

**Execution order** (dbt DAG ensures dependency order):

1. `fct_sonnell_subsystem_cost` -- inserts new cost rows for any new DailySummary versions
2. `fct_sonnell_subsystem_offset` -- inserts new offset aggregations for any new Checkpoint versions
3. `fct_sonnell_invoice_totals` -- inserts new invoice lines for closed months

**Per-model flow:**

| Phase | What happens |
|---|---|
| **pre_hook** | UPDATE `CurrentVersion = 0` on existing rows where a newer Version exists (with NOT EXISTS idempotence guard) |
| **SELECT** | Insert new rows with `CurrentVersion = 1` for data not yet in the target (NOT EXISTS dedup on the unique key) |
| **post_hooks** | Populate computed columns: InvoiceID, Trips, RevenueMiles/Hours, Checkpoints, Timestamps. Propagate InvoiceID to offset and cost tables. Insert 4% audit data into SonnellOffsetApplied |

**Key filters in daily mode:**

- `CurrentVersion = 1` on all source table joins
- Closed-month filter: `month < current month` (only processes completed months)
- `IsActive` on `SonnellRates`: NOT applied for MU (commented out in SP1, replicated)
- Dedup: `NOT EXISTS (Year, Month, Subsystem, Type, Version)` against target

### 3. Monthly Reprocess

Reprocesses a specific month. Two invocation methods:

**Method A: via dbt vars**

```bash
dbt run -s fct_sonnell_invoice_totals \
  --vars '{
    sonnell_invoice_reprocess: true,
    sonnell_invoice_reprocess_month: 1,
    sonnell_invoice_reprocess_year: 2026
  }'
```

**Method B: via run-operation macro**

```bash
dbt run-operation sonnell_recalc_invoice_by_month --args '{"year": 2026, "month": 1}'
```

| Aspect | Daily | Reprocess |
|---|---|---|
| Version handling | pre_hook marks old with `CurrentVersion = 0` | pre_hook DELETEs rows for the month |
| CurrentVersion on sources | `= 1` (only current data) | No filter (all versions) |
| IsActive on SonnellRates (MU) | Not applied | `IsActive = 1` |
| InvoiceID generation | One `NEWID()` per `(Year, Month)` via CTE | Single `NEWID()` for entire month via `CROSS JOIN` |
| InvoiceID propagation | JOIN by `(Year, Month, Subsystem)` | `CROSS JOIN` to all rows in the month |
| SonnellOffsetApplied | INSERT with NOT EXISTS guard | DELETE first, then INSERT |
| Stats (Trips, Revenue, Checkpoints) | `CurrentVersion = 1` on cost/offset | No `CurrentVersion` filter |
| Scope | All closed months with new data | Single specified month |

### Available var names by model

| Model | Reprocess flag | Month var | Year var |
|---|---|---|---|
| `fct_sonnell_subsystem_cost` | `sonnell_reprocess` | `sonnell_reprocess_month` | `sonnell_reprocess_year` |
| `fct_sonnell_subsystem_offset` | `sonnell_offset_reprocess` | `sonnell_offset_reprocess_month` | `sonnell_offset_reprocess_year` |
| `fct_sonnell_invoice_totals` | `sonnell_invoice_reprocess` | `sonnell_invoice_reprocess_month` | `sonnell_invoice_reprocess_year` |

## Invoice Model Details

### Grain

`(Year, Month, Subsystem, Type, Version)`

- Versioning operates at the `(Year, Month)` level: when ANY new version arrives for a period, ALL existing invoice lines for that period are marked `CurrentVersion = 0`

### 3 UNION ALL Blocks

Each execution mode produces the same 3 invoice line types:

**Block 1 -- Regular MB/TC** (one row per subsystem per month):

- Source: `fct_sonnell_subsystem_offset` INNER JOIN `fct_sonnell_subsystem_cost` (double-join pattern)
- Total = `TotalSubsystemCost` (from offset table)
- Description format: `"MB JanuaryAll2026 (123.45 miles @ $1.23; 456.78 hours @ $4.56)"`

**Block 2 -- Regular MU** (one row per route: '20', '30'):

- Source: offset + cost for Version, then recalculates from raw `SonnellDailySummary x SonnellRates`
- `Subsystem = 'MU - ' + RouteId` (e.g. `'MU - 20'`)
- Total = `RevenueMilesAmount + RevenueHoursAmount` per route
- Description format varies by route (E20 vs E30 vs XXX fallback)

**Block 3 -- Offset** (one row per subsystem per month):

- Source: `fct_sonnell_subsystem_offset` directly
- Total = `TotalOffset`
- `OffsetPercentage = (TotalOffset / TotalSubsystemCost) * 100`
- Description format: `"MB January 2026 Offset (Checkpoints: 100; OTP: 95.50% ==> Effective rate: 2.00%"`

### Post-hooks (10 sequential operations)

Executed only in incremental mode (daily or reprocess):

| # | Operation | Description |
|---|---|---|
| 1 | InvoiceID generation | One `NEWID()` per `(Year, Month)` (daily) or single `NEWID()` for the month (reprocess) |
| 2 | SonnellOffsetApplied DELETE | Reprocess only: clears old audit data for the month |
| 3 | SonnellOffsetApplied INSERT | 4% rule: computes `FourPercentRegularTotal = SUM(Regular) * 0.04`, caps offset if exceeds 4%. NOT EXISTS guard for idempotence |
| 4 | InvoiceID -> offset table | Propagates InvoiceID to `fct_sonnell_subsystem_offset` |
| 5 | InvoiceID -> cost table | Propagates InvoiceID to `fct_sonnell_subsystem_cost` |
| 6 | Trips (MB/TC) | `SUM(NumTrips)` from cost grouped by `GroupId` |
| 7 | Trips (MU routes) | `SUM(NumTrips)` from cost grouped by `RouteId` |
| 8 | RevenueMiles + RevenueHours | MB/TC only (MU is a no-op due to Subsystem mismatch, replicating SP behavior) |
| 9 | Checkpoints | `CompliantCheckpoints` and `NonCompliantCheckpoints` from offset, for `Type = 'Offset'` rows |
| 10 | Timestamps | `CreatedAt = GETDATE(), UpdatedAt = GETDATE()` |

### 4% Offset Cap Rule

The 4% rule is informational only -- it does NOT modify invoice line Totals. It persists audit data in `SonnellOffsetApplied`:

1. Compute `TotalInvoiceRegular = SUM(Total) WHERE Type = 'Regular'` per (Year, Month, Subsystem)
2. Compute `FourPercentRegularTotal = TotalInvoiceRegular * 0.04`
3. Compute `TotalCalculatedOffset = SUM(Total) WHERE Type = 'Offset'`
4. If offset > 4% of regular: `ApplySuggestedOffset = 1`, cap at `FourPercentRegularTotal`
5. If offset is NULL: `ApplySuggestedOffset = 0`, `TotalInvoiceSuggestedOffset = NULL`
6. Otherwise: `ApplySuggestedOffset = 0`, use actual offset

### Columns Never Populated

These columns exist in the original table but are never set by any SP or dbt model:

- `ReenueMiles` (typo column in original table)
- `ApprovalStatus`
- `PaymentStatus`

## File Structure

```
models/sonnell/
├── README.md                              # This file
├── schema.yml                             # Column docs and tests for all models
├── staging/
│   ├── stg_sonnell_daily_summary.sql      # View: normalize DailySummary
│   ├── stg_sonnell_rates.sql              # View: pass-through Rates
│   ├── stg_sonnell_checkpoints.sql        # View: pass-through Checkpoints
│   └── stg_sonnell_parameters.sql         # View: pass-through Parameters
├── fct_sonnell_subsystem_cost.sql         # Incremental: daily costs (Fase I)
├── fct_sonnell_subsystem_offset.sql       # Incremental: OTP offsets (Fase II)
└── fct_sonnell_invoice_totals.sql         # Incremental: invoice totals (Fase III)

models/sources/
└── sonnell.yml                            # Source definitions (dbo schema)

macros/
├── sonnell_recalc_subsystem_costs.sql     # run-operation: reprocess costs
├── sonnell_recalc_offsets_by_month.sql    # run-operation: reprocess offsets
└── sonnell_recalc_invoice_by_month.sql    # run-operation: reprocess invoices
```

## Target Tables

| dbt Model | Target Table (alias) | Original SP Target |
|---|---|---|
| `fct_sonnell_subsystem_cost` | `dbt_SonnellSubsystemCost` | `dbo.SonnellSubsystemCost` |
| `fct_sonnell_subsystem_offset` | `dbt_SonnellSubsystemOffset` | `dbo.SonnellSubsystemOffset` |
| `fct_sonnell_invoice_totals` | `dbt_SonnellInvoiceTotals` | `dbo.SonnellInvoiceTotals` |

All tables use `ROUND_ROBIN` distribution and `CLUSTERED COLUMNSTORE INDEX`.

## Design Decisions

1. **`append` strategy over `merge`**: The SPs use INSERT + UPDATE patterns (SCD Type 2), not MERGE. The `append` strategy with `pre_hook` UPDATEs replicates this faithfully while keeping the model idempotent via NOT EXISTS guards.

2. **Post_hooks for cross-table writes**: InvoiceID propagation and SonnellOffsetApplied writes can't be expressed in a pure SELECT. Post_hooks execute sequentially after the INSERT, matching the SP execution order.

3. **MU recalculation from raw data**: The MU block uses `source('sonnell', 'SonnellDailySummary')` instead of `ref('fct_sonnell_subsystem_cost')` because the SP recalculates with `ROUND(SUM(RevenueMeters)/1609.34, 2)`. Using the cost table would produce different rounding.

4. **MU route stats as no-ops**: SP2 attempts to update RevenueMiles/RevenueHours for MU routes, but the `GroupId = Subsystem` join fails ('MU' != 'MU - 20'). This is a known SP behavior that we replicate by omitting those UPDATEs.

5. **`IsActive` filter difference**: SP1 (daily) has `IsActive` commented out on SonnellRates for MU. SP3 (reprocess) has `IsActive = 1`. This difference is preserved in the dbt model.

6. **Separate macros for reprocess**: `run-operation` macros provide a CLI-friendly interface for ad-hoc reprocessing without requiring `--vars` flags. They use `api.Relation.create()` since `ref()` and `source()` are unavailable in run-operation context.
