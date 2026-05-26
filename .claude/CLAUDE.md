# wh_transform — Project Instructions

> ⚠️ Este archivo es la fuente de verdad del proyecto para Claude Code.
> Los agentes lo leen al iniciar cada tarea. Mantenerlo actualizado es crítico.
> Ver sección "Cómo actualizar este archivo" al final.

---

## Proyecto

**Nombre**: wh_transform
**Tipo**: Proyecto DBT puro — capa de transformación de data warehouse
**Repositorio**: https://github.com/ellipsisar/wh_transform
**Rama principal**: `main`
**Adapter DBT**: `synapse` (T-SQL dialect, Azure Synapse Analytics Dedicated SQL Pool)
**Data warehouse destino**: `ati_datawarehouse` en `ati-cdw-synapse.sql.azuresynapse.net`
**Herramienta de orquestación**: 📌 COMPLETAR (dbt Cloud | Airflow | Azure DevOps — no identificado en archivos)

---

## Agent Team

| Agente | Archivo | Modelo | Cuándo usar |
|--------|---------|--------|-------------|
| `wh-orchestrator` | agents/wh-orchestrator.md | Opus 4 | Requerimientos multi-agente o poco claros |
| `analytics-engineer` | agents/analytics-engineer.md | Sonnet 4 | Modelos DBT, SQL, staging/marts, macros, docs |
| `data-quality-qa` | agents/data-quality-qa.md | Sonnet 4 | Code review, tests, DQ checks, QA reports |
| `devops-cloud-engineer` | agents/devops-cloud-engineer.md | Sonnet 4 | CI/CD, Git, Slim CI, automatización |

---

## Routing Rules

### Delegar automáticamente a `analytics-engineer` cuando:
- Crear o modificar cualquier archivo `.sql` en `models/`
- Agregar o modificar `schema.yml`, `sources.yml`
- Crear macros en `macros/`
- Crear snapshots en `snapshots/`
- Preguntas sobre lógica de negocio en SQL o estructura de modelos

### Delegar automáticamente a `data-quality-qa` cuando:
- Code review de modelos nuevos o modificados
- Agregar tests a schema.yml
- Detectar anomalías o problemas de calidad en datos
- Revisar un PR antes de merge
- Auditar modelos existentes sin tests

### Delegar automáticamente a `devops-cloud-engineer` cuando:
- Configurar o modificar pipelines CI/CD
- Configurar DBT Slim CI (state-based runs)
- Git flow, branch strategy, protección de ramas
- Configurar variables de entorno o secretos para DBT
- Automatizar ejecución de `dbt run` / `dbt test`

### Usar `wh-orchestrator` cuando:
- El requerimiento involucra 2+ agentes
- No está claro qué agente necesita la tarea
- Feature de punta a punta (nueva fuente → staging → marts → CI → tests)

---

## Estructura del Proyecto

```
wh_transform/
├── .claude/
│   ├── CLAUDE.md              ← Este archivo
│   ├── agents/
│   │   ├── wh-orchestrator.md
│   │   ├── analytics-engineer.md
│   │   ├── data-quality-qa.md
│   │   └── devops-cloud-engineer.md
│   ├── skills/
│   │   ├── analytics-engineer/SKILL.md
│   │   ├── data-quality-qa-agent/SKILL.md
│   │   └── devops-cloud-engineer/SKILL.md
│   └── memory/
├── models/
│   ├── sonnell/                        ← Pipeline de facturación Sonnell (tag: sonnell)
│   │   ├── staging/                    ← Views: stg_sonnell_*
│   │   │   ├── stg_sonnell_daily_summary.sql
│   │   │   ├── stg_sonnell_rates.sql
│   │   │   ├── stg_sonnell_checkpoints.sql
│   │   │   ├── stg_sonnell_parameters.sql
│   │   │   └── stg_sonnell_trip.sql
│   │   ├── fct_sonnell_subsystem_cost.sql
│   │   ├── fct_sonnell_subsystem_offset.sql
│   │   ├── fct_sonnell_invoice_totals.sql
│   │   └── schema.yml
│   ├── ama/                            ← Vistas dashboard AMA fleet (tag: dashboard_AMA)
│   │   ├── intermediate/
│   │   │   ├── stg_geotab__zone.sql    ← Ephemeral: parseo zone_types_json
│   │   │   └── stg_geotab_device.sql   ← Ephemeral: staging de dispositivos Geotab
│   │   ├── AMA_Geotab_Viajes.sql
│   │   ├── AMA_Geotab_Salida_Vehiculos.sql
│   │   ├── AMA_Geotab_Exceso_Velocidad.sql
│   │   ├── AMA_Geotab_GPS_Comunicacion.sql
│   │   └── AMA_Geotab_Terminales.sql
│   ├── hms/                            ← Pipeline ferry HMS (tag: hms)
│   │   ├── staging/stg_hms__trips.sql  ← Ephemeral: cast + clean + dedup
│   │   ├── fct_hms_monthly_trips.sql   ← table, alias HMS_MonthlyDataTrip
│   │   ├── fct_hms_ntd_data.sql        ← incremental, alias HmsNtdData
│   │   └── schema.yml
│   ├── monitoring/                     ← Monitoreo de salud de pipelines (tag: monitoring)
│   │   ├── staging/                    ← Ephemeral: stg_control_status, stg_entity_config
│   │   ├── intermediate/               ← Ephemeral: int_daily_aggregates, int_baseline_7d, int_latest_errors, int_health_metrics
│   │   └── marts/fct_pipeline_health_daily.sql  ← incremental delete+insert, ROUND_ROBIN
│   ├── ati_datawarehouse/              ← Migración Korbato ATI DWH (tag: ati_datawarehouse)
│   │   ├── deduplicated/               ← Ephemeral dedup staging por entidad
│   │   ├── intermediate/               ← Intermediate por entidad
│   │   └── fleet.sql / trip.sql / vehicle_day.sql / visit.sql / pattern.sql / pattern_stop.sql / Trip_Sch_Match.sql
│   ├── sources/                        ← Definiciones YAML de fuentes
│   │   ├── sonnell.yml
│   │   ├── geotab.yml
│   │   ├── ama.yml
│   │   ├── hms.yml
│   │   ├── gtfs.yml
│   │   ├── korbato.yml
│   │   ├── tdev.yml
│   │   └── external.yml
│   ├── fct_operator_scd.sql            ← SCD Type 2 para operadores (schema.yml en raíz)
│   ├── fct_route_scd.sql               ← SCD Type 2 para rutas (schema.yml en raíz)
│   ├── dimOperators.sql                ← Modelos legacy (sin schema.yml)
│   ├── dimRoutes.sql
│   ├── FrequencyDailySummary.sql
│   ├── example/                        ← Modelos de ejemplo dbt (no productivos)
│   └── temp/                           ← Modelos exploratorios ad-hoc
│       └── trip_gtfs_match.sql
├── macros/
│   ├── sonnell_recalc_subsystem_costs.sql
│   ├── sonnell_recalc_offsets_by_month.sql
│   ├── sonnell_recalc_invoice_by_month.sql
│   └── ama_backfill_gps_comunicacion.sql  ← Backfill GPS_Comunicacion para N días
├── tests/                              ← Tests singulares (assert_*.sql)
├── snapshots/
├── seeds/
├── dbt_project.yml
├── profiles.yml                        ← En la raíz del repo (NO en ~/.dbt/). Contiene credenciales en texto plano — nunca commitear cambios.
└── packages.yml                        ← (archivo no encontrado)
```

---

## Convenciones de Naming

| Objeto | Patrón | Ejemplo |
|--------|--------|---------|
| Staging | `stg_{source}__{entity}` | `stg_salesforce__accounts` |
| Intermediate | `int_{domain}__{verb}` | `int_sales__orders_enriched` |
| Fact | `fct_{event}` | `fct_orders` |
| Dimension | `dim_{entity}` | `dim_customer` |
| Snapshot | `snap_{entity}` | `snap_dim_customer` |
| Macro | `{verb}_{noun}` | `clean_null_values` |
| Test singular | `assert_{condicion}` | `assert_revenue_non_negative` |

> **⚠️ Excepciones reales en el proyecto:**
> - **Modelos AMA**: usan `PascalCase` sin prefijo de capa (ej: `AMA_Geotab_Viajes`), no siguen el patrón `fct_`/`stg_`. No tienen `schema.yml` ni tests.
> - **Modelos legacy raíz**: `dimOperators`, `dimRoutes`, `FrequencyDailySummary` — usan convención propia sin prefijo estándar y sin schema.yml.
> - **Intermediate AMA**: `stg_geotab__zone` usa prefijo `stg_` aunque es un modelo ephemeral de capa intermedia. `stg_geotab_device` está en `models/ama/intermediate/` con un solo guión bajo (no sigue doble underscore estrictamente).

---

## Coding Standards

- **SQL**: snake_case, sin `SELECT *` en producción, CTEs para legibilidad
- **DBT refs**: siempre `ref()` para modelos internos, `source()` para raw — nunca nombre hardcodeado
- **Schema tests**: todo modelo nuevo necesita al menos `unique` + `not_null` en su PK
- **Documentación**: description obligatoria en todo modelo y columna importante
- **Idempotencia**: todo modelo debe poder re-correrse sin efectos secundarios
- **Header**: comment de propósito + source + fecha en todo archivo nuevo

---

## Reglas de Git

- Rama principal: `main` (protegida, PR obligatorio)
- Feature branches: `feature/{descripcion-breve}`
- Fix branches: `fix/{descripcion-breve}`
- Commit convention: `feat(model): add stg_erp__orders` / `fix(test): correct null check`
- CI debe pasar antes de merge (dbt compile + dbt test --select state:modified+)

---

## Fuentes de Datos Registradas

| Source | Database | Schema | Entidades Principales | Notas |
|--------|----------|--------|-----------------------|-------|
| `sonnell` | ati_datawarehouse | `dbo` | SonnellDailySummary, SonnellRates, SonnellCheckpoints, SonnellParameters, SonnellOffsetApplied, SonnellSubsystemCost, SonnellInvoiceTotals | Pipeline de facturación Sonnell. Las tablas de output también registradas como source para post-hooks. |
| `external` | ati_datawarehouse | `raw` | sonnell_trip, sonnell_subsystem, Sonnell_checkpoin | Tablas externas del data lake (Parquet). Registradas en `external.yml`. |
| `geotab` | ati_datawarehouse | `staging` | geotab_trip, geotab_device, geotab_device_status_info, geotab_log_record, geotab_exception_event, geotab_zone, geotab_zone_type, geotab_rule, geotab_route, geotab_route_plan_item, geotab_fault_data, geotab_group, geotab_status_data, geotab_planned_vs_actual | Fuente de datos de flota AMA. |
| `ama` | ati_datawarehouse | `dbo` | AMA_Itinerario | Itinerario programado AMA. Disponible desde 2026-03-26. |
| `hms` | ati_datawarehouse | `raw` | hms_trips | Trips de ferry HMS (datos crudos del data lake). Registrado en `hms.yml`. |
| `hms_output` | ati_datawarehouse | `dbo` | HMS_MonthlyDataTrip, HmsNtdData | Tablas de salida HMS registradas para referencias en post-hooks. |
| `ati_lakehouse` | ati_lakehouse | `dbo` | control_raw_file_status | Tabla de control de ingesta de notebooks. Fuente del modelo de monitoreo. Base de datos separada — requiere acceso cross-database. |
| `gtfs` | ati_datawarehouse | `dbo` | gtfs_trips, gtfs_stop_times, gtfs_routes, gtfs_calendar_dates | GTFS estático. Nota: `.gtfs_calendar_dates` tiene un punto en el nombre en el YAML (posible typo). |
| `korbato` | ati_datawarehouse | `dbo` | fleet, vehicle_day, trip, vwRoutes, visit, trip_sch_match | Fuente de datos Korbato. |
| `tdev` | ati_datawarehouse | `dbo` | TDEVDailySummary, TDEVCheckpoints, TDEVTrips, TDEVSubsystemCost, TDEVSubsystemOffset | Pipeline TDEV (producción). |
| `tdev_dev` | ati_datawarehouse | `dev` | TDEVDailySummary, TDEVCheckpoints, TDEVTrips, TDEVSubsystemCost, TDEVSubsystemOffset | Pipeline TDEV (entorno desarrollo). |

---

## Modelos Clave del Proyecto

### Dominio: Sonnell (Facturación)

| Modelo | Layer | Grain | Materialización | Tests |
|--------|-------|-------|-----------------|-------|
| `stg_sonnell_daily_summary` | staging | Row de SonnellDailySummary | view | not_null en ServiceDate, GroupId, Version |
| `stg_sonnell_rates` | staging | Row de SonnellRates | view | not_null en GroupId |
| `stg_sonnell_checkpoints` | staging | Row de SonnellCheckpoints | view | not_null en ServiceDate, Subsystem, Version |
| `stg_sonnell_parameters` | staging | Row de SonnellParameters | view | not_null en GroupId |
| `stg_sonnell_trip` | staging | Row de sonnell_trip (raw) | view | — |
| `fct_sonnell_subsystem_cost` | fact | `DailySummaryId` (SCD Type 2 por ServiceDate) | incremental (append) | not_null + unique en DailySummaryId; not_null en ServiceDate, Year, Month, GroupId, RouteId, Version, CurrentVersion |
| `fct_sonnell_subsystem_offset` | fact | `(Year, Month, Subsystem, Version)` (SCD Type 2) | incremental (append) | not_null en Year, Month, Subsystem, Version, CurrentVersion |
| `fct_sonnell_invoice_totals` | fact | `(Year, Month, Subsystem, Type, Version)` (SCD Type 2) | incremental (append) | not_null en Year, Month, Subsystem, Type, Version, CurrentVersion; accepted_values en Type y CurrentVersion |

### Dominio: AMA (Dashboard Flota)

| Modelo | Layer | Grain | Materialización | Tests |
|--------|-------|-------|-----------------|-------|
| `AMA_Geotab_Viajes` | fact | `trip_id` / viaje Geotab | incremental (append, last 1 day on `trip_start`) | Sin schema.yml — sin tests |
| `AMA_Geotab_Salida_Vehiculos` | fact | `(route_id, date)` KPIs diarios de salida | incremental (append, last 1 day on `[date]`) | Sin schema.yml — sin tests |
| `AMA_Geotab_Exceso_Velocidad` | fact | Evento de velocidad excesiva | incremental (append, last 1 day on `log_datetime`) | Sin schema.yml — sin tests |
| `AMA_Geotab_GPS_Comunicacion` | snapshot diario | `(device_id, snapshot_date)` | incremental (append, snapshot diario) | Sin schema.yml — sin tests |
| `AMA_Geotab_Terminales` | fact | Evento dwell en terminal | incremental (append, last 1 day on `event_date`) | Sin schema.yml — sin tests |
| `stg_geotab__zone` | intermediate (ephemeral) | Zona Geotab con tipo parseado | ephemeral | Sin tests |
| `stg_geotab_device` | intermediate (ephemeral) | Dispositivo Geotab normalizado | ephemeral | Sin tests |

### Dominio: HMS (Ferry)

| Modelo | Layer | Grain | Materialización | Tests |
|--------|-------|-------|-----------------|-------|
| `stg_hms__trips` | staging (ephemeral) | Row deduplicado de hms_trips | ephemeral | not_null en date, vessel, route, scheduled_departure_time, pax |
| `fct_hms_monthly_trips` | fact | Row de trip ferry (alias HMS_MonthlyDataTrip) | table (`HASH(Id)`, CCI) | unique + not_null en Id; not_null en Date, Vessel, Route |
| `fct_hms_ntd_data` | fact | `(Vessel, Route, Date, IsOutbound, IsMissed)` (alias HmsNtdData) | incremental (append, `HASH(Date)`, CCI) | unique compuesto; not_null + accepted_values en IsOutbound, IsMissed, DayOfWeek |

### Dominio: Monitoring (Pipeline Health)

| Modelo | Layer | Grain | Materialización | Tests |
|--------|-------|-------|-----------------|-------|
| `stg_control_status` | staging (ephemeral) | Row de control_raw_file_status con entity_name + domain parseados | ephemeral | — |
| `stg_entity_config` | staging (ephemeral) | expected_frequency_hrs por dominio (inline) | ephemeral | — |
| `int_daily_aggregates` | intermediate (ephemeral) | `(event_date, entity_name, domain)` | ephemeral | — |
| `int_baseline_7d` | intermediate (ephemeral) | `(event_date, entity_name, domain)` con baseline estadístico | ephemeral | — |
| `int_latest_errors` | intermediate (ephemeral) | Último error por `(event_date, entity_name, domain)` | ephemeral | — |
| `int_health_metrics` | intermediate (ephemeral) | health_score + SLA flags | ephemeral | — |
| `fct_pipeline_health_daily` | fact | `(event_date, entity_name, domain)` | incremental (delete+insert, 7-day lookback, ROUND_ROBIN) | unique_combination_of_columns; expression_is_true; accepted_values en health_status, domain |

### Dominio: ATI Datawarehouse (Korbato Migration)

| Modelo | Layer | Notas |
|--------|-------|-------|
| `ati_datawarehouse/fleet` | fact | Tabla: `HASH(fleet_key)`, CCI. Tag: `ati_datawarehouse` |
| `ati_datawarehouse/trip` etc. | fact | Similar patrón: table, HASH dist, CCI. Leer cada modelo para grain. |
| `ati_datawarehouse/deduplicated/*` | staging (ephemeral) | Dedup de cada entidad Korbato |
| `ati_datawarehouse/intermediate/*` | intermediate | Transformaciones previas a la tabla final |

### SCD Type 2 (raíz de models/)

| Modelo | Grain | Materialización | Tests |
|--------|-------|-----------------|-------|
| `fct_operator_scd` | `(operator_id, valid_from)` | incremental (UPDATE pre_hook + INSERT append) | unique + not_null en scd_id; not_null en operator_id, valid_from, is_current |
| `fct_route_scd` | `(operator_id, route_id, valid_from)` | incremental (UPDATE pre_hook + INSERT append) | unique + not_null en scd_id; not_null en route_id, is_current |

### Modelos Legacy / Ad-hoc

| Modelo | Layer | Notas |
|--------|-------|-------|
| `dimOperators` | dimension (legacy) | Sin schema.yml, sin tests. Raíz de `models/`. |
| `dimRoutes` | dimension (legacy) | Sin schema.yml, sin tests. Raíz de `models/`. |
| `FrequencyDailySummary` | fact (legacy) | Sin schema.yml, sin tests. Raíz de `models/`. |
| `temp/trip_gtfs_match` | ad-hoc | Modelo exploratorio, no productivo. |

---

## Packages Instalados

> `packages.yml` — **(archivo no encontrado en el repositorio)**
> El modelo `fct_pipeline_health_daily` usa tests de `dbt_utils` (`unique_combination_of_columns`, `expression_is_true`). Crear `packages.yml` con `dbt-labs/dbt_utils` antes de correr `dbt test` sobre el dominio monitoring.

---

## Cómo Actualizar Este Archivo

Cuando la estructura del proyecto cambie (nuevos modelos, fuentes, convenciones), pegá este
prompt en Claude Code para que actualice CLAUDE.md automáticamente:

```
Lee los siguientes archivos del proyecto wh_transform y actualiza el CLAUDE.md en .claude/CLAUDE.md
para que refleje el estado real del proyecto:

Archivos a leer:
- dbt_project.yml
- packages.yml
- models/**/*.yml (todos los schema.yml y sources.yml)
- Glob de models/**/*.sql para mapear la estructura de directorios real

Secciones a actualizar en CLAUDE.md:
1. "Estructura del Proyecto" → reflejar árbol real de directorios con los modelos existentes
2. "Fuentes de Datos Registradas" → poblar desde los sources.yml encontrados
3. "Modelos Clave del Proyecto" → listar los modelos de marts y sus grains
4. "Packages Instalados" → copiar desde packages.yml
5. Completar los 📌 COMPLETAR con datos reales

Reglas:
- No eliminar ninguna sección existente
- No modificar Agent Team, Routing Rules ni Coding Standards
- Solo actualizar las secciones de datos del proyecto (estructura, fuentes, modelos, packages)
- Si encontrás convenciones que difieren de las documentadas, agregar una nota en "Convenciones de Naming"
- Guardar el archivo actualizado en .claude/CLAUDE.md
```
