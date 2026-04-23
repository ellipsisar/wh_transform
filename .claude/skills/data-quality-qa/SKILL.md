---
name: data-quality-qa-agent
description: >
  Use this skill whenever the user is working on data quality, code review, or QA automation
  for data pipelines and analytics code. Triggers include: reviewing DBT models for quality
  issues, writing or improving data quality tests, performing code review of SQL, Python,
  PySpark, or ADF pipelines, designing automated QA frameworks, writing Great Expectations
  suites, auditing data freshness or completeness, detecting anomalies or data drift,
  reviewing schema changes, building data observability pipelines, or writing QA reports.
  Also trigger for requests about data contracts, SLA monitoring, data lineage checks,
  null/duplicate detection, referential integrity checks, or setting up alerting on data
  quality failures. If the user mentions "data quality", "QA", "code review", "test",
  "anomaly", "data drift", "observability", "data contract", "validation",
  "freshness check", or "audit" — always use this skill.
---

# Data Quality, Code Review & QA Automation Agent Skill

You are an expert Data Quality Engineer and QA Automation specialist. You systematically
identify data quality issues, enforce code standards, and build automated validation
frameworks across the modern data stack.

---

## Core Stack

| Category | Technology |
|---|---|
| DBT Testing | dbt built-in tests, dbt-utils, dbt-expectations |
| Python QA | Great Expectations, pandas-profiling, Soda Core |
| PySpark QA | DataFrame assertions, schema validation |
| Observability | Azure Monitor, Application Insights, custom log tables |
| Code Review | SQL linting (sqlfluff), Python linting (ruff, pylint) |
| Alerting | Azure Data Factory alerts, Azure Monitor alerts, Teams webhooks |

---

## Data Quality Dimensions

Always evaluate data against these six dimensions:

| Dimension | Definition | How to Test |
|---|---|---|
| **Completeness** | No unexpected nulls or missing records | NOT NULL tests, row count checks |
| **Accuracy** | Values match real-world facts | Range checks, business rule assertions |
| **Consistency** | Same entity = same value across systems | Cross-system reconciliation |
| **Timeliness** | Data is fresh within SLA | Freshness checks, max(updated_at) |
| **Uniqueness** | No unintended duplicates | DISTINCT count vs COUNT |
| **Validity** | Values conform to expected format/domain | Regex checks, accepted_values |

---

## DBT Testing Framework

### Comprehensive Schema Tests
```yaml
version: 2

models:
  - name: fct_orders
    description: "Grain: one row per order line item"
    meta:
      owner: data-team
      sla_freshness_hours: 4
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('stg_erp__orders')
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
              severity: error

      - name: order_date
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: "'2018-01-01'"
              max_value: "current_date"

      - name: net_revenue
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: -10000      # allow small corrections
              max_value: 10000000

      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']

      - name: country_code
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[A-Z]{2}$"
```

### Freshness Checks (sources.yml)
```yaml
sources:
  - name: erp
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: updated_at
    tables:
      - name: orders
        freshness:
          warn_after: {count: 2, period: hour}
          error_after: {count: 4, period: hour}
```

### Custom Singular Tests
```sql
-- tests/assert_no_duplicate_orders.sql
-- Every order_id must appear exactly once in fct_orders

SELECT order_id, COUNT(*) AS cnt
FROM {{ ref('fct_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
```

```sql
-- tests/assert_revenue_reconciles_with_source.sql
-- Net revenue in fct_orders must reconcile with raw source (tolerance: 0.01%)

WITH fct AS (
    SELECT SUM(net_revenue) AS fct_total
    FROM {{ ref('fct_orders') }}
    WHERE order_date = DATEADD(day, -1, CAST(GETDATE() AS DATE))
),
src AS (
    SELECT SUM(amount) AS src_total
    FROM {{ source('erp', 'orders') }}
    WHERE order_date = DATEADD(day, -1, CAST(GETDATE() AS DATE))
      AND status != 'cancelled'
)
SELECT
    fct_total,
    src_total,
    ABS(fct_total - src_total) / NULLIF(src_total, 0) AS variance_pct
FROM fct, src
WHERE ABS(fct_total - src_total) / NULLIF(src_total, 0) > 0.0001  -- fail if > 0.01%
```

```sql
-- tests/assert_no_future_dates.sql
SELECT order_id, order_date
FROM {{ ref('fct_orders') }}
WHERE order_date > CAST(GETDATE() AS DATE)
```

---

## Great Expectations (Python QA Framework)

### Suite Definition Pattern
```python
import great_expectations as gx

context = gx.get_context()

# Define expectation suite
suite = context.add_or_update_expectation_suite("orders.critical")

validator = context.sources.pandas_default.read_dataframe(
    df,
    asset_name="fct_orders",
    batch_slice=-1
)

# Completeness
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("order_date")

# Uniqueness
validator.expect_column_values_to_be_unique("order_id")

# Validity
validator.expect_column_values_to_be_in_set(
    "status", ["pending", "confirmed", "shipped", "delivered", "cancelled"]
)
validator.expect_column_values_to_match_regex("country_code", r"^[A-Z]{2}$")

# Range checks
validator.expect_column_values_to_be_between("net_revenue", min_value=0)
validator.expect_column_values_to_be_between(
    "order_date",
    min_value="2018-01-01",
    max_value=pd.Timestamp.today().strftime("%Y-%m-%d")
)

# Volume check
validator.expect_table_row_count_to_be_between(min_value=1000, max_value=10_000_000)

# Freshness (custom)
max_date = df["order_date"].max()
assert (pd.Timestamp.today() - max_date).days <= 1, f"Data is stale: max date = {max_date}"

validator.save_expectation_suite()
results = validator.validate()
print(f"Success: {results.success} | Failed: {results.statistics['unsuccessful_expectations']}")
```

---

## PySpark Data Quality

### Schema Validation
```python
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from pyspark.sql import DataFrame

EXPECTED_SCHEMA = StructType([
    StructField("order_id",     StringType(),       nullable=False),
    StructField("customer_id",  StringType(),       nullable=False),
    StructField("order_date",   DateType(),         nullable=False),
    StructField("net_revenue",  DecimalType(18,2),  nullable=False),
    StructField("status",       StringType(),       nullable=True),
])

def validate_schema(df: DataFrame, expected: StructType) -> None:
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    for field in expected.fields:
        assert field.name in actual_fields, f"Missing column: {field.name}"
        assert actual_fields[field.name] == field.dataType, \
            f"Type mismatch on {field.name}: expected {field.dataType}, got {actual_fields[field.name]}"
    print("✅ Schema validation passed")
```

### Data Quality Checks in PySpark
```python
from pyspark.sql import functions as F
from typing import Dict, List

def run_dq_checks(df: DataFrame, table_name: str) -> Dict:
    """Run standard DQ checks and return a quality report."""
    total = df.count()
    results = {"table": table_name, "total_rows": total, "checks": []}

    # Null checks
    for col in ["order_id", "customer_id", "order_date", "net_revenue"]:
        null_count = df.filter(F.col(col).isNull()).count()
        results["checks"].append({
            "check": f"null_check_{col}",
            "status": "PASS" if null_count == 0 else "FAIL",
            "details": f"{null_count} nulls ({null_count/total*100:.2f}%)"
        })

    # Duplicate check
    dup_count = total - df.dropDuplicates(["order_id"]).count()
    results["checks"].append({
        "check": "duplicate_order_id",
        "status": "PASS" if dup_count == 0 else "FAIL",
        "details": f"{dup_count} duplicate order_ids"
    })

    # Range check: no negative revenue
    neg_revenue = df.filter(F.col("net_revenue") < 0).count()
    results["checks"].append({
        "check": "negative_revenue",
        "status": "PASS" if neg_revenue == 0 else "WARN",
        "details": f"{neg_revenue} rows with negative revenue"
    })

    # Freshness check
    max_date = df.agg(F.max("order_date")).collect()[0][0]
    days_stale = (pd.Timestamp.today().date() - max_date).days
    results["checks"].append({
        "check": "data_freshness",
        "status": "PASS" if days_stale <= 1 else "FAIL",
        "details": f"Max date: {max_date} ({days_stale} days ago)"
    })

    # Log results to QA table in ADLS/Synapse
    _log_dq_results(results)
    return results

def _log_dq_results(results: Dict) -> None:
    """Persist DQ results to a quality log table."""
    log_df = spark.createDataFrame([{
        "table_name": results["table"],
        "run_timestamp": pd.Timestamp.now(),
        "total_rows": results["total_rows"],
        "checks_passed": sum(1 for c in results["checks"] if c["status"] == "PASS"),
        "checks_failed": sum(1 for c in results["checks"] if c["status"] == "FAIL"),
        "details_json": str(results["checks"])
    }])
    log_df.write.format("delta").mode("append").save(
        "abfss://gold@<account>.dfs.core.windows.net/dq_log/"
    )
```

---

## Code Review Checklist

### SQL Code Review
```markdown
## SQL Review Checklist

### Correctness
- [ ] JOIN type is correct (INNER vs LEFT vs FULL OUTER)
- [ ] WHERE filters applied before GROUP BY (not HAVING unless needed)
- [ ] DISTINCT used intentionally, not to mask join fan-out issues
- [ ] Date arithmetic is timezone-aware
- [ ] Division uses NULLIF to avoid divide-by-zero
- [ ] Window functions have correct PARTITION BY / ORDER BY

### Performance
- [ ] No SELECT * in production models
- [ ] Large tables filtered early (predicate pushdown friendly)
- [ ] No implicit type casts in JOIN conditions
- [ ] Appropriate distribution key for Synapse tables
- [ ] Statistics up to date on large tables

### Data Quality
- [ ] NULL handling explicit (COALESCE, ISNULL, NULLIF where needed)
- [ ] String comparisons case-insensitive where needed (LOWER/UPPER)
- [ ] Date columns cast to DATE, not left as DATETIME unless needed
- [ ] Hardcoded values replaced with source columns or config tables

### DBT Specific
- [ ] Model has description in schema.yml
- [ ] All columns have tests (unique, not_null at minimum for PKs)
- [ ] `ref()` used for model references, never hardcoded table names
- [ ] `source()` used for raw source tables
- [ ] Incremental logic is correct and idempotent
```

### Python / PySpark Code Review
```markdown
## Python Review Checklist

### Code Quality
- [ ] Functions are documented with docstrings
- [ ] Type hints present on function signatures
- [ ] No hardcoded credentials or connection strings
- [ ] Exception handling with specific exception types (not bare `except:`)
- [ ] Logging via `logging` module, not `print()`

### PySpark Specific
- [ ] Schema defined explicitly (no `inferSchema=True` in production)
- [ ] Actions (.count(), .collect(), .show()) minimized in loops
- [ ] `cache()` used for DataFrames reused multiple times (and `unpersist()` after use)
- [ ] Shuffle partitions tuned (`spark.conf.set("spark.sql.shuffle.partitions", N)`)
- [ ] No `.toPandas()` on large DataFrames
- [ ] Broadcast joins used for small dimension tables

### Data Handling
- [ ] Idempotent writes (upsert with Delta, not append-only for mutable data)
- [ ] Partitioning strategy defined for output tables
- [ ] No silent data loss (validate row counts before/after transformation)
```

---

## Anomaly Detection

### Statistical Anomaly Detection (Z-Score)
```python
import pandas as pd
import numpy as np

def detect_anomalies_zscore(df: pd.DataFrame, metric_col: str,
                              threshold: float = 3.0) -> pd.DataFrame:
    """Flag rows where metric deviates > threshold standard deviations from mean."""
    mean = df[metric_col].mean()
    std = df[metric_col].std()
    df["z_score"] = (df[metric_col] - mean) / std
    df["is_anomaly"] = df["z_score"].abs() > threshold
    return df[df["is_anomaly"]]
```

### Row Count Anomaly Detection (Rolling Baseline)
```sql
-- Flag days where row count deviates > 2 stdev from 30-day rolling average
WITH daily_counts AS (
    SELECT
        CAST(loaded_at AS DATE)     AS load_date,
        COUNT(*)                    AS row_count
    FROM fct_orders
    GROUP BY CAST(loaded_at AS DATE)
),
rolling_stats AS (
    SELECT
        load_date,
        row_count,
        AVG(row_count) OVER (ORDER BY load_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS rolling_avg,
        STDEV(row_count) OVER (ORDER BY load_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS rolling_std
    FROM daily_counts
)
SELECT
    load_date,
    row_count,
    rolling_avg,
    ABS(row_count - rolling_avg) / NULLIF(rolling_std, 0) AS z_score,
    CASE WHEN ABS(row_count - rolling_avg) / NULLIF(rolling_std, 0) > 2 THEN 'ANOMALY' ELSE 'OK' END AS status
FROM rolling_stats
ORDER BY load_date DESC;
```

---

## Data Contracts

### Contract Definition Template
```yaml
# data_contracts/orders.yaml
contract_version: "1.0"
owner: "data-engineering-team"
sla:
  freshness_hours: 4
  availability_pct: 99.5

schema:
  - name: order_id
    type: STRING
    nullable: false
    unique: true
    description: "Unique identifier for each order"

  - name: customer_id
    type: STRING
    nullable: false
    description: "FK to dim_customer"

  - name: net_revenue
    type: DECIMAL(18,2)
    nullable: false
    min_value: 0
    description: "Revenue after discounts, excluding tax"

quality_rules:
  - name: no_duplicates
    sql: "SELECT order_id, COUNT(*) FROM fct_orders GROUP BY order_id HAVING COUNT(*) > 1"
    expect_zero_rows: true

  - name: revenue_reconciliation
    description: "Net revenue must reconcile with source within 0.01%"
    tolerance_pct: 0.01
```

---

## Alerting & Observability

### Azure Monitor Alert (via Terraform/Bicep)
```bicep
resource dqAlert 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = {
  name: 'dq-fct-orders-freshness-alert'
  location: resourceGroup().location
  properties: {
    description: 'Alert when fct_orders data is older than 4 hours'
    severity: 1
    enabled: true
    evaluationFrequency: 'PT30M'
    windowSize: 'PT1H'
    criteria: {
      allOf: [{
        query: '''
          DQLog
          | where table_name == "fct_orders"
          | where check_name == "data_freshness"
          | where status == "FAIL"
        '''
        threshold: 0
        operator: 'GreaterThan'
        timeAggregation: 'Count'
      }]
    }
    actions: {
      actionGroups: [teamsActionGroupId]
    }
  }
}
```

### Teams Webhook Notification
```python
import requests
import json

def send_dq_alert(table: str, check: str, details: str, webhook_url: str):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "FF0000",
        "summary": f"Data Quality Alert: {table}",
        "sections": [{
            "activityTitle": f"⚠️ DQ Failure: {table}",
            "facts": [
                {"name": "Check", "value": check},
                {"name": "Details", "value": details},
                {"name": "Time", "value": pd.Timestamp.now().isoformat()}
            ]
        }]
    }
    requests.post(webhook_url, data=json.dumps(payload),
                  headers={"Content-Type": "application/json"})
```

---

## SQLFluff Configuration

```ini
# .sqlfluff
[sqlfluff]
dialect = tsql
templater = dbt
max_line_length = 120
indent_unit = space
indent_width = 4

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower

[sqlfluff:rules:layout.long_lines]
ignore_comment_lines = True
```

Run in CI:
```bash
sqlfluff lint models/ --dialect tsql
sqlfluff fix models/ --dialect tsql --force
```

---

## QA Report Template

When producing a data quality report, always include:

```markdown
## Data Quality Report — {table_name} — {date}

**Overall Status**: ✅ PASS / ⚠️ WARN / ❌ FAIL

| Check | Status | Details |
|---|---|---|
| Row Count | ✅ PASS | 1,243,891 rows (expected: >1M) |
| Null: order_id | ✅ PASS | 0 nulls |
| Null: customer_id | ✅ PASS | 0 nulls |
| Duplicates | ✅ PASS | 0 duplicate order_ids |
| Revenue Range | ✅ PASS | Min: $0.01, Max: $98,432 |
| Freshness | ✅ PASS | Max date: 2025-04-21 (0 days ago) |
| Reconciliation | ⚠️ WARN | Variance: 0.003% (within 0.01% tolerance) |

**Action Items**: None required.
**Next run**: 2025-04-22 08:00 UTC
```

---

## References
- [dbt Testing Docs](https://docs.getdbt.com/docs/build/tests)
- [dbt-expectations Package](https://github.com/calogica/dbt-expectations)
- [Great Expectations Docs](https://docs.greatexpectations.io/)
- [Soda Core](https://docs.soda.io/soda-core/overview-main.html)
- [SQLFluff](https://docs.sqlfluff.com/)