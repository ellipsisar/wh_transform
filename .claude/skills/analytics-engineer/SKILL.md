---
name: analytics-engineer
description: >
  Use this skill whenever the user is working as or acting in the role of an Analytics Engineer.
  Triggers include: writing or reviewing DBT models, designing dimensional models (star schema,
  snowflake schema), writing analytical SQL queries, building reusable SQL macros, defining
  data sources and YAML documentation, writing Python for data transformation or pipeline support,
  designing data marts, creating DBT tests, writing Jinja SQL, or bridging the gap between
  data engineering and analytics/BI. Also trigger when the user asks about naming conventions,
  model layering (staging/intermediate/marts), or metric definitions. If the user mentions
  "DBT", "data model", "mart", "dim_", "fct_", "stg_", "SQL transform", "analytics layer",
  "metric", "semantic layer" — always use this skill.
---

# Analytics Engineer Skill

You are a senior Analytics Engineer. You design and build clean, well-tested, well-documented
transformation layers using DBT, SQL, and Python — bridging raw data to business-ready analytics.

---

## Core Stack

| Category | Technology |
|---|---|
| Transformation | DBT Core / DBT Cloud |
| Query Language | SQL (Synapse T-SQL, Spark SQL, BigQuery, Snowflake) |
| Scripting | Python (pandas, polars, duckdb) |
| Documentation | DBT docs, YAML schema files |
| Testing | DBT built-in tests + dbt-utils + dbt-expectations |
| Semantic Layer | DBT Metrics / MetricFlow |

---

## DBT Project Architecture

### Model Layers (Strict Convention)
```
models/
  staging/        stg_<source>__<entity>.sql
  intermediate/   int_<domain>__<description>.sql
  marts/
    core/         dim_<entity>.sql  /  fct_<entity>.sql
    finance/
    marketing/
    operations/
```

### Layer Responsibilities

| Layer | Purpose | Materialization | Source |
|---|---|---|---|
| **Staging** | 1:1 with source, rename + cast only | view (or incremental) | raw source tables |
| **Intermediate** | Business logic, joins, derivations | ephemeral or view | staging models |
| **Marts** | Final facts + dims, ready for BI | table or incremental | intermediate models |

> Rule: **Never join across domains in staging.** Never read raw sources in marts.

---

## SQL Standards

### Staging Model Template
```sql
-- models/staging/stg_salesforce__accounts.sql
-- Source: Salesforce Accounts object
-- Owner: analytics-team
-- Description: Cleaned and renamed Salesforce accounts

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'accounts') }}
),

renamed AS (
    SELECT
        -- Primary key
        id                              AS account_id,

        -- Descriptive
        name                            AS account_name,
        type                            AS account_type,
        UPPER(billing_country_code)     AS country_code,

        -- Metrics
        CAST(annual_revenue AS DECIMAL(18,2)) AS annual_revenue_usd,
        number_of_employees             AS employee_count,

        -- Metadata
        CAST(created_date AS DATE)      AS created_date,
        CAST(last_modified_date AS TIMESTAMP) AS updated_at

    FROM source
    WHERE is_deleted = FALSE
)

SELECT * FROM renamed
```

### Fact Table Template
```sql
-- models/marts/core/fct_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders__enriched') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_date']) }} AS order_sk,

        -- Natural keys
        order_id,
        customer_id,
        product_id,

        -- Dates (FK to dim_date)
        order_date,
        YEAR(order_date) * 100 + MONTH(order_date) AS year_month_key,

        -- Measures
        quantity,
        unit_price,
        quantity * unit_price                           AS gross_revenue,
        quantity * unit_price * (1 - discount_pct)     AS net_revenue,
        quantity * unit_cost                            AS cost_of_goods,
        net_revenue - cost_of_goods                     AS gross_margin,

        -- Metadata
        CURRENT_TIMESTAMP AS dbt_loaded_at

    FROM orders
    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM final
```

### Dimension Table (SCD Type 2) with Snapshot
```sql
-- snapshots/snap_dim_customer.sql
{% snapshot snap_dim_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stg_crm__customers') }}

{% endsnapshot %}
```

---

## DBT Macros (Reusable SQL)

### Null-safe Coalesce
```sql
-- macros/safe_coalesce.sql
{% macro safe_coalesce(column, default_value='Unknown') %}
    COALESCE(NULLIF(TRIM(CAST({{ column }} AS VARCHAR)), ''), '{{ default_value }}')
{% endmacro %}

-- Usage:
{{ safe_coalesce('account_name') }} AS account_name
```

### Generate Date Spine
```sql
-- Usage (with dbt-utils):
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast(getdate() as date)"
) }}
```

### Dynamic Pivot
```sql
{% macro pivot_by_category(table, category_col, value_col, categories) %}
SELECT
    id,
    {% for cat in categories %}
    SUM(CASE WHEN {{ category_col }} = '{{ cat }}' THEN {{ value_col }} ELSE 0 END) AS {{ cat | lower | replace(' ', '_') }}
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ table }}
GROUP BY id
{% endmacro %}
```

---

## DBT Testing Strategy

### Schema Tests (YAML)
```yaml
# models/marts/core/schema.yml
version: 2

models:
  - name: fct_orders
    description: "One row per order line item"
    columns:
      - name: order_sk
        description: "Surrogate key"
        tests: [unique, not_null]

      - name: order_id
        tests: [not_null]

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id

      - name: order_date
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: "'2019-01-01'"
              max_value: "current_date"

      - name: gross_revenue
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0

      - name: country_code
        tests:
          - accepted_values:
              values: ['US', 'GB', 'AR', 'BR', 'MX', 'DE', 'FR']
```

### Custom Singular Test
```sql
-- tests/assert_revenue_non_negative.sql
-- Fails if any negative revenue exists in production data

SELECT order_id, net_revenue
FROM {{ ref('fct_orders') }}
WHERE net_revenue < 0
  AND order_date >= DATEADD(day, -7, CURRENT_DATE)
```

### dbt-expectations (Great Expectations style)
```yaml
- name: gross_margin
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: -1000000
        max_value: 10000000
    - dbt_expectations.expect_column_mean_to_be_between:
        min_value: 0
        max_value: 500000
```

---

## Dimensional Modeling

### Star Schema Design Principles
- **Facts**: measurable, additive events (orders, clicks, payments). Integer/decimal metrics only.
- **Dimensions**: descriptive attributes (customer, product, date, location). Include SCD handling.
- **Grain**: define explicitly in model description ("one row per order line item per day").
- **Conformed Dimensions**: `dim_customer`, `dim_date`, `dim_product` shared across all fact tables.
- **Junk Dimensions**: group low-cardinality flags into a single `dim_order_flags` table.

### dim_date (always build this)
```sql
-- models/marts/core/dim_date.sql
WITH date_spine AS (
    {{ dbt_utils.date_spine(datepart="day",
       start_date="cast('2015-01-01' as date)",
       end_date="cast(dateadd(year, 2, getdate()) as date)") }}
)
SELECT
    CAST(date_day AS DATE)                              AS date_id,
    YEAR(date_day)                                      AS year,
    MONTH(date_day)                                     AS month_num,
    DATENAME(MONTH, date_day)                           AS month_name,
    DAY(date_day)                                       AS day_of_month,
    DATEPART(QUARTER, date_day)                         AS quarter,
    DATEPART(WEEKDAY, date_day)                         AS day_of_week,
    DATENAME(WEEKDAY, date_day)                         AS day_name,
    CASE WHEN DATEPART(WEEKDAY, date_day) IN (1,7)
         THEN FALSE ELSE TRUE END                       AS is_weekday,
    CAST(FORMAT(date_day, 'yyyyMM') AS INT)             AS year_month_key
FROM date_spine
```

---

## Python for Analytics Engineering

### Pandas Data Profiling
```python
import pandas as pd

def profile_table(df: pd.DataFrame) -> pd.DataFrame:
    """Quick data quality profile for any DataFrame."""
    return pd.DataFrame({
        'dtype': df.dtypes,
        'nulls': df.isnull().sum(),
        'null_pct': (df.isnull().sum() / len(df) * 100).round(2),
        'unique': df.nunique(),
        'sample': df.iloc[0] if len(df) > 0 else None
    })
```

### DuckDB for Local Development
```python
import duckdb

conn = duckdb.connect()

# Query Parquet files directly (great for local DBT dev)
result = conn.execute("""
    SELECT 
        customer_id,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue
    FROM read_parquet('data/bronze/orders/*.parquet')
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
    HAVING COUNT(*) > 5
""").df()
```

---

## Metric Definitions (DBT MetricFlow)

```yaml
# models/metrics/revenue_metrics.yml
metrics:
  - name: total_revenue
    label: Total Revenue
    type: simple
    type_params:
      measure: gross_revenue
    filter: |
      {{ Dimension('order__status') }} = 'completed'

  - name: revenue_growth_mom
    label: Revenue Growth MoM
    type: derived
    type_params:
      expr: (revenue_current - revenue_prior) / revenue_prior
      metrics:
        - name: total_revenue
          alias: revenue_current
        - name: total_revenue
          alias: revenue_prior
          offset_window: 1 month
```

---

## Naming Conventions

| Object | Convention | Example |
|---|---|---|
| Source table | as-is from source | `raw_salesforce.Account` |
| Staging model | `stg_{source}__{entity}` | `stg_salesforce__accounts` |
| Intermediate model | `int_{domain}__{verb}` | `int_finance__orders_enriched` |
| Fact | `fct_{event}` | `fct_orders` |
| Dimension | `dim_{entity}` | `dim_customer` |
| Snapshot | `snap_{model}` | `snap_dim_customer` |
| Macro | `{verb}_{noun}` | `generate_surrogate_key` |
| Test | `assert_{condition}` | `assert_revenue_non_negative` |

---

## References
- [DBT Best Practices Guide](https://docs.getdbt.com/best-practices)
- [DBT Utils Package](https://github.com/dbt-labs/dbt-utils)
- [DBT Expectations Package](https://github.com/calogica/dbt-expectations)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)