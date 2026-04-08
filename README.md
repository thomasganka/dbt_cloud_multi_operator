# dbt Cloud Multi-Operator Pipeline

Dagster orchestration for **dbt Cloud on Databricks** with a **multi-operator (country) fan-out pattern**. Demonstrates Bronze/Silver/Gold medallion architecture where 25 operators are processed in parallel via partitioned assets.

## The Pattern

| Layer | Partitioning | dbt Cloud Execution |
|---|---|---|
| **Bronze** | Per-operator separate tables (`bronze_BE_transactions`, etc.) | One dbt Cloud run per operator partition |
| **Silver** | Consolidated tables, partitioned by `operator_id` | Same model, 25 parallel runs with `--vars '{operator_id: XX}'` |
| **Gold** | Unpartitioned aggregates across all operators | Single run after ALL operator partitions complete |

### Why Dagster for this?

- **Partitioned Assets** = natural mapping to operators (25 static partitions)
- **Concurrency Limits** = control parallel dbt Cloud API calls (`dagster/max_concurrency: 5`)
- **AllPartitionMapping** = Gold layer waits for ALL operator partitions to complete
- **Per-operator lineage** = track success/failure per country
- **Single dbt project** = one set of models, parameterized via Dagster variables

## Architecture

```
Bronze (per-operator tables)         Silver (consolidated, partitioned)        Gold (aggregated)
┌─────────────────────────┐         ┌──────────────────────────┐              ┌─────────────────────┐
│ bronze_BE_transactions  │──┐      │                          │              │                     │
│ bronze_NL_transactions  │──┼─────>│  silver_transactions     │──┐           │ gold_revenue_summary│
│ bronze_DE_transactions  │──┘      │  (partitioned by op_id)  │  │           │                     │
│ ...25 operators         │         │                          │  ├──────────>│ gold_customer_ltv   │
│                         │         │  silver_customers        │  │           │                     │
│ bronze_BE_customers     │────────>│  silver_products         │  │           │ gold_product_perf   │
│ bronze_BE_products      │────────>│  silver_payments         │──┘           │                     │
│ bronze_BE_payments      │────────>│  silver_enriched_txns    │              │ gold_operator_score │
└─────────────────────────┘         └──────────────────────────┘              └─────────────────────┘
    25 partitions x 4 assets            25 partitions x 5 assets                  5 unpartitioned
```

## Definitions Summary

- **14 assets** (4 bronze + 5 silver + 5 gold), bronze/silver partitioned across 25 operators
- **5 asset checks** (row counts, uniqueness, referential integrity, cross-operator reconciliation)
- **3 jobs** (full pipeline, gold refresh, priority backfill)
- **2 schedules** (daily bronze+silver at 2 AM UTC, gold at 6 AM UTC)
- **2 sensors** (operator failure alerts, declarative automation)

## Quick Start

```bash
cd my-demos/dbt_cloud_multi_operator
source .venv/bin/activate

# Verify
uv run dg check defs

# Launch UI
uv run dg dev
```

## dbt Cloud Integration

In production, each asset would call the dbt Cloud API:

```python
# steps_override passes operator_id to dbt
dbt_cloud.run_job(
    job_id=SILVER_JOB_ID,
    steps_override=[
        f"dbt build --select silver+ --vars '{{operator_id: {operator_id}}}'"
    ],
)
```

The dbt models use `{{ var('operator_id') }}` to read from the correct bronze source and write a disjoint slice into the consolidated silver table.

## Demo Mode

All assets run in demo mode — they log what the dbt Cloud API call would look like without requiring actual credentials.
