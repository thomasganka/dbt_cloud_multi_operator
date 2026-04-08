# dbt Cloud Multi-Operator Pipeline

Dagster orchestration for **dbt Cloud on Databricks** with a **multi-operator (country) fan-out pattern**. Demonstrates Bronze/Silver/Gold medallion architecture where 25 operators are processed in parallel via partitioned assets.

## The Problem

We operate **~20-25 operators (countries)**. Bronze is split per operator (separate tables). Silver and above use one consolidated model/table, partitioned or clustered by operator. We need to **run the same dbt model (and its downstream lineage) many times in parallel**, each run with a different `operator_id`, writing into the same physical relation as disjoint slices.

**dbt Cloud's orchestration doesn't give us that fan-out** without creating one job per operator (doesn't scale) or giving up parallelism.

## How Dagster Solves It

dbt Cloud is great at running dbt. It's not great at running dbt **25 times in parallel with different parameters**. Dagster sits on top and provides exactly what's missing:

```
                    Dagster (orchestration layer)
                    ┌─────────────────────────────────┐
                    │  25 static partitions (countries) │
                    │  Concurrency limit: 5 at a time  │
                    │  Per-operator lineage & alerting  │
                    └──────────┬──────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   dbt Cloud run #1     dbt Cloud run #2     dbt Cloud run #3  ...x25
   --vars {op: BE}      --vars {op: NL}      --vars {op: DE}
          │                    │                    │
          └────────────────────┼────────────────────┘
                               ▼
                    Same physical Delta table
                    silver_transactions
                    (partitioned by operator_id)
```

You keep your **single dbt project**, your **single dbt Cloud job definition**, and your existing Databricks warehouse. Dagster calls the dbt Cloud API 25 times with different `--vars`, throttled to whatever your account can handle.

### Capability Mapping

| Requirement | Dagster Solution |
|---|---|
| Run same dbt model 25x with different params | **Static partitions** — one per operator, each triggers a dbt Cloud API call with `--vars '{operator_id: XX}'` |
| Parallel execution | Dagster materializes partitions in parallel automatically |
| Control concurrency (API/account limits) | `dagster/max_concurrency: 5` — tunable in YAML, no code changes |
| Bronze = separate tables per operator | Bronze assets produce `bronze_{op}_transactions`, etc. |
| Silver+ = consolidated, partitioned by operator | Silver writes disjoint slices into shared tables via per-run `--vars` |
| Gold waits for ALL operators | `AllPartitionMapping` — gold only materializes after all 25 silver partitions complete |
| Per-operator lineage & failure tracking | Partition-level metadata, failure sensor alerts with specific operator |
| Selective backfills | `dg launch --partition BE --partition NL --partition DE` |
| Single dbt project | One job definition, parameterized via Dagster variables |
| Adding a new operator | Add a country code to the YAML list — no Python or dbt Cloud changes |

## Architecture

### Data Flow

| Layer | Partitioning | dbt Cloud Execution |
|---|---|---|
| **Bronze** | Per-operator separate tables (`bronze_BE_transactions`, etc.) | One dbt Cloud run per operator partition |
| **Silver** | Consolidated tables, partitioned by `operator_id` | Same model, 25 parallel runs with `--vars '{operator_id: XX}'` |
| **Gold** | Unpartitioned aggregates across all operators | Single run after ALL operator partitions complete |

### Asset Graph

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

## Component-Driven Design

The entire pipeline is driven by a single custom **`DbtCloudMultiOperatorComponent`** and one `defs.yaml` file. No boilerplate Python per layer — all configuration is declarative:

```yaml
type: dbt_cloud_multi_operator.components...DbtCloudMultiOperatorComponent

attributes:
  operators: [BE, NL, DE, FR, UK, ...]   # Add a country = add a line
  max_concurrency: 5                      # Tune without code changes

  bronze:
    - name: bronze_transactions
      dbt_cloud_job_id: 100001
      dbt_select: "bronze_transactions"

  silver:
    - name: silver_transactions
      deps: [bronze_transactions, bronze_customers]
      eager: true                         # Auto-materialize when deps complete

  gold:
    - name: gold_revenue_summary
      deps:
        - asset: silver_enriched_transactions
          all_partitions: true            # Wait for ALL 25 operators
```

The component generates **14 assets, 5 checks, 2 jobs, 2 schedules, and 2 sensors** from this single YAML file.

## Definitions Summary

- **14 assets** (4 bronze + 5 silver + 5 gold), bronze/silver partitioned across 25 operators
- **5 asset checks** (row counts, uniqueness, referential integrity, cross-operator reconciliation)
- **2 jobs** (operator pipeline with concurrency limit, gold refresh)
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

Open http://localhost:3000 — click any asset, pick a partition (e.g. `BE`), materialize it, and see the dbt Cloud command that would fire. Backfill all 25 and watch the concurrency limit hold 20 in queue while 5 run.

## dbt Cloud Integration

In production, each partition materialization calls the dbt Cloud API:

```python
dbt_cloud.run_job(
    job_id=SILVER_JOB_ID,
    steps_override=[
        f"dbt build --select silver+ --vars '{{operator_id: {operator_id}}}'"
    ],
)
```

The dbt models use `{{ var('operator_id') }}` to read from the correct bronze source and write a disjoint slice into the consolidated silver table.

## Migration Path

1. **Prototype with 2-3 operators** — validate the pattern works end-to-end
2. **Test concurrency limits** — find optimal parallelism for your dbt Cloud account
3. **Migrate incrementally** — start with non-critical operators
4. **Monitor performance** — track dbt Cloud costs and run times per operator
5. **Scale to all 25 operators** — roll out to production

## Demo Mode

All assets run in demo mode — they log what the dbt Cloud API call would look like without requiring actual credentials. Safe to materialize anything for demonstration purposes.
