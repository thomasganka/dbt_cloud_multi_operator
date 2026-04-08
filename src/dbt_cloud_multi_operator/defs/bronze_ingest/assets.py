"""Bronze layer — raw data ingestion per operator into Databricks.

Each operator has its own set of bronze tables (e.g., bronze_BE_transactions,
bronze_NL_customers). These are ingested from source systems into Databricks
Delta tables via dbt Cloud, one operator partition at a time.
"""

import random

import dagster as dg

from dbt_cloud_multi_operator.constants import (
    DATABRICKS_BRONZE_SCHEMA,
    DATABRICKS_CATALOG,
    DBT_CLOUD_BRONZE_JOB_ID,
    operators_partitions,
)


# ── Bronze source tables (per-operator, separate tables) ─────────

@dg.asset(
    partitions_def=operators_partitions,
    group_name="bronze",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "bronze", "dbt_cloud_job_id": str(DBT_CLOUD_BRONZE_JOB_ID)},
    owners=["team:data-engineering"],
    description="Raw transaction data per operator. Each operator has a separate "
    "bronze table (e.g., bronze_BE_transactions). Loaded via dbt Cloud "
    "run with operator_id variable.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.LINEAR),
)
def bronze_transactions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key
    table_name = f"bronze_{operator_id}_transactions"

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_BRONZE_JOB_ID}:\n"
        f"  dbt run --select {table_name}\n"
        f"  --vars '{{operator_id: {operator_id}}}'\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
    )

    row_count = random.randint(10000, 500000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
            ),
            "row_count": dg.MetadataValue.int(row_count),
            "dbt_cloud_job_id": dg.MetadataValue.int(DBT_CLOUD_BRONZE_JOB_ID),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="bronze",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "bronze", "dbt_cloud_job_id": str(DBT_CLOUD_BRONZE_JOB_ID)},
    owners=["team:data-engineering"],
    description="Raw customer data per operator. Separate bronze table per country.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.LINEAR),
)
def bronze_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key
    table_name = f"bronze_{operator_id}_customers"

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_BRONZE_JOB_ID}:\n"
        f"  dbt run --select {table_name}\n"
        f"  --vars '{{operator_id: {operator_id}}}'\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
    )

    row_count = random.randint(5000, 100000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="bronze",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "bronze", "dbt_cloud_job_id": str(DBT_CLOUD_BRONZE_JOB_ID)},
    owners=["team:data-engineering"],
    description="Raw product catalog per operator. Separate bronze table per country.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.LINEAR),
)
def bronze_products(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key
    table_name = f"bronze_{operator_id}_products"

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_BRONZE_JOB_ID}:\n"
        f"  dbt run --select {table_name}\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(500, 10000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="bronze",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "bronze", "dbt_cloud_job_id": str(DBT_CLOUD_BRONZE_JOB_ID)},
    owners=["team:data-engineering"],
    description="Raw payment events per operator. Separate bronze table per country.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.LINEAR),
)
def bronze_payments(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key
    table_name = f"bronze_{operator_id}_payments"

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_BRONZE_JOB_ID}:\n"
        f"  dbt run --select {table_name}\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(8000, 300000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_BRONZE_SCHEMA}.{table_name}"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


# Asset checks
@dg.asset_check(
    asset=bronze_transactions,
    description="Verify transaction row count exceeds minimum threshold per operator",
)
def check_bronze_transactions_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    context.log.info("[DEMO MODE] Would query Databricks to check row count > 0")
    return dg.AssetCheckResult(passed=True)


defs = dg.Definitions(
    assets=[bronze_transactions, bronze_customers, bronze_products, bronze_payments],
    asset_checks=[check_bronze_transactions_not_empty],
)
