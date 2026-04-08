"""Silver layer — consolidated dbt Cloud models, partitioned by operator.

Silver models read from per-operator bronze tables and write into a single
consolidated table partitioned/clustered by operator_id. Each Dagster partition
triggers a dbt Cloud run with `--vars '{operator_id: XX}'`, writing a disjoint
slice into the shared silver table.

This is the core of the multi-operator pattern: same dbt model, 25 parallel
runs, each writing to the same physical table as non-overlapping partitions.
"""

import random

import dagster as dg

from dbt_cloud_multi_operator.constants import (
    DATABRICKS_CATALOG,
    DATABRICKS_SILVER_SCHEMA,
    DBT_CLOUD_SILVER_JOB_ID,
    operators_partitions,
)


@dg.asset(
    partitions_def=operators_partitions,
    group_name="silver",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "silver", "dbt_cloud_job_id": str(DBT_CLOUD_SILVER_JOB_ID)},
    owners=["team:data-engineering"],
    deps=[dg.AssetKey("bronze_transactions"), dg.AssetKey("bronze_customers")],
    description="Cleaned and standardized transactions. Consolidated table partitioned "
    "by operator_id. Each dbt Cloud run processes one operator's slice.\n\n"
    "dbt command: `dbt build --select silver_transactions --vars '{operator_id: XX}'`",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_transactions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_SILVER_JOB_ID}:\n"
        f"  dbt build --select silver_transactions\n"
        f"  --vars '{{operator_id: {operator_id}}}'\n"
        f"  Writing to: {DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_transactions\n"
        f"  Partition: operator_id = '{operator_id}'"
    )

    row_count = random.randint(8000, 400000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_transactions"
            ),
            "row_count": dg.MetadataValue.int(row_count),
            "dbt_cloud_job_id": dg.MetadataValue.int(DBT_CLOUD_SILVER_JOB_ID),
            "dbt_command": dg.MetadataValue.text(
                f"dbt build --select silver_transactions "
                f"--vars '{{operator_id: {operator_id}}}'"
            ),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="silver",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "silver", "dbt_cloud_job_id": str(DBT_CLOUD_SILVER_JOB_ID)},
    owners=["team:data-engineering"],
    deps=[dg.AssetKey("bronze_customers")],
    description="Cleaned customer dimension. Consolidated table partitioned by operator_id. "
    "Deduped, standardized names, validated emails.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_SILVER_JOB_ID}:\n"
        f"  dbt build --select silver_customers\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(3000, 80000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_customers"
            ),
            "row_count": dg.MetadataValue.int(row_count),
            "dbt_command": dg.MetadataValue.text(
                f"dbt build --select silver_customers "
                f"--vars '{{operator_id: {operator_id}}}'"
            ),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="silver",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "silver", "dbt_cloud_job_id": str(DBT_CLOUD_SILVER_JOB_ID)},
    owners=["team:data-engineering"],
    deps=[dg.AssetKey("bronze_products")],
    description="Standardized product catalog. Consolidated table partitioned by operator_id. "
    "Normalized categories, currency conversion.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_products(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_SILVER_JOB_ID}:\n"
        f"  dbt build --select silver_products\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(400, 8000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_products"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="silver",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "silver", "dbt_cloud_job_id": str(DBT_CLOUD_SILVER_JOB_ID)},
    owners=["team:data-engineering"],
    deps=[dg.AssetKey("bronze_payments"), dg.AssetKey("bronze_transactions")],
    description="Reconciled payment events. Consolidated table partitioned by operator_id. "
    "Matched to transactions, currency-normalized.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_payments(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_SILVER_JOB_ID}:\n"
        f"  dbt build --select silver_payments\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(6000, 250000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_payments"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


@dg.asset(
    partitions_def=operators_partitions,
    group_name="silver",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "silver", "dbt_cloud_job_id": str(DBT_CLOUD_SILVER_JOB_ID)},
    owners=["team:data-engineering"],
    deps=[dg.AssetKey("silver_transactions"), dg.AssetKey("silver_customers")],
    description="Transaction-customer join. Enriches transactions with customer attributes. "
    "Consolidated table partitioned by operator_id.",
    op_tags={"dagster/concurrency_key": "dbt_cloud"},
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_enriched_transactions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    operator_id = context.partition_key

    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_SILVER_JOB_ID}:\n"
        f"  dbt build --select silver_enriched_transactions\n"
        f"  --vars '{{operator_id: {operator_id}}}'"
    )

    row_count = random.randint(7000, 350000)
    return dg.MaterializeResult(
        metadata={
            "operator_id": dg.MetadataValue.text(operator_id),
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_SILVER_SCHEMA}.silver_enriched_transactions"
            ),
            "row_count": dg.MetadataValue.int(row_count),
        }
    )


# Asset checks for silver layer
@dg.asset_check(
    asset=silver_transactions,
    description="Verify no duplicate transaction_ids within an operator partition",
)
def check_silver_transactions_unique(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    context.log.info("[DEMO MODE] Would check for duplicate transaction_ids in Databricks")
    return dg.AssetCheckResult(passed=True)


@dg.asset_check(
    asset=silver_payments,
    description="Verify all payments link to valid transactions",
)
def check_silver_payments_referential_integrity(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    context.log.info("[DEMO MODE] Would verify FK integrity between payments and transactions")
    return dg.AssetCheckResult(passed=True)


defs = dg.Definitions(
    assets=[
        silver_transactions,
        silver_customers,
        silver_products,
        silver_payments,
        silver_enriched_transactions,
    ],
    asset_checks=[
        check_silver_transactions_unique,
        check_silver_payments_referential_integrity,
    ],
)
