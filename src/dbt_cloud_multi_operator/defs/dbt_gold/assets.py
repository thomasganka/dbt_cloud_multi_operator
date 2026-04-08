"""Gold layer — aggregated analytics models, cross-operator.

Gold models read from ALL operator partitions in the silver layer and produce
consolidated analytics tables. These run AFTER all operator partitions complete
using AllPartitionMapping, ensuring data completeness across all 25 operators.

Gold layer uses a single dbt Cloud run (no per-operator fan-out needed).
"""

import random

import dagster as dg

from dbt_cloud_multi_operator.constants import (
    DATABRICKS_CATALOG,
    DATABRICKS_GOLD_SCHEMA,
    DBT_CLOUD_GOLD_JOB_ID,
)


@dg.asset(
    group_name="gold",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "gold", "dbt_cloud_job_id": str(DBT_CLOUD_GOLD_JOB_ID)},
    owners=["team:analytics"],
    deps=[
        dg.AssetDep(
            "silver_enriched_transactions",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    description="Revenue summary by operator and month. Aggregates all operator "
    "partitions from silver_enriched_transactions. Runs after ALL "
    "operators complete.",
)
def gold_revenue_summary(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_GOLD_JOB_ID}:\n"
        f"  dbt build --select gold_revenue_summary\n"
        f"  Reads from ALL operator partitions in silver_enriched_transactions\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_revenue_summary"
    )

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_revenue_summary"
            ),
            "operators_included": dg.MetadataValue.int(25),
            "row_count": dg.MetadataValue.int(random.randint(500, 2000)),
            "dbt_cloud_job_id": dg.MetadataValue.int(DBT_CLOUD_GOLD_JOB_ID),
        }
    )


@dg.asset(
    group_name="gold",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "gold", "dbt_cloud_job_id": str(DBT_CLOUD_GOLD_JOB_ID)},
    owners=["team:analytics"],
    deps=[
        dg.AssetDep(
            "silver_customers",
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            "silver_enriched_transactions",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    description="Customer lifetime value by operator. Cross-operator customer analytics "
    "with cohort analysis and retention metrics.",
)
def gold_customer_ltv(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_GOLD_JOB_ID}:\n"
        f"  dbt build --select gold_customer_ltv\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_customer_ltv"
    )

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_customer_ltv"
            ),
            "operators_included": dg.MetadataValue.int(25),
            "row_count": dg.MetadataValue.int(random.randint(50000, 500000)),
        }
    )


@dg.asset(
    group_name="gold",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "gold", "dbt_cloud_job_id": str(DBT_CLOUD_GOLD_JOB_ID)},
    owners=["team:analytics"],
    deps=[
        dg.AssetDep(
            "silver_products",
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            "silver_enriched_transactions",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    description="Product performance across all operators. Revenue, units sold, "
    "and growth metrics by product and operator.",
)
def gold_product_performance(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_GOLD_JOB_ID}:\n"
        f"  dbt build --select gold_product_performance\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_product_performance"
    )

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_product_performance"
            ),
            "operators_included": dg.MetadataValue.int(25),
            "row_count": dg.MetadataValue.int(random.randint(5000, 50000)),
        }
    )


@dg.asset(
    group_name="gold",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "gold", "dbt_cloud_job_id": str(DBT_CLOUD_GOLD_JOB_ID)},
    owners=["team:analytics"],
    deps=[
        dg.AssetDep(
            "silver_payments",
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            "silver_enriched_transactions",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    description="Payment reconciliation report. Cross-operator payment matching, "
    "failed payment analysis, and settlement tracking.",
)
def gold_payment_reconciliation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_GOLD_JOB_ID}:\n"
        f"  dbt build --select gold_payment_reconciliation\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_payment_reconciliation"
    )

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_payment_reconciliation"
            ),
            "operators_included": dg.MetadataValue.int(25),
            "row_count": dg.MetadataValue.int(random.randint(10000, 100000)),
        }
    )


@dg.asset(
    group_name="gold",
    kinds={"databricks", "dbt-cloud", "delta"},
    tags={"layer": "gold", "dbt_cloud_job_id": str(DBT_CLOUD_GOLD_JOB_ID)},
    owners=["team:analytics"],
    deps=[
        dg.AssetKey("gold_revenue_summary"),
        dg.AssetKey("gold_customer_ltv"),
        dg.AssetKey("gold_product_performance"),
    ],
    description="Executive KPI dashboard table. Top-level operator comparison with "
    "revenue, customer, and product KPIs side by side.",
)
def gold_operator_scorecard(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(
        f"[DEMO MODE] Would trigger dbt Cloud job {DBT_CLOUD_GOLD_JOB_ID}:\n"
        f"  dbt build --select gold_operator_scorecard\n"
        f"  Target: {DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_operator_scorecard"
    )

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(
                f"{DATABRICKS_CATALOG}.{DATABRICKS_GOLD_SCHEMA}.gold_operator_scorecard"
            ),
            "operators_included": dg.MetadataValue.int(25),
            "row_count": dg.MetadataValue.int(25),
        }
    )


# Asset checks for gold layer
@dg.asset_check(
    asset=gold_revenue_summary,
    description="Verify all 25 operators are represented in revenue summary",
)
def check_gold_revenue_all_operators(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    context.log.info("[DEMO MODE] Would verify 25 distinct operator_ids in gold_revenue_summary")
    return dg.AssetCheckResult(passed=True)


@dg.asset_check(
    asset=gold_revenue_summary,
    description="Cross-operator revenue reconciliation — totals match silver layer",
)
def check_gold_revenue_reconciliation(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    context.log.info(
        "[DEMO MODE] Would compare SUM(revenue) in gold vs silver_enriched_transactions"
    )
    return dg.AssetCheckResult(passed=True)


defs = dg.Definitions(
    assets=[
        gold_revenue_summary,
        gold_customer_ltv,
        gold_product_performance,
        gold_payment_reconciliation,
        gold_operator_scorecard,
    ],
    asset_checks=[
        check_gold_revenue_all_operators,
        check_gold_revenue_reconciliation,
    ],
)
