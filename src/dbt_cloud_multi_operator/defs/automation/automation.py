"""Schedules, sensors, and jobs for the multi-operator pipeline.

Key patterns:
- operator_pipeline_job: Materializes all layers for all operators (with concurrency)
- gold_refresh_job: Runs gold layer after all silver partitions complete
- Failure alerting sensor per operator
"""

import dagster as dg

from dbt_cloud_multi_operator.constants import operators_partitions

# ── Jobs ───────────────────────────────────────────────────

# Full pipeline — bronze through gold, concurrency-limited
operator_pipeline_job = dg.define_asset_job(
    name="operator_pipeline",
    selection=dg.AssetSelection.groups("bronze", "silver"),
    tags={"dagster/max_concurrency": "5"},
    description="Full Bronze→Silver pipeline for all operators. "
    "Concurrency limited to 5 parallel dbt Cloud runs.",
    partitions_def=operators_partitions,
)

# Gold layer refresh — unpartitioned, runs after all silver partitions
gold_refresh_job = dg.define_asset_job(
    name="gold_refresh",
    selection=dg.AssetSelection.groups("gold"),
    description="Refresh all gold layer aggregations after silver completes.",
)

# Per-operator backfill for priority markets
priority_operators_job = dg.define_asset_job(
    name="priority_operators_backfill",
    selection=dg.AssetSelection.groups("bronze", "silver"),
    tags={"dagster/max_concurrency": "3"},
    description="Backfill priority operators (high-revenue countries).",
    partitions_def=operators_partitions,
)

# ── Schedules ──────────────────────────────────────────────

# Daily bronze+silver for all operators at 2 AM UTC
daily_operator_schedule = dg.ScheduleDefinition(
    job=operator_pipeline_job,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    description="Daily Bronze→Silver pipeline for ALL 25 operators at 2 AM UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# Daily gold refresh at 6 AM UTC (after silver expected to complete)
daily_gold_schedule = dg.ScheduleDefinition(
    job=gold_refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    description="Daily gold layer refresh at 6 AM UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# ── Sensors ────────────────────────────────────────────────

@dg.run_status_sensor(
    monitored_jobs=[operator_pipeline_job],
    run_status=dg.DagsterRunStatus.FAILURE,
    description="Alert when any operator partition fails in the pipeline",
)
def operator_failure_alert(context: dg.RunStatusSensorContext):
    run = context.dagster_run
    failed_partition = run.tags.get("dagster/partition", "unknown")

    context.log.info(
        f"[DEMO MODE] Would send alert:\n"
        f"  Operator {failed_partition} pipeline FAILED\n"
        f"  Run ID: {run.run_id}\n"
        f"  Would notify #data-alerts Slack channel"
    )


defs = dg.Definitions(
    jobs=[operator_pipeline_job, gold_refresh_job, priority_operators_job],
    schedules=[daily_operator_schedule, daily_gold_schedule],
    sensors=[operator_failure_alert],
)
