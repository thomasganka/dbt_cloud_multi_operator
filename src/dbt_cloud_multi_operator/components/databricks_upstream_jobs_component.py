"""DatabricksUpstreamJobsComponent — models Databricks jobs as Dagster assets.

Upstream Databricks jobs (ETL, ingestion, ML pipelines) are represented as
assets so that downstream dbt Cloud models can declare dependencies on them.
Dagster ensures dbt models only run after their upstream Databricks jobs complete.

Replaces manual trigger chains, polling, or time-based scheduling with
event-driven dependency resolution.
"""

import random

import dagster as dg
from dagster import AssetExecutionContext


def _make_databricks_job_asset(
    name: str,
    job_id: int,
    workspace_url: str,
    description: str,
    group_name: str,
    dep_keys: list[dg.AssetKey],
    owners: list[str],
    schedule_cron: str | None = None,
):
    """Factory for a Databricks job asset."""

    @dg.asset(
        name=name,
        group_name=group_name,
        kinds={"databricks", "spark"},
        tags={"databricks_job_id": str(job_id)},
        owners=owners,
        deps=dep_keys,
        description=description,
        metadata={
            "databricks_job_id": dg.MetadataValue.int(job_id),
            "workspace_url": dg.MetadataValue.url(
                f"{workspace_url}/#job/{job_id}"
            ),
        },
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        context.log.info(
            f"[DEMO MODE] Would trigger Databricks job {job_id}:\n"
            f"  Workspace: {workspace_url}\n"
            f"  Job: {name} (ID: {job_id})\n"
            f"  Via dagster-databricks Pipes or REST API"
        )

        run_id = random.randint(100000, 999999)
        return dg.MaterializeResult(
            metadata={
                "databricks_job_id": dg.MetadataValue.int(job_id),
                "databricks_run_id": dg.MetadataValue.int(run_id),
                "run_url": dg.MetadataValue.url(
                    f"{workspace_url}/#job/{job_id}/run/{run_id}"
                ),
                "duration_seconds": dg.MetadataValue.int(random.randint(30, 600)),
                "rows_written": dg.MetadataValue.int(random.randint(10000, 5000000)),
            }
        )

    return _asset


class DatabricksUpstreamJobsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Models Databricks jobs as Dagster assets for upstream dependency resolution.

    Downstream dbt Cloud models declare deps on these assets. Dagster ensures
    dbt models only materialize after their upstream Databricks jobs complete —
    replacing manual trigger chains, polling, or time-based scheduling.
    """

    workspace_url: str = "{{ env.DATABRICKS_HOST }}"
    group_name: str = "databricks_jobs"
    owners: list[str] = ["team:data-engineering"]
    jobs: list[dict] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        all_assets = []
        all_schedules = []

        for job_config in self.jobs:
            name = job_config["name"]
            job_id = job_config["job_id"]
            description = job_config.get(
                "description", f"Databricks job: {name}"
            )
            deps = job_config.get("deps", [])
            dep_keys = [dg.AssetKey(d) for d in deps]

            asset_def = _make_databricks_job_asset(
                name=name,
                job_id=job_id,
                workspace_url=self.workspace_url,
                description=description,
                group_name=self.group_name,
                dep_keys=dep_keys,
                owners=self.owners,
            )
            all_assets.append(asset_def)

            # Optional per-job schedule
            cron = job_config.get("cron")
            if cron:
                job_def = dg.define_asset_job(
                    name=f"{name}_job",
                    selection=[asset_def],
                    description=f"Scheduled run of Databricks job: {name}",
                )
                schedule_def = dg.ScheduleDefinition(
                    job=job_def,
                    cron_schedule=cron,
                    execution_timezone=job_config.get("timezone", "UTC"),
                    default_status=dg.DefaultScheduleStatus.RUNNING,
                )
                all_schedules.append(schedule_def)

        return dg.Definitions(
            assets=all_assets,
            schedules=all_schedules if all_schedules else None,
        )
