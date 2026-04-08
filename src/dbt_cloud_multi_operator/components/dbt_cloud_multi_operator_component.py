"""DbtCloudMultiOperatorComponent — YAML-driven multi-operator dbt Cloud fan-out.

Generates partitioned Dagster assets across Bronze/Silver/Gold layers from
declarative YAML config. Each operator (country) becomes a static partition,
and dbt Cloud jobs are triggered per-partition with operator-specific variables.
"""

import random
from collections.abc import Iterator

import dagster as dg
from dagster import AssetExecutionContext, AssetCheckExecutionContext


def _make_partitioned_asset(
    name: str,
    layer: str,
    partitions_def: dg.StaticPartitionsDefinition,
    dep_keys: list[dg.AssetKey],
    description: str,
    dbt_cloud_job_id: int,
    dbt_select: str,
    catalog: str,
    schema: str,
    concurrency_key: str,
    kinds: set[str],
    owners: list[str],
    automation: dg.AutomationCondition | None = None,
    checks: list[dict] | None = None,
):
    """Factory that produces a partitioned @dg.asset + optional checks."""

    @dg.asset(
        name=name,
        partitions_def=partitions_def,
        group_name=layer,
        kinds=kinds,
        tags={"layer": layer, "dbt_cloud_job_id": str(dbt_cloud_job_id)},
        owners=owners,
        deps=dep_keys,
        description=description,
        op_tags={"dagster/concurrency_key": concurrency_key},
        retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.LINEAR),
        automation_condition=automation,
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        operator_id = context.partition_key
        full_table = f"{catalog}.{schema}.{name}"

        context.log.info(
            f"[DEMO MODE] dbt Cloud job {dbt_cloud_job_id}:\n"
            f"  dbt build --select {dbt_select}\n"
            f"  --vars '{{operator_id: {operator_id}}}'\n"
            f"  Target: {full_table} (partition: {operator_id})"
        )

        return dg.MaterializeResult(
            metadata={
                "operator_id": dg.MetadataValue.text(operator_id),
                "table": dg.MetadataValue.text(full_table),
                "row_count": dg.MetadataValue.int(random.randint(1000, 500000)),
                "dbt_cloud_job_id": dg.MetadataValue.int(dbt_cloud_job_id),
                "dbt_command": dg.MetadataValue.text(
                    f"dbt build --select {dbt_select} "
                    f"--vars '{{operator_id: {operator_id}}}'"
                ),
            }
        )

    # Build asset checks
    check_defs = []
    if checks:
        for chk in checks:
            check_name = chk["name"]
            check_desc = chk.get("description", f"Check {check_name} on {name}")

            @dg.asset_check(name=check_name, asset=_asset, description=check_desc)
            def _check(
                context: AssetCheckExecutionContext,
                _desc=check_desc,
            ) -> dg.AssetCheckResult:
                context.log.info(f"[DEMO MODE] {_desc}")
                return dg.AssetCheckResult(passed=True)

            check_defs.append(_check)

    return _asset, check_defs


def _make_gold_asset(
    name: str,
    dep_configs: list[dict],
    description: str,
    dbt_cloud_job_id: int,
    dbt_select: str,
    catalog: str,
    schema: str,
    kinds: set[str],
    owners: list[str],
    checks: list[dict] | None = None,
):
    """Factory for unpartitioned gold assets that depend on ALL operator partitions."""
    dep_list = []
    for dep in dep_configs:
        dep_name = dep["asset"]
        if dep.get("all_partitions", False):
            dep_list.append(
                dg.AssetDep(dep_name, partition_mapping=dg.AllPartitionMapping())
            )
        else:
            dep_list.append(dg.AssetDep(dep_name))

    @dg.asset(
        name=name,
        group_name="gold",
        kinds=kinds,
        tags={"layer": "gold", "dbt_cloud_job_id": str(dbt_cloud_job_id)},
        owners=owners,
        deps=dep_list,
        description=description,
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        full_table = f"{catalog}.{schema}.{name}"

        context.log.info(
            f"[DEMO MODE] dbt Cloud job {dbt_cloud_job_id}:\n"
            f"  dbt build --select {dbt_select}\n"
            f"  Reads ALL operator partitions\n"
            f"  Target: {full_table}"
        )

        return dg.MaterializeResult(
            metadata={
                "table": dg.MetadataValue.text(full_table),
                "operators_included": dg.MetadataValue.int(25),
                "row_count": dg.MetadataValue.int(random.randint(500, 500000)),
                "dbt_cloud_job_id": dg.MetadataValue.int(dbt_cloud_job_id),
            }
        )

    check_defs = []
    if checks:
        for chk in checks:
            check_name = chk["name"]
            check_desc = chk.get("description", f"Check {check_name} on {name}")

            @dg.asset_check(name=check_name, asset=_asset, description=check_desc)
            def _check(
                context: AssetCheckExecutionContext,
                _desc=check_desc,
            ) -> dg.AssetCheckResult:
                context.log.info(f"[DEMO MODE] {_desc}")
                return dg.AssetCheckResult(passed=True)

            check_defs.append(_check)

    return _asset, check_defs


class DbtCloudMultiOperatorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Orchestrates dbt Cloud across multiple operators (countries) with Bronze/Silver/Gold layers.

    Each operator is a static partition. Bronze and Silver assets are partitioned —
    each materialization triggers a dbt Cloud job with operator-specific variables.
    Gold assets aggregate across ALL operator partitions using AllPartitionMapping.

    Concurrency is controlled via a configurable key and max_concurrency on jobs.
    """

    # ── Configuration fields (map to YAML schema) ─────────
    project_name: str = "default"
    operators: list[str]
    dbt_cloud_account_id: int = 0
    dbt_cloud_api_token: str = "{{ env.DBT_CLOUD_API_TOKEN }}"
    databricks_catalog: str = "hooli_prod"
    concurrency_key: str = "dbt_cloud"
    max_concurrency: int = 5
    owners: list[str] = ["team:data-engineering"]
    kinds: list[str] = ["databricks", "dbt-cloud", "delta"]

    # Layer definitions
    bronze: list[dict] = []
    silver: list[dict] = []
    gold: list[dict] = []

    # Automation
    schedules: list[dict] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = dg.StaticPartitionsDefinition(self.operators)
        kind_set = set(self.kinds)
        all_assets: list = []
        all_checks: list = []
        all_jobs: list = []
        all_schedules: list = []
        all_sensors: list = []

        # ── Bronze layer ──────────────────────────────────
        for model in self.bronze:
            dep_keys = [dg.AssetKey(d) for d in model.get("deps", [])]
            asset_def, checks = _make_partitioned_asset(
                name=model["name"],
                layer="bronze",
                partitions_def=partitions_def,
                dep_keys=dep_keys,
                description=model.get("description", f"Bronze table: {model['name']}"),
                dbt_cloud_job_id=model.get("dbt_cloud_job_id", 0),
                dbt_select=model.get("dbt_select", model["name"]),
                catalog=self.databricks_catalog,
                schema=model.get("schema", "bronze"),
                concurrency_key=self.concurrency_key,
                kinds=kind_set,
                owners=self.owners,
                checks=model.get("checks"),
            )
            all_assets.append(asset_def)
            all_checks.extend(checks)

        # ── Silver layer ──────────────────────────────────
        for model in self.silver:
            dep_keys = [dg.AssetKey(d) for d in model.get("deps", [])]
            automation = None
            if model.get("eager", False):
                automation = dg.AutomationCondition.eager()

            asset_def, checks = _make_partitioned_asset(
                name=model["name"],
                layer="silver",
                partitions_def=partitions_def,
                dep_keys=dep_keys,
                description=model.get("description", f"Silver model: {model['name']}"),
                dbt_cloud_job_id=model.get("dbt_cloud_job_id", 0),
                dbt_select=model.get("dbt_select", model["name"]),
                catalog=self.databricks_catalog,
                schema=model.get("schema", "silver"),
                concurrency_key=self.concurrency_key,
                kinds=kind_set,
                owners=model.get("owners", self.owners),
                automation=automation,
                checks=model.get("checks"),
            )
            all_assets.append(asset_def)
            all_checks.extend(checks)

        # ── Gold layer ────────────────────────────────────
        for model in self.gold:
            asset_def, checks = _make_gold_asset(
                name=model["name"],
                dep_configs=model.get("deps", []),
                description=model.get("description", f"Gold model: {model['name']}"),
                dbt_cloud_job_id=model.get("dbt_cloud_job_id", 0),
                dbt_select=model.get("dbt_select", model["name"]),
                catalog=self.databricks_catalog,
                schema=model.get("schema", "gold"),
                kinds=kind_set,
                owners=model.get("owners", ["team:analytics"]),
                checks=model.get("checks"),
            )
            all_assets.append(asset_def)
            all_checks.extend(checks)

        # ── Jobs & Schedules ──────────────────────────────
        for sched in self.schedules:
            layers = sched.get("layers", [])
            selection = dg.AssetSelection.groups(*layers)

            job = dg.define_asset_job(
                name=sched["job_name"],
                selection=selection,
                tags={"dagster/max_concurrency": str(self.max_concurrency)},
                description=sched.get("description", ""),
                partitions_def=partitions_def if any(
                    l in ("bronze", "silver") for l in layers
                ) else None,
            )
            all_jobs.append(job)

            if sched.get("cron"):
                schedule_def = dg.ScheduleDefinition(
                    job=job,
                    cron_schedule=sched["cron"],
                    execution_timezone=sched.get("timezone", "UTC"),
                    description=sched.get("description", ""),
                    default_status=dg.DefaultScheduleStatus.RUNNING,
                )
                all_schedules.append(schedule_def)

        # ── Failure sensor ────────────────────────────────
        if all_jobs:
            monitored = [j for j in all_jobs]

            @dg.run_status_sensor(
                name=f"{self.project_name}_failure_alert",
                monitored_jobs=monitored,
                run_status=dg.DagsterRunStatus.FAILURE,
                description="Alert when any operator partition fails",
            )
            def _failure_sensor(context: dg.RunStatusSensorContext):
                run = context.dagster_run
                partition = run.tags.get("dagster/partition", "unknown")
                context.log.info(
                    f"[DEMO MODE] ALERT: Operator {partition} failed "
                    f"(run {run.run_id})"
                )

            all_sensors.append(_failure_sensor)

        return dg.Definitions(
            assets=all_assets,
            asset_checks=all_checks if all_checks else None,
            jobs=all_jobs if all_jobs else None,
            schedules=all_schedules if all_schedules else None,
            sensors=all_sensors if all_sensors else None,
        )
