"""Shared constants for the multi-operator dbt Cloud pipeline."""

import dagster as dg

# 25 operators (countries) to process
OPERATORS = [
    "BE", "NL", "DE", "FR", "UK", "ES", "IT", "PT", "GR", "CY",
    "RO", "BG", "CZ", "PL", "HU", "SK", "SI", "HR", "RS", "BA",
    "ME", "MK", "AL", "XK", "MD",
]

operators_partitions = dg.StaticPartitionsDefinition(OPERATORS)

# dbt Cloud configuration
DBT_CLOUD_ACCOUNT_ID = 12345
DBT_CLOUD_PROJECT_ID = 67890

# Job IDs for each layer
DBT_CLOUD_BRONZE_JOB_ID = 100001
DBT_CLOUD_SILVER_JOB_ID = 100002
DBT_CLOUD_GOLD_JOB_ID = 100003

# Databricks configuration
DATABRICKS_CATALOG = "hooli_prod"
DATABRICKS_BRONZE_SCHEMA = "bronze"
DATABRICKS_SILVER_SCHEMA = "silver"
DATABRICKS_GOLD_SCHEMA = "gold"
