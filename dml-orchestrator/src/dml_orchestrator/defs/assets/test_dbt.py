from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
)

# Path to your dbt project manifest
DBT_PROJECT_DIR = Path("/opt/dbt/project")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=DagsterDbtTranslator(),
)
def dml_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    All dbt models as Dagster assets
    Transform raw NYC taxi data into analytics-ready metrics
    """
    yield from dbt.cli(["build"], context=context).stream()
