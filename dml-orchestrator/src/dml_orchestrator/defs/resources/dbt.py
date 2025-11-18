from pathlib import Path
from dagster_dbt import DbtCliResource


def get_dbt_resource() -> DbtCliResource:
    """
    Configure dbt to use the mounted directory
    """
    dbt_project_dir = Path("/opt/dbt/project")

    return DbtCliResource(
        project_dir=dbt_project_dir,
        profiles_dir=dbt_project_dir
    )