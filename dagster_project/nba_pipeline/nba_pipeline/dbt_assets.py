from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent.parent / "dbt_project"

nba_dbt_project = DbtProject(project_dir=DBT_PROJECT_PATH)
nba_dbt_project.prepare_if_dev()


@dbt_assets(manifest=nba_dbt_project.manifest_path)
def nba_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
