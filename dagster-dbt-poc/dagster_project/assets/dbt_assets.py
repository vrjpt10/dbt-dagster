from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)
from pathlib import Path
import os

# Get the path to dbt project
DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dbt_project"
DBT_PROFILES_PATH = DBT_PROJECT_PATH / "profiles.yml"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Custom translator to control how dbt models are translated to Dagster assets."""

    def get_group_name(self, dbt_resource_props) -> str:
        """Assign dbt models to groups based on their path."""
        path_parts = dbt_resource_props["fqn"]
        if "staging" in path_parts:
            return "staging"
        elif "marts" in path_parts:
            return "marts"
        return "default"


@dbt_assets(
    manifest=DBT_PROJECT_PATH / "target" / "manifest.json",
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROJECT_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    The dbt models for our project.
    This will automatically create Dagster assets for each dbt model.
    """
    yield from dbt.cli(["build"], context=context).stream()