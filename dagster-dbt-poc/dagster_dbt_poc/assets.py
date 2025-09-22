import os
from pathlib import Path
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Get the directory that contains this file
CURRENT_DIR = Path(__file__).parent
DBT_PROJECT_PATH = CURRENT_DIR.parent / "dbt_project"

# Simple asset first
@asset
def raw_data_asset():
    """
    Represents raw data that feeds into our dbt models.
    """
    return {"status": "Raw data loaded", "record_count": 5}

# dbt assets - let's create a simpler version first
@asset
def dbt_analytics_assets(context: AssetExecutionContext):
    """
    Execute dbt models and return status.
    """
    try:
        # Create dbt resource
        dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_PATH))
        
        # Run dbt commands
        context.log.info("Running dbt seed...")
        seed_result = dbt_resource.cli(["seed"], context=context)
        
        context.log.info("Running dbt run...")
        run_result = dbt_resource.cli(["run"], context=context)
        
        return {
            "status": "dbt models executed successfully",
            "seed_success": seed_result.success,
            "run_success": run_result.success
        }
    except Exception as e:
        context.log.error(f"dbt execution failed: {e}")
        raise