from dagster import Definitions, asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
from pathlib import Path
import os

# Get the dbt project directory
CURRENT_DIR = Path(__file__).parent
DBT_PROJECT_PATH = CURRENT_DIR.parent / "dbt_project"

@asset
def raw_data_asset():
    """Sample raw data asset that represents upstream data"""
    return {"message": "Raw data loaded", "count": 5}

# First, ensure dbt manifest exists
def ensure_dbt_manifest():
    """Ensure dbt manifest exists by running dbt parse"""
    import subprocess
    result = subprocess.run(
        ["dbt", "parse"], 
        cwd=DBT_PROJECT_PATH,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Warning: dbt parse failed: {result.stderr}")

# Generate manifest
ensure_dbt_manifest()

# Check if manifest exists
manifest_path = DBT_PROJECT_PATH / "target" / "manifest.json"

if manifest_path.exists():
    # Use dbt build command which runs seed + run + test in correct order
    @dbt_assets(
        manifest=manifest_path,
        project=DbtProject(project_dir=DBT_PROJECT_PATH)
    )
    def dbt_analytics_models(context: AssetExecutionContext, dbt: DbtCliResource):
        """
        Execute all dbt operations (seed, run, test) in correct order
        """
        yield from dbt.cli(["build"], context=context).stream()
    
    assets_list = [raw_data_asset, dbt_analytics_models]
else:
    # Fallback to simple subprocess approach
    @asset
    def dbt_models(context: AssetExecutionContext, raw_data_asset):
        """Run dbt models using subprocess"""
        import subprocess
        
        context.log.info("Running dbt build (seed + run + test)...")
        result = subprocess.run(
            ["dbt", "build"], 
            cwd=DBT_PROJECT_PATH, 
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            context.log.error(f"dbt build failed: {result.stderr}")
            raise Exception(f"dbt build failed: {result.stderr}")
        
        context.log.info("dbt build completed successfully")
        return {"status": "completed", "stdout": result.stdout}
    
    assets_list = [raw_data_asset, dbt_models]

# Create the definitions
repository = Definitions(
    assets=assets_list,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_PATH))
    }
)