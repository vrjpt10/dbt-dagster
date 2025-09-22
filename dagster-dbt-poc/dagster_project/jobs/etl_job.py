from dagster import define_asset_job, AssetSelection

# Define job that includes both Python and dbt assets
daily_etl_job = define_asset_job(
    name="daily_etl_job",
    selection=AssetSelection.groups("staging", "marts") | AssetSelection.keys([
        "source_users_data",
        "transformed_users",
        "target_users_data",
        "user_metrics"
    ]),
    description="Daily ETL job that extracts, transforms, and loads user data"
)

# Define job for only Python transformations
python_transform_job = define_asset_job(
    name="python_transform_job",
    selection=AssetSelection.keys([
        "source_users_data",
        "transformed_users",
        "user_metrics"
    ]),
    description="Job that only runs Python transformations"
)

# Define job for only dbt models
dbt_transform_job = define_asset_job(
    name="dbt_transform_job",
    selection=AssetSelection.groups("staging", "marts"),
    description="Job that only runs dbt transformations"
)