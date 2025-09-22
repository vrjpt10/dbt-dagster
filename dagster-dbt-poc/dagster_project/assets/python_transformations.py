from dagster import asset, AssetIn, Output, AssetMaterialization
import pandas as pd
from typing import Optional
import hashlib


@asset(
    description="Extract data from source PostgreSQL schema"
)
def source_users_data(postgres: PostgresResource) -> pd.DataFrame:
    """
    Extract users data from source schema.
    This simulates reading from a source system.
    """
    # For POC, we'll create sample data
    # In real scenario, this would be:
    # return postgres.execute_query("SELECT * FROM source_schema.users")

    sample_data = {
        'user_id': range(1, 101),
        'username': [f'user_{i}' for i in range(1, 101)],
        'email': [f'user_{i}@example.com' for i in range(1, 101)],
        'created_at': pd.date_range('2023-01-01', periods=100, freq='D'),
        'country': ['USA', 'UK', 'Canada', 'Australia', 'Germany'] * 20,
        'age': [20 + (i % 50) for i in range(100)],
    }

    df = pd.DataFrame(sample_data)

    yield AssetMaterialization(
        asset_key="source_users_data",
        metadata={
            "row_count": len(df),
            "columns": df.columns.tolist(),
        }
    )

    yield Output(df)


@asset(
    ins={"users_df": AssetIn("source_users_data")},
    description="Transform users data with Python"
)
def transformed_users(users_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to users data using Python.
    This demonstrates complex transformations that might be easier in Python than SQL.
    """
    df = users_df.copy()

    # Add age group
    df['age_group'] = pd.cut(
        df['age'],
        bins=[0, 25, 35, 50, 100],
        labels=['Gen Z', 'Millennial', 'Gen X', 'Boomer']
    )

    # Add email domain
    df['email_domain'] = df['email'].str.split('@').str[1]

    # Add hashed user_id for privacy
    df['user_hash'] = df['user_id'].apply(
        lambda x: hashlib.md5(str(x).encode()).hexdigest()[:8]
    )

    # Add days since registration
    df['days_since_registration'] = (pd.Timestamp.now() - df['created_at']).dt.days

    # Categorize users by activity
    df['user_segment'] = df['days_since_registration'].apply(
        lambda x: 'New' if x < 30 else 'Active' if x < 180 else 'Dormant'
    )

    yield AssetMaterialization(
        asset_key="transformed_users",
        metadata={
            "row_count": len(df),
            "new_columns": ['age_group', 'email_domain', 'user_hash',
                            'days_since_registration', 'user_segment'],
            "segments": df['user_segment'].value_counts().to_dict()
        }
    )

    yield Output(df)


@asset(
    ins={"transformed_df": AssetIn("transformed_users")},
    description="Load transformed data to target PostgreSQL schema"
)
def target_users_data(transformed_df: pd.DataFrame, postgres: PostgresResource):
    """
    Load transformed users data into target schema.
    This completes the ETL pipeline.
    """
    # In a real scenario, you would write to PostgreSQL:
    # postgres.write_dataframe(
    #     df=transformed_df,
    #     table_name="users_transformed",
    #     schema="target_schema",
    #     if_exists="replace"
    # )

    # For POC, we'll simulate the write
    target_path = "./data/target_users.csv"
    transformed_df.to_csv(target_path, index=False)

    yield AssetMaterialization(
        asset_key="target_users_data",
        metadata={
            "row_count": len(transformed_df),
            "target_location": target_path,
            "load_timestamp": pd.Timestamp.now().isoformat()
        }
    )


@asset(
    description="Create aggregated user metrics"
)
def user_metrics(transformed_users: pd.DataFrame) -> pd.DataFrame:
    """
    Create aggregated metrics from transformed users data.
    This demonstrates how to create summary tables.
    """
    # Aggregate by country and age group
    metrics = transformed_users.groupby(['country', 'age_group']).agg({
        'user_id': 'count',
        'days_since_registration': 'mean',
        'age': ['mean', 'min', 'max']
    }).round(2)

    # Flatten column names
    metrics.columns = ['_'.join(col).strip() for col in metrics.columns]
    metrics = metrics.reset_index()
    metrics.rename(columns={'user_id_count': 'user_count'}, inplace=True)

    yield AssetMaterialization(
        asset_key="user_metrics",
        metadata={
            "row_count": len(metrics),
            "countries": metrics['country'].unique().tolist(),
            "age_groups": metrics['age_group'].unique().tolist()
        }
    )

    yield Output(metrics)