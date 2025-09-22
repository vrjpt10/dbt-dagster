from dagster import asset, Output, AssetMaterialization
import pandas as pd
from datetime import datetime


@asset(
    description="Hello World asset - generates sample data"
)
def hello_world_data():
    """
    A simple asset that generates hello world data.
    This is our first Dagster asset!
    """
    # Create sample data
    data = {
        'id': [1, 2, 3, 4, 5],
        'message': ['Hello', 'World', 'From', 'Dagster', 'Asset'],
        'timestamp': [datetime.now() for _ in range(5)]
    }

    df = pd.DataFrame(data)

    # Log materialization with metadata
    yield AssetMaterialization(
        asset_key="hello_world_data",
        metadata={
            "num_records": len(df),
            "columns": df.columns.tolist(),
            "preview": df.head().to_dict()
        }
    )

    # Return the dataframe
    yield Output(df)


@asset(
    deps=[hello_world_data],
    description="Transforms hello world data"
)
def transformed_hello_data(hello_world_data: pd.DataFrame):
    """
    Transform the hello world data by adding a new column.
    This demonstrates asset dependencies.
    """
    # Add a new column
    df = hello_world_data.copy()
    df['message_length'] = df['message'].str.len()
    df['message_upper'] = df['message'].str.upper()

    yield AssetMaterialization(
        asset_key="transformed_hello_data",
        metadata={
            "num_records": len(df),
            "new_columns": ["message_length", "message_upper"],
            "preview": df.head().to_dict()
        }
    )

    yield Output(df)