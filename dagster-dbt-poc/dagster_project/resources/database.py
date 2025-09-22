from dagster import ConfigurableResource
from sqlalchemy import create_engine
import pandas as pd
from typing import Optional


class PostgresResource(ConfigurableResource):
    """Resource for PostgreSQL database connections."""

    host: str
    port: int
    username: str
    password: str
    database: str

    def get_connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    def execute_query(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        engine = create_engine(self.get_connection_string())
        return pd.read_sql(query, engine, params=params)

    def write_dataframe(self, df: pd.DataFrame, table_name: str, schema: str, if_exists: str = "replace"):
        """Write a DataFrame to PostgreSQL."""
        engine = create_engine(self.get_connection_string())
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False
        )


class DuckDBResource(ConfigurableResource):
    """Resource for DuckDB connections."""

    database_path: str

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        import duckdb
        conn = duckdb.connect(self.database_path)
        return conn.execute(query).fetchdf()

    def write_dataframe(self, df: pd.DataFrame, table_name: str):
        """Write a DataFrame to DuckDB."""
        import duckdb
        conn = duckdb.connect(self.database_path)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        conn.close()