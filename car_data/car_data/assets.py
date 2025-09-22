import dagster as dg
import polars as pl
import duckdb

CSV_URL = 'https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv'
CSV_PATH = 'data/carprice.csv'

DUCKDB_PATH = 'data/car_data.duckdb'
TABLE_NAME = 'avg_price_per_brand'

@dg.asset
def car_data_file(context: dg.AssetExecutionContext):
    """Download car data."""
    context.log.info('Downloading car data.')
    df = pl.read_csv(CSV_URL)
    df = df.with_columns([
        pl.col('normalized-losses').cast(pl.Float64, strict = False),
        pl.col('price').cast(pl.Float64, strict=False)
    ])
    df.write_csv(CSV_PATH)

@dg.asset(deps=[car_data_file])
def avg_price_table(context: dg.AssetExecutionContext):
    """Compute average price for each brand."""
    context.log.info('Computing average price for each brand.')
    df = pl.read_csv(CSV_PATH)
    df = df.drop_nulls(["price"])
    avg_price_df = df.group_by(["make"]).agg(
        pl.col("price").mean().alias("avg_price")
    )

    data = [(row["make"], row["avg_price"]) for row in avg_price_df.to_dicts()]

    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(f"CREATE TABLE {TABLE_NAME} (make TEXT, avg_price DOUBLE)")
        con.executemany(f"INSERT INTO {TABLE_NAME} (make, avg_price) VALUES (?, ?)", data)