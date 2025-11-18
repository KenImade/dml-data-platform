import io
import requests
import polars as pl
import duckdb
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from ..resources.minio import MinIOResource


@asset(
    group_name="nyc_taxi",
    compute_kind="python"
)
def download_nyc_taxi_data(context: AssetExecutionContext) -> Output[bytes]:
    """
    Download NYC Yellow Taxi trip data from public dataset (January 2023)
    """
    # NYC Taxi & Limousine Commission public data
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

    context.log.info(f"Downloading NYC taxi data from {url}")

    response = requests.get(url, timeout=300)
    response.raise_for_status()

    data = response.content
    file_size_mb = len(data) / (1024 * 1024)

    context.log.info(f"Downloaded {file_size_mb:.2f} MB of data")

    return Output(
        value=data,
        metadata={
            "file_size_mb": file_size_mb,
            "source_url": url,
            "file_format": "parquet"
        }
    )


@asset(
    group_name="nyc_taxi",
    compute_kind="minio"
)
def upload_taxi_data_to_minio(
    context: AssetExecutionContext,
    minio: MinIOResource,
    download_nyc_taxi_data: bytes
) -> Output[str]:
    """
    Upload the downloaded taxi data to MinIO and return the S3 key
    """
    key = "raw/nyc_taxi/yellow_tripdata_2023-01.parquet"

    context.log.info(f"Uploading data to MinIO: {key}")

    minio.put_object(key, download_nyc_taxi_data)

    context.log.info(f"Successfully uploaded to MinIO bucket: {minio.bucket}")

    return Output(
        value=key,
        metadata={
            "bucket": minio.bucket,
            "key": key,
            "size_mb": len(download_nyc_taxi_data) / (1024 * 1024)
        }
    )


@asset(
    group_name="nyc_taxi",
    compute_kind="duckdb"
)
def load_taxi_data_to_duckdb(
    context: AssetExecutionContext,
    minio: MinIOResource,
    upload_taxi_data_to_minio: str
) -> Output[int]:
    """
    Load NYC taxi data from MinIO into DuckDB raw table using Polars
    """
    # Get data from MinIO using the S3 key
    s3_client = minio.get_client()
    context.log.info(f"Fetching data from MinIO: {upload_taxi_data_to_minio}")

    response = s3_client.get_object(Bucket=minio.bucket, Key=upload_taxi_data_to_minio)
    parquet_data = response['Body'].read()

    context.log.info("Reading parquet data with Polars")
    df = pl.read_parquet(io.BytesIO(parquet_data))

    # Connect to DuckDB
    db_path = "/opt/dbt/project/data/warehouse/warehouse.duckdb"
    context.log.info(f"Connecting to DuckDB at {db_path}")

    conn = duckdb.connect(db_path)

    # Create raw schema if it doesn't exist
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Drop existing table and create new one
    conn.execute("DROP TABLE IF EXISTS raw.nyc_taxi_trips")

    # Load data - DuckDB can directly query Polars DataFrames
    context.log.info("Loading data into DuckDB")
    conn.execute("CREATE TABLE raw.nyc_taxi_trips AS SELECT * FROM df")

    # Get row count
    row_count = conn.execute("SELECT COUNT(*) FROM raw.nyc_taxi_trips").fetchone()[0]

    # Get sample data for preview (as JSON instead of markdown)
    sample_data = conn.execute("SELECT * FROM raw.nyc_taxi_trips LIMIT 5").pl()

    # Get column info
    columns = df.columns
    schema_info = {col: str(df[col].dtype) for col in columns}

    conn.close()

    context.log.info(f"Loaded {row_count:,} rows into raw.nyc_taxi_trips")

    # Format sample data as a simple table string
    sample_preview = f"Sample data (first 5 rows):\n{sample_data}"

    return Output(
        value=row_count,
        metadata={
            "row_count": row_count,
            "column_count": len(columns),
            "table": "raw.nyc_taxi_trips",
            "database": db_path,
            "schema": MetadataValue.json(schema_info),
            "sample_data": MetadataValue.text(sample_preview)
        }
    )
