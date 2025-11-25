import io
from dagster import asset, AssetExecutionContext
import requests
import polars as pl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError
from .config import github_config, minio_config
from .validation import validate_players_csv
from ..utils.parquet import dataframe_to_parquet_bytes


@asset(
    description="Downloads the raw players CSV file from GitHub",
    group_name="football_ingestion",
    required_resource_keys={"minio"},
)
def raw_players_data(
    context: AssetExecutionContext,
) -> bytes:
    """
    Downloads the raw players CSV from GitHub and stores it in the MinIO raw bucket.
    """
    csv_path = github_config.data_files["players"]
    url = f"{github_config.base_url}{csv_path}"
    context.log.info(f"Downloading players CSV from {url}")

    session = requests.Session()
    retries = Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if not response.content:
            raise ValueError("Download file is empty")
        context.log.info(f"Downloaded {len(response.content)} bytes")
    except requests.exceptions.RequestException as e:
        context.log.error(f"Error downloading CSV: {e}")
        raise

    minio = context.resources.minio
    minio_key = f"{minio_config.raw_data_path}/{github_config.season}/players.csv"
    try:
        context.log.info(
            f"Uploading to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )

        minio.put_object(
            bucket=minio_config.main_bucket,
            key=minio_key,
            body=response.content,
        )

        context.log.info(
            f"Uploaded CSV to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )
    except ClientError as e:
        context.log.info(f"Failed to upload file to MinIO: {e}")
        raise

    metadata = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "bytes_downloaded": len(response.content),
        "source_url": url,
    }

    context.add_output_metadata(metadata)

    return response.content


@asset(
    description="Validated players data",
    group_name="football_ingestion",
    deps=[raw_players_data],
    required_resource_keys={"minio"},
)
def validated_players_data(context: AssetExecutionContext, raw_players_data: bytes):
    minio = context.resources.minio

    try:
        df = validate_players_csv(raw_players_data)
        context.log.info(f"Validated {df.height} players")
    except Exception as e:
        context.log.error(f"Validation failed: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    minio_key = (
        f"{minio_config.staging_data_path}/{github_config.season}/players.parquet"
    )
    minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=parquet_bytes)

    context.log.info(
        f"Uploaded validated Parquet to s3://{minio_config.main_bucket}/{minio_key}"
    )
    return parquet_bytes
