from dagster import asset, AssetIn, AssetExecutionContext, MetadataValue
from typing import Dict, Any
from botocore.exceptions import ClientError

from .config import github_config, minio_config
from ..utils.validate_csv import validate_csv
from ..utils.parquet import dataframe_to_parquet_bytes
from ..utils.ingest_csv import get_csv

from .models import Player, Team


@asset(
    description="Ingests the raw players CSV file from GitHub",
    group_name="football_ingestion",
    required_resource_keys={"minio"},
)
def raw_players_data(
    context: AssetExecutionContext,
) -> bytes:
    """
    Ingests the raw players CSV from GitHub and stores it in the MinIO raw bucket.
    """
    csv_path = github_config.data_files["players"]
    url = f"{github_config.base_url}{csv_path}"

    context.log.info(f"Downloading players CSV from {url}")
    csv_bytes = get_csv(url)

    minio = context.resources.minio
    minio_key = f"{minio_config.raw_data_path}/{github_config.season}/players.csv"

    try:
        context.log.info(
            f"Uploading to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )

        minio.put_object(
            bucket=minio_config.main_bucket,
            key=minio_key,
            body=csv_bytes,
        )

        context.log.info(
            f"Uploaded CSV to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )
    except ClientError as e:
        context.log.info(f"Failed to upload file to MinIO: {e}")
        raise

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "bytes_downloaded": len(csv_bytes),
        "source_url": url,
    }

    context.add_output_metadata(metadata)

    return csv_bytes


@asset(
    description="Validated players data",
    group_name="football_ingestion",
    ins={"raw_players_data": AssetIn()},
    required_resource_keys={"minio"},
)
def validated_players_data(
    context: AssetExecutionContext, raw_players_data: bytes
) -> bytes:
    minio = context.resources.minio

    try:
        df = validate_csv(raw_players_data, Player)
        context.log.info(f"Validated {df.height} players")
    except Exception as e:
        context.log.error(f"Validation failed: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    minio_key = (
        f"{minio_config.staging_data_path}/" f"{github_config.season}/players.parquet"
    )
    minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=parquet_bytes)

    context.log.info(
        f"Uploaded validated Parquet to s3://{minio_config.main_bucket}/{minio_key}"
    )

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "players_count": df.height,
        "players_preview": MetadataValue.md(
            df.head(10).to_pandas().to_markdown(index=False)
        ),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes


@asset(
    description="Ingests the raw teams CSV file from GitHub",
    group_name="football_ingestion",
    required_resource_keys={"minio"},
)
def raw_teams_data(context: AssetExecutionContext) -> bytes:
    """Ingests the raw teams CSV file from GitHub and stores it in MinIO storage"""
    csv_path = github_config.data_files["teams"]
    url = f"{github_config.base_url}{csv_path}"

    context.log.info(f"Ingesting teams data from {url}")
    csv_bytes = get_csv(url)

    minio = context.resources.minio
    minio_key = f"{minio_config.raw_data_path}/{github_config.season}/teams.csv"

    try:
        context.log.info(
            f"Uploading to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )

        minio.put_object(
            bucket=minio_config.main_bucket,
            key=minio_key,
            body=csv_bytes,
        )

        context.log.info(
            f"Uploaded CSV to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )
    except ClientError as e:
        context.log.info(f"Failed to upload file to MinIO: {e}")
        raise

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "bytes_downloaded": len(csv_bytes),
        "source_url": url,
    }

    context.add_output_metadata(metadata)

    return csv_bytes


@asset(
    description="Validated teams data",
    group_name="football_ingestion",
    ins={"raw_teams_data": AssetIn()},
    required_resource_keys={"minio"},
)
def validated_teams_data(
    context: AssetExecutionContext, raw_teams_data: bytes
) -> bytes:

    minio = context.resources.minio

    try:
        df = validate_csv(raw_teams_data, Team)
        context.log.info(f"Validated {df.height} teams")
    except Exception as e:
        context.log.error(f"Failed to validate raw teams data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    minio_key = (
        f"{minio_config.staging_data_path}/" f"{github_config.season}/teams.parquet"
    )

    minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=parquet_bytes)

    context.log.info(
        f"Uploaded validated Parquet to s3://{minio_config.main_bucket}/{minio_key}"
    )

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "teams_count": df.height,
        "teams_preview": MetadataValue.md(
            df.head(10).to_pandas().to_markdown(index=False)
        ),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes
