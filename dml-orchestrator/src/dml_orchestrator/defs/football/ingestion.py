from dagster import asset, AssetExecutionContext
from typing import Dict, Any
from botocore.exceptions import ClientError

from .config import github_config, minio_config

from ..utils.ingest_csv import get_csv

from .partitions import gameweek_partitions


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
        context.log.error(f"Failed to upload file to MinIO: {e}")
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
    description="Raw playerstats CSV file from GitHub",
    group_name="football_ingestion",
    required_resource_keys={"minio"},
)
def raw_playerstats_data(context: AssetExecutionContext) -> bytes:
    """
    Raw playerstats CSV from GitHub.
    """
    csv_path = github_config.data_files["playerstats"]
    url = f"{github_config.base_url}{csv_path}"

    context.log.info(f"Downloading playerstats CSV from {url}")
    csv_bytes = get_csv(url)

    minio = context.resources.minio
    minio_key = f"{minio_config.raw_data_path}/{github_config.season}/playerstats.csv"

    try:
        context.log.info(
            f"Uploading to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )

        minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=csv_bytes)

        context.log.info(
            f"Uploaded playerstats CSV to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )
    except ClientError as e:
        context.log.error(f"Failed to upload file to MinIO: {e}")
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
    description="raw playermatchstats data in CSV format",
    group_name="football_ingestion",
    required_resource_keys={"minio"},
    partitions_def=gameweek_partitions,
)
def raw_playermatchstats_data(context: AssetExecutionContext) -> bytes:
    season = github_config.season
    gw = context.partition_key

    url = github_config.build_gw_url("playermatchstats", gw_override=gw)

    context.log.info(f"Downloading playermatchstats CSV from {url}")
    csv_bytes = get_csv(url)

    minio = context.resources.minio
    minio_key = minio_config.build_raw_gw_key("playermatchstats", season, gw)

    try:
        context.log.info(
            f"Uploading to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )

        minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=csv_bytes)

        context.log.info(
            f"Uploaded playermatchstats CSV to MinIO bucket '{minio_config.main_bucket}' at key '{minio_key}'"
        )
    except Exception as e:
        context.log.error(f"Failed to upload file to MinIO: {e}")
        raise

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "bytes_downloaded": len(csv_bytes),
        "source_url": url,
    }

    context.add_output_metadata(metadata)

    return csv_bytes
