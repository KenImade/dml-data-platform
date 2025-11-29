from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue
from typing import Dict, Any

from ..utils.validate_csv import validate_csv
from ..utils.parquet import dataframe_to_parquet_bytes

from .config import github_config, minio_config

from .models import Player, Team, PlayerStats, PlayerMatchStats


@asset(
    description="Validated players data",
    group_name="football_ingestion",
    ins={"raw_players_data": AssetIn()},
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
    description="Validated teams data",
    group_name="football_ingestion",
    ins={"raw_teams_data": AssetIn()},
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


@asset(
    description="Validation of playerstats data",
    group_name="football_ingestion",
    ins={"raw_playerstats_data": AssetIn()},
)
def validated_playerstats_data(
    context: AssetExecutionContext, raw_playerstats_data: bytes
) -> bytes:
    minio = context.resources.minio

    try:
        df = validate_csv(raw_playerstats_data, PlayerStats)
    except Exception as e:
        context.log.error(f"Validation failed: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    minio_key = (
        f"{minio_config.staging_data_path}/"
        f"{github_config.season}/playerstats.parquet"
    )
    minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=parquet_bytes)

    context.log.info(
        f"Uploaded validated Parquet to s3://{minio_config.main_bucket}/{minio_key}"
    )

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
        "playerstats_count": df.height,
        "playerstats_preview": MetadataValue.md(
            df.head(10).to_pandas().to_markdown(index=False)
        ),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes


@asset(
    description="Validated playermatchstats data",
    group_name="football_ingestion",
    ins={"raw_playermatchstats_data": AssetIn()},
)
def validated_playermatchstats_data(
    context: AssetExecutionContext, raw_playermatchstats_data: bytes
) -> bytes:
    minio = context.resources.minio

    try:
        df = validate_csv(raw_playermatchstats_data, PlayerMatchStats)
        context.log.info(f"Validated {df.height} player match stats")
    except Exception as e:
        context.log.error(f"Failed to validate raw player match stats data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    minio_key = (
        f"{minio_config.staging_data_path}/"
        f"{github_config.season}/"
        f"{github_config.gameweek}/"
        f"playermatchstats.csv"
    )

    minio.put_object(bucket=minio_config.main_bucket, key=minio_key, body=parquet_bytes)

    context.log.info(
        f"Uploaded validated parquet to s3://{minio_config.main_bucket}/{minio_key}"
    )

    metadata: Dict[str, Any] = {
        "minio_bucket": minio_config.main_bucket,
        "minio_key": minio_key,
    }

    context.add_output_metadata(metadata)
    return parquet_bytes
