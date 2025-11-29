from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue
from typing import Dict, Any

from .partitions import gameweek_partitions

from ..utils.validate_csv import validate_csv
from ..utils.parquet import dataframe_to_parquet_bytes

from .config import github_config, minio_config

from .models import Player, Team, PlayerStats, PlayerMatchStats, Matches


@asset(
    description="Validated players data",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    ins={"raw_players_data": AssetIn()},
    metadata={
        "path_prefix": lambda context: minio_config.build_staging_gw_key(
            "players", github_config.season, context.partition_key
        ),
        "file_extension": "parquet",
    },
)
def validated_players_data(
    context: AssetExecutionContext, raw_players_data: bytes
) -> bytes:

    context.log.info("Validating raw players data")

    try:
        df = validate_csv(raw_players_data, Player)
        context.log.info(f"Validated {df.height} players")
    except Exception as e:
        context.log.error(f"Failed to validate raw players data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    context.log.info(
        f"Uploaded validated Parquet to "
        f"{minio_config.main_bucket}/{minio_config.build_staging_gw_key(
            "players", github_config.season, context.partition_key)}"
    )

    metadata: Dict[str, Any] = {
        "players_count": df.height,
        "players_preview": MetadataValue.md(df.head(10).write_csv()),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes


@asset(
    description="Validated teams data",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    ins={"raw_teams_data": AssetIn()},
    metadata={
        "path_prefix": lambda context: minio_config.build_staging_gw_key(
            "teams", github_config.season, context.partition_key
        ),
        "file_extension": "parquet",
    },
)
def validated_teams_data(
    context: AssetExecutionContext, raw_teams_data: bytes
) -> bytes:

    context.log.info("Validating Raw Teams Data")

    try:
        df = validate_csv(raw_teams_data, Team)
        context.log.info(f"Validated {df.height} teams")
    except Exception as e:
        context.log.error(f"Failed to validate raw teams data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    context.log.info(
        f"Uploaded validated Parquet to "
        f"{minio_config.main_bucket}/{minio_config.build_staging_gw_key(
            "teams", github_config.season, context.partition_key
        )}"
    )

    metadata: Dict[str, Any] = {
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
    partitions_def=gameweek_partitions,
    ins={"raw_playerstats_data": AssetIn()},
    metadata={
        "path_prefix": lambda context: minio_config.build_staging_gw_key(
            "playerstats", github_config.season, context.partition_key
        ),
        "file_extension": "parquet",
    },
)
def validated_playerstats_data(
    context: AssetExecutionContext, raw_playerstats_data: bytes
) -> bytes:
    context.log.info("Validating raw playerstats data")

    try:
        df = validate_csv(raw_playerstats_data, PlayerStats)
        context.log.info(f"Validated {df.height} playerstats")
    except Exception as e:
        context.log.error(f"Failed to validate raw playerstats data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    context.log.info(
        f"Uploaded validated Parquet to "
        f"{minio_config.main_bucket}/{minio_config.build_staging_gw_key(
            "playerstats", github_config.season, context.partition_key)}"
    )

    metadata: Dict[str, Any] = {
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
    partitions_def=gameweek_partitions,
    ins={"raw_playermatchstats_data": AssetIn()},
    metadata={
        "path_prefix": lambda context: minio_config.build_staging_gw_key(
            "playermatchstats", github_config.season, context.partition_key
        ),
        "file_extension": "parquet",
    },
)
def validated_playermatchstats_data(
    context: AssetExecutionContext, raw_playermatchstats_data: bytes
) -> bytes:
    context.log.info("Validating raw player match stats data")

    try:
        df = validate_csv(raw_playermatchstats_data, PlayerMatchStats)
        context.log.info(f"Validated {df.height} player match stats")
    except Exception as e:
        context.log.error(f"Failed to validate raw player match stats data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    context.log.info(
        f"Uploaded validated Parquet to "
        f"{minio_config.main_bucket}/{minio_config.build_staging_gw_key(
            "playermatchstats", github_config.season, context.partition_key)}"
    )

    metadata: Dict[str, Any] = {
        "playermatchstats_count": df.height,
        "playermatchstats_preview": MetadataValue.md(
            df.head(10).to_pandas().to_markdown(index=False)
        ),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes


@asset(
    description="Validated matches data",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    ins={"raw_matches_data": AssetIn()},
    metadata={
        "path_prefix": lambda context: minio_config.build_staging_gw_key(
            "matches", github_config.season, context.partition_key
        ),
        "file_extension": "parquet",
    },
)
def validated_matches_data(
    context: AssetExecutionContext, raw_matches_data: bytes
) -> bytes:
    context.log.info("Validating raw player match stats data")

    try:
        df = validate_csv(raw_matches_data, Matches)
        context.log.info(f"Validated {df.height} matches")
    except Exception as e:
        context.log.error(f"Failed to validate raw player match stats data: {e}")
        raise

    parquet_bytes = dataframe_to_parquet_bytes(df)

    context.log.info(
        f"Uploaded validated Parquet to "
        f"{minio_config.main_bucket}/{minio_config.build_staging_gw_key(
            "matches", github_config.season, context.partition_key)}"
    )

    metadata: Dict[str, Any] = {
        "matches_count": df.height,
        "matches_preview": MetadataValue.md(
            df.head(10).to_pandas().to_markdown(index=False)
        ),
    }

    context.add_output_metadata(metadata)
    return parquet_bytes
