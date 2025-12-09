from typing import Any
from dagster import asset, AssetExecutionContext

from .config import github_config, minio_config

from ..utils.ingest_csv import get_csv

from .partitions import gameweek_partitions


@asset(
    description="Raw players data fetched from GitHub (season and gameweek aware)",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    metadata={
        "path_prefix": lambda context: minio_config.build_raw_gw_key(
            "players", github_config.season, context.partition_key
        ),
        "file_extension": "csv",
    },
)
def raw_players_data(context: AssetExecutionContext) -> bytes:

    season = github_config.season

    gw = context.partition_key

    url = github_config.build_gw_url("players", gw_override=gw)

    context.log.info(f"Downloading players CSV from {url}")

    csv_bytes = get_csv(url)

    metadata = {
        "source_url": url,
        "season": season,
        "gameweek": gw,
    }

    context.add_output_metadata(metadata)

    return csv_bytes


@asset(
    description="Ingests the raw teams CSV file from GitHub",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    metadata={
        "path_prefix": lambda context: minio_config.build_raw_gw_key(
            "teams", github_config.season, context.partition_key
        ),
        "file_extension": "csv",
    },
)
def raw_teams_data(context: AssetExecutionContext) -> bytes:
    season = github_config.season

    gw = context.partition_key

    url = github_config.build_gw_url("teams", gw_override=gw)

    context.log.info(f"Ingesting teams data from {url}")

    csv_bytes = get_csv(url)

    context.add_output_metadata(
        {
            "source_url": url,
            "season": season,
            "gameweek": gw,
        }
    )

    return csv_bytes


@asset(
    description="Raw playerstats CSV file from GitHub",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    metadata={
        "path_prefix": lambda context: minio_config.build_raw_gw_key(
            "playerstats", github_config.season, context.partition_key
        ),
        "file_extension": "csv",
    },
)
def raw_playerstats_data(context: AssetExecutionContext) -> bytes:
    season = github_config.season

    gw = context.partition_key

    url = github_config.build_gw_url("playerstats", gw_override=gw)

    context.log.info(f"Downloading playerstats CSV from {url}")
    csv_bytes = get_csv(url)

    context.add_output_metadata(
        {
            "source_url": url,
            "season": season,
            "gameweek": gw,
        }
    )

    return csv_bytes


@asset(
    description="raw playermatchstats data in CSV format",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    metadata={
        "path_prefix": lambda context: minio_config.build_raw_gw_key(
            "playermatchstats", github_config.season, context.partition_key
        ),
        "file_extension": "csv",
    },
)
def raw_playermatchstats_data(
    context: AssetExecutionContext,
) -> bytes:
    season = github_config.season
    gw = context.partition_key

    url = github_config.build_gw_url("playermatchstats", gw_override=gw)

    context.log.info(f"Downloading playermatchstats CSV from {url}")

    csv_bytes = get_csv(url)

    context.add_output_metadata(
        {
            "source_url": url,
            "season": season,
            "gameweek": gw,
        }
    )

    return csv_bytes


@asset(
    description="Raw matches data fetched from GitHub (season and gameweek aware)",
    group_name="football_ingestion",
    partitions_def=gameweek_partitions,
    metadata={
        "path_prefix": lambda context: minio_config.build_raw_gw_key(
            "matches", github_config.season, context.partition_key
        ),
        "file_extension": "csv",
    },
)
def raw_matches_data(context: AssetExecutionContext) -> bytes:

    season = github_config.season

    gw = context.partition_key

    url = github_config.build_gw_url("matches", gw_override=gw)

    context.log.info(f"Downloading matches CSV from {url}")

    csv_bytes = get_csv(url)

    context.add_output_metadata(
        {
            "source_url": url,
            "season": season,
            "gameweek": gw,
        }
    )

    return csv_bytes
