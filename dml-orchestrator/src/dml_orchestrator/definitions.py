from dagster import Definitions
from .defs.resources.minio import MinIOResource
from .defs.football.ingestion import (
    raw_players_data,
    raw_teams_data,
    raw_playerstats_data,
    raw_playermatchstats_data,
)
from .defs.football.validation import (
    validated_players_data,
    validated_teams_data,
    validated_playerstats_data,
    validated_playermatchstats_data,
)
from .defs.football.asset_checks import (
    check_players_csv_non_empty,
    check_players_csv_headers,
    check_players_schema,
    check_number_of_teams,
)
from .defs.football.config import settings


defs = Definitions(
    assets=[
        raw_players_data,
        validated_players_data,
        raw_teams_data,
        validated_teams_data,
        raw_playerstats_data,
        validated_playerstats_data,
        raw_playermatchstats_data,
        validated_playermatchstats_data,
    ],
    asset_checks=[
        check_players_csv_non_empty,
        check_players_csv_headers,
        check_players_schema,
        check_number_of_teams,
    ],
    resources={
        "minio": MinIOResource(
            endpoint=settings.minio.endpoint_url,
            access_key=settings.minio.access_key,
            secret_key=settings.minio.secret_key,
            region_name=settings.minio.region,
        ),
        # "dbt": get_dbt_resource(),
    },
)
