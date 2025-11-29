from dagster import Definitions
from .defs.football.ingestion import (
    raw_players_data,
    raw_teams_data,
    raw_playerstats_data,
    raw_playermatchstats_data,
    raw_matches_data,
)
from .defs.football.validation import (
    validated_players_data,
    validated_teams_data,
    validated_playerstats_data,
    validated_playermatchstats_data,
)

# from .defs.football.asset_checks import (
#     check_players_csv_non_empty,
#     check_players_csv_headers,
#     check_players_schema,
#     check_number_of_teams,
# )
from .defs.resources.io_manager import minio_io_manager
from .defs.football.config import minio_config


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
        raw_matches_data,
    ],
    # asset_checks=[
    #     check_players_csv_non_empty,
    #     check_players_csv_headers,
    #     check_players_schema,
    #     check_number_of_teams,
    # ],
    resources={
        "io_manager": minio_io_manager.configured(
            {
                # "endpoint": minio_config.endpoint_url,
                "endpoint": "minio:9000",
                "access_key": minio_config.access_key,
                "secret_key": minio_config.secret_key,
                "bucket_name": minio_config.main_bucket,
                "secure": minio_config.secure,
            }
        )
    },
)
