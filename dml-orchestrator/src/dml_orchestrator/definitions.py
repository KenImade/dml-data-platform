from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from .defs.football import ingestion
from .defs.football.asset_checks import raw_player_data_checks, raw_teams_data_checks
from .defs.resources.io_manager import minio_io_manager
from .defs.football.config import minio_config


defs = Definitions(
    assets=load_assets_from_modules([ingestion]),
    # asset_checks=load_asset_checks_from_modules(
    #     [raw_player_data_checks, raw_teams_data_checks]
    # ),
    asset_checks=load_asset_checks_from_modules(
        [raw_player_data_checks, raw_teams_data_checks]
    ),
    resources={
        "io_manager": minio_io_manager.configured(
            {
                "endpoint": minio_config.endpoint_url,
                "access_key": minio_config.access_key,
                "secret_key": minio_config.secret_key,
                "bucket_name": minio_config.main_bucket,
                "secure": minio_config.secure,
            }
        )
    },
)
