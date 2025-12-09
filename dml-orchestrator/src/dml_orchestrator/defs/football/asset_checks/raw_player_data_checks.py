import yaml
import importlib.resources as pkg_resources
from typing import Any

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
)

from ...utils.csv_checks import (
    validate_schema,
    check_csv_downloaded,
    check_csv_file_size_reasonable,
    check_csv_has_header,
    check_csv_minimum_records,
    check_csv_not_html_error,
    check_csv_parseable,
    check_csv_structure_consistent,
)

from ..config import github_config


@asset_check(asset="raw_players_data", blocking=True)
def raw_players_schema_check(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Validates schema for players dataset"""

    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    try:
        with pkg_resources.files(
            "dml_orchestrator.defs.football.asset_checks.data_contracts"
        ).joinpath("player_schema.yaml").open("r") as file:
            contract = yaml.safe_load(file)
            context.log.info(f"Contract found: {contract['name']}")
    except ModuleNotFoundError as e:
        context.log.error(f"Module import failed: {e}")
    except FileNotFoundError as e:
        context.log.error(f"File not found: {e}")
        return AssetCheckResult(
            passed=False, metadata={"error": "Data contract YAML file not found"}
        )

    versions: dict[str, Any] = contract["versions"]

    if season not in versions:
        return AssetCheckResult(
            passed=False,
            metadata={
                "gameweek": gameweek,
                "season": season,
                "error": f"No schema contract for season {season}",
            },
        )

    season_contract = versions[season]

    passed, info = validate_schema(dataset=raw_players_data, contract=season_contract)

    return AssetCheckResult(
        passed=passed, metadata={"gameweek": gameweek, "season": season, "info": info}
    )


@asset_check(asset="raw_players_data")
def check_players_downloaded(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Verify players CSV was successfully downloaded and is not empty."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_downloaded(raw_players_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_parseable(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Verify players CSV can be parsed without errors."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_parseable(raw_players_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_not_html(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Verify players data is not an HTML error page."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_not_html_error(raw_players_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_file_size(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Check players file size is within reasonable bounds."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    # Players CSV typically 50KB - 5MB
    passed, base_metadata = check_csv_file_size_reasonable(
        raw_players_data, min_size=10_000, max_size=10_000_000
    )

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_structure(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Verify players CSV has consistent column counts."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_structure_consistent(raw_players_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_minimum_records(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Ensure players data has minimum expected records."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    # Premier League typically has 500+ players
    passed, base_metadata = check_csv_minimum_records(raw_players_data, min_records=400)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )


@asset_check(asset="raw_players_data")
def check_players_has_header(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    """Verify players CSV has a valid header row."""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_has_header(raw_players_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )
