import yaml
from typing import Any
from dagster import (
    asset_check,
    AssetCheckExecutionContext,
    AssetCheckResult,
    file_relative_path,
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


@asset_check(asset="raw_teams_data", blocking=True)
def raw_teams_schema_check(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Validates schema for teams dataset"""

    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    try:
        with open(
            file_relative_path(__file__, "data_contracts/team_schema.yaml")
        ) as file:
            contract = yaml.safe_load(file)
            context.log.info(f"Contract found: {contract['name']}")
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

    passed, info = validate_schema(raw_teams_data, contract=season_contract)

    return AssetCheckResult(
        passed=passed, metadata={"gameweek": gameweek, "season": season, "info": info}
    )


@asset_check(asset="raw_teams_data")
def check_teams_downloaded(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams CSV was successfulyy downloaded and is not empty"""
    gameweek = context.op_execution_context.partition_key
    season = github_config.season

    passed, base_metadata = check_csv_downloaded(raw_teams_data)

    return AssetCheckResult(
        passed=passed,
        metadata={"gameweek": gameweek, "season": season, **base_metadata},
    )
