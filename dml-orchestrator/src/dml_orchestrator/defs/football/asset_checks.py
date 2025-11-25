import io
import csv
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity


@asset_check(asset="raw_players_data", description="Ensure player file is not empty")
def check_players_csv_non_empty(data: bytes) -> AssetCheckResult:
    if len(data) > 0:
        return AssetCheckResult(
            passed=True, description="Players CSV file is not empty"
        )
    return AssetCheckResult(
        passed=False,
        description="Players CSV file is empty",
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(
    asset="raw_players_data",
    description="Ensure players CSV file has the right headers",
)
def check_players_csv_headers(data: bytes) -> AssetCheckResult:
    try:
        csv_reader = csv.reader(io.StringIO(data.decode("utf-8")))
        headers = next(csv_reader)
        expected_headers = [
            "player_code",
            "player_id",
            "first_name",
            "second_name",
            "web_name",
            "team_code",
            "position",
        ]
        if headers == expected_headers:
            return AssetCheckResult(
                passed=True, description="Players CSV contains expected headers"
            )
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            description="Players CSV does not contain expected headers",
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": str(e)},
        )
