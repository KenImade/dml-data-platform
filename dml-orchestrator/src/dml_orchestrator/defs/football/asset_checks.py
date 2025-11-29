import io
import csv
import polars as pl
from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
    AssetCheckSeverity,
)


@asset_check(
    asset="raw_players_data",
    description="Ensure players CSV file is not empty",
)
def check_players_csv_non_empty(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    if raw_players_data and len(raw_players_data) > 0:
        return AssetCheckResult(
            passed=True,
            description="Players CSV file is not empty",
        )

    return AssetCheckResult(
        passed=False,
        description="Players CSV file is empty",
        severity=AssetCheckSeverity.ERROR,
        metadata={"partition": context.partition_key},
    )


@asset_check(
    asset="raw_players_data",
    description="Ensure players CSV file has the correct headers",
)
def check_players_csv_headers(
    context: AssetCheckExecutionContext, raw_players_data: bytes
) -> AssetCheckResult:
    try:
        text = raw_players_data.decode("utf-8")
        csv_reader = csv.reader(io.StringIO(text))
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
                passed=True,
                description="Players CSV contains expected headers",
            )

        return AssetCheckResult(
            passed=False,
            description="Players CSV headers did not match expected schema",
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "read_headers": headers,
                "expected_headers": expected_headers,
                "partition": context.partition_key,
            },
        )

    except Exception as e:
        return AssetCheckResult(
            passed=False,
            description="Failed to parse players CSV",
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "error": str(e),
                "partition": context.partition_key,
            },
        )


@asset_check(
    asset="validated_players_data",
    description="Ensure validated players data has the required schema",
)
def check_players_schema(validated_players_data: bytes):
    df = pl.read_parquet(io.BytesIO(validated_players_data))

    required_columns = {
        "player_code",
        "player_id",
        "first_name",
        "second_name",
        "web_name",
        "team_code",
        "position",
    }

    missing = required_columns - set(df.columns)

    if missing:
        return AssetCheckResult(
            passed=False,
            description="Validated players data does not have required columns",
            severity=AssetCheckSeverity.WARN,
            metadata={"missing_columns": list(missing)},
        )

    return AssetCheckResult(
        passed=True,
        description="Validated players data has required columns",
        metadata={"rows": df.height},
    )


@asset_check(
    asset="validated_teams_data",
    description="Ensure that the dataset has the required number of teams",
)
def check_number_of_teams(validated_teams_data: bytes):
    df = pl.read_parquet(io.BytesIO(validated_teams_data))

    if df.height != 20:
        return AssetCheckResult(
            passed=False,
            description="Teams dataset does not contain the right number of teams",
            severity=AssetCheckSeverity.ERROR,
            metadata={"number_of_teams": df.height},
        )

    return AssetCheckResult(
        passed=True,
        description="Teams dataset contains right number of teams.",
        metadata={"number_of_teams": df.height},
    )
