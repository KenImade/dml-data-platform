"""
Asset checks for raw data ingestion stage.
Ensures data availability and basic validity before Pydantic validation.
"""

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

# ============================================================================
# Asset Checks: raw_teams_data
# ============================================================================


@asset_check(asset="raw_teams_data")
def check_teams_downloaded(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams CSV was successfully downloaded and is not empty."""
    passed, metadata = check_csv_downloaded(raw_teams_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_parseable(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams CSV can be parsed without errors."""
    passed, metadata = check_csv_parseable(raw_teams_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_not_html(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams data is not an HTML error page."""
    passed, metadata = check_csv_not_html_error(raw_teams_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_file_size(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Check teams file size is within reasonable bounds."""
    # Teams CSV typically small (20 teams)
    passed, metadata = check_csv_file_size_reasonable(
        raw_teams_data, min_size=1_000, max_size=1_000_000
    )
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_structure(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams CSV has consistent column counts."""
    passed, metadata = check_csv_structure_consistent(raw_teams_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_minimum_records(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Ensure teams data has minimum expected records."""
    # Premier League has 20 teams
    passed, metadata = check_csv_minimum_records(raw_teams_data, min_records=15)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_teams_data")
def check_teams_has_header(
    context: AssetCheckExecutionContext, raw_teams_data: bytes
) -> AssetCheckResult:
    """Verify teams CSV has a valid header row."""
    passed, metadata = check_csv_has_header(raw_teams_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


# ============================================================================
# Asset Checks: raw_playerstats_data
# ============================================================================


@asset_check(asset="raw_playerstats_data")
def check_playerstats_downloaded(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Verify playerstats CSV was successfully downloaded and is not empty."""
    passed, metadata = check_csv_downloaded(raw_playerstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_parseable(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Verify playerstats CSV can be parsed without errors."""
    passed, metadata = check_csv_parseable(raw_playerstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_not_html(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Verify playerstats data is not an HTML error page."""
    passed, metadata = check_csv_not_html_error(raw_playerstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_file_size(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Check playerstats file size is within reasonable bounds."""
    passed, metadata = check_csv_file_size_reasonable(
        raw_playerstats_data, min_size=10_000, max_size=10_000_000
    )
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_structure(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Verify playerstats CSV has consistent column counts."""
    passed, metadata = check_csv_structure_consistent(raw_playerstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_minimum_records(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Ensure playerstats data has minimum expected records."""
    passed, metadata = check_csv_minimum_records(raw_playerstats_data, min_records=400)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playerstats_data")
def check_playerstats_has_header(
    context: AssetCheckExecutionContext, raw_playerstats_data: bytes
) -> AssetCheckResult:
    """Verify playerstats CSV has a valid header row."""
    passed, metadata = check_csv_has_header(raw_playerstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


# ============================================================================
# Asset Checks: raw_playermatchstats_data
# ============================================================================


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_downloaded(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Verify playermatchstats CSV was successfully downloaded and is not empty."""
    passed, metadata = check_csv_downloaded(raw_playermatchstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_parseable(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Verify playermatchstats CSV can be parsed without errors."""
    passed, metadata = check_csv_parseable(raw_playermatchstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_not_html(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Verify playermatchstats data is not an HTML error page."""
    passed, metadata = check_csv_not_html_error(raw_playermatchstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_file_size(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Check playermatchstats file size is within reasonable bounds."""
    passed, metadata = check_csv_file_size_reasonable(
        raw_playermatchstats_data, min_size=10_000, max_size=20_000_000
    )
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_structure(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Verify playermatchstats CSV has consistent column counts."""
    passed, metadata = check_csv_structure_consistent(raw_playermatchstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_minimum_records(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Ensure playermatchstats data has minimum expected records."""
    # Match-level stats should have many records
    passed, metadata = check_csv_minimum_records(
        raw_playermatchstats_data, min_records=200
    )
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_playermatchstats_data")
def check_playermatchstats_has_header(
    context: AssetCheckExecutionContext, raw_playermatchstats_data: bytes
) -> AssetCheckResult:
    """Verify playermatchstats CSV has a valid header row."""
    passed, metadata = check_csv_has_header(raw_playermatchstats_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


# ============================================================================
# Asset Checks: raw_matches_data
# ============================================================================


@asset_check(asset="raw_matches_data")
def check_matches_downloaded(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Verify matches CSV was successfully downloaded and is not empty."""
    passed, metadata = check_csv_downloaded(raw_matches_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_parseable(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Verify matches CSV can be parsed without errors."""
    passed, metadata = check_csv_parseable(raw_matches_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_not_html(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Verify matches data is not an HTML error page."""
    passed, metadata = check_csv_not_html_error(raw_matches_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_file_size(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Check matches file size is within reasonable bounds."""
    passed, metadata = check_csv_file_size_reasonable(
        raw_matches_data, min_size=1_000, max_size=5_000_000
    )
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_structure(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Verify matches CSV has consistent column counts."""
    passed, metadata = check_csv_structure_consistent(raw_matches_data)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_minimum_records(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Ensure matches data has minimum expected records."""
    # Each gameweek typically has ~10 matches
    passed, metadata = check_csv_minimum_records(raw_matches_data, min_records=5)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(asset="raw_matches_data")
def check_matches_has_header(
    context: AssetCheckExecutionContext, raw_matches_data: bytes
) -> AssetCheckResult:
    """Verify matches CSV has a valid header row."""
    passed, metadata = check_csv_has_header(raw_matches_data)
    return AssetCheckResult(passed=passed, metadata=metadata)
