import csv
import io
import polars as pl
from typing import Tuple, Any, Dict

TYPE_MAP = {
    "Int64": "integer",
    "Int32": "integer",
    "Int16": "integer",
    "Float64": "float",
    "Float32": "float",
    "Utf8": "string",
    "Boolean": "boolean",
}


def normalize_dtype(dtype: Any) -> str:
    """Convert Polars dtype to a normalized string type"""
    return TYPE_MAP.get(str(dtype), str(dtype).lower())


def validate_schema(
    dataset: bytes, contract: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    """
    Validate a CSV dataset (in memory) against a contract
    describing expected columns and types.

    Returns:
        (passed: bool, metadata: dict)
        Where metadata includes missing, extra and mismatched columns.

    Failure states produce passed=False with metadata.
    """

    if not dataset:
        return False, {"error": "Dataset is empty or not provided"}

    if not contract:
        return False, {"error": "Contract missing or empty"}

    if "schema" not in contract:
        return False, {"error": "Contract missing top-level 'schema' key"}

    if "columns" not in contract["schema"]:
        return False, {"error": "Contract missing 'schema.columns' section"}

    try:
        raw_schema = pl.read_csv(dataset).schema
    except Exception as e:
        return False, {"error": f"Failed to read CSV dataset: {e}"}

    # Get actual schema from csv file
    actual_schema: dict[str, str] = {
        col: normalize_dtype(dtype) for col, dtype in raw_schema.items()
    }

    # Extract expected schema from data contract
    expected_schema: dict[str, str] = {}
    for col_name, col_info in contract["schema"]["columns"].items():
        expected_schema[col_name] = col_info.get("type", "unknown")

    mismatches: list[dict[str, str]] = []
    missing_columns: list[str] = []
    extra_columns: list[str] = []

    # Check for missing columns in actual schema
    for col_name in expected_schema:
        if col_name not in actual_schema:
            missing_columns.append(col_name)

    # Check for extra columns in actual schema
    for col_name in actual_schema:
        if col_name not in expected_schema:
            extra_columns.append(col_name)

    # Check for columns in actual schema with wrong data type
    for col_name in expected_schema:
        if col_name in actual_schema:
            if actual_schema[col_name] != expected_schema[col_name]:
                mismatches.append(
                    {
                        "column": col_name,
                        "expected": expected_schema[col_name],
                        "actual": actual_schema[col_name],
                    }
                )

    # Determine if check passed
    passed = (
        len(mismatches) == 0 and len(missing_columns) == 0 and len(extra_columns) == 0
    )

    metadata: dict[str, Any] = {
        "expected_schema": expected_schema,
        "actual_schema": actual_schema,
        "mismatches": mismatches,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
    }

    return passed, metadata


def check_csv_downloaded(csv_bytes: bytes) -> Tuple[bool, Dict[str, Any]]:
    """Check if CSV data was successfully downloaded (non-empty)."""
    file_size = len(csv_bytes)
    return file_size > 0, {"file_size_bytes": file_size}


def check_csv_parseable(csv_bytes: bytes) -> Tuple[bool, Dict[str, Any]]:
    """Check if CSV can be decoded and parsed without errors."""
    try:
        csv_text = csv_bytes.decode("utf-8")
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        row_count = len(rows)

        passed = row_count > 1  # At least header + 1 data row
        metadata = {
            "row_count": row_count,
            "has_header": row_count > 0,
        }
        return passed, metadata
    except Exception as e:
        return False, {"error": str(e)}


def check_csv_not_html_error(csv_bytes: bytes) -> Tuple[bool, Dict[str, Any]]:
    """Verify we didn't receive an HTML error page instead of CSV."""
    try:
        csv_text = csv_bytes.decode("utf-8")
        first_100 = csv_text[:100].lower()

        # Check for HTML markers
        html_markers = ["<html", "<!doctype", "<head", "404", "not found", "<body"]
        is_html = any(marker in first_100 for marker in html_markers)

        return not is_html, {
            "appears_to_be_html": is_html,
            "first_100_chars": csv_text[:100],
        }
    except Exception as e:
        return False, {"error": str(e)}


def check_csv_file_size_reasonable(
    csv_bytes: bytes, min_size: int = 10_000, max_size: int = 10_000_000
) -> Tuple[bool, Dict[str, Any]]:
    """Check file size is within reasonable bounds."""
    file_size = len(csv_bytes)
    passed = min_size <= file_size <= max_size

    metadata = {
        "file_size_bytes": file_size,
        "file_size_kb": round(file_size / 1024, 2),
        "min_expected_bytes": min_size,
        "max_expected_bytes": max_size,
    }
    return passed, metadata


def check_csv_structure_consistent(csv_bytes: bytes) -> Tuple[bool, Dict[str, Any]]:
    """Verify CSV has consistent column counts across all rows."""
    try:
        csv_text = csv_bytes.decode("utf-8")
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)

        if len(rows) < 2:
            return False, {"error": "Insufficient rows (need header + data)"}

        header_cols = len(rows[0])
        inconsistent_rows = []

        for i, row in enumerate(rows[1:], start=1):
            # Skip trailing or empty rows
            if len(row) == 0 or all(cell.strip() == "" for cell in row):
                continue

            if len(row) != header_cols:
                inconsistent_rows.append({"row": i, "cols": len(row)})

        passed = len(inconsistent_rows) == 0
        metadata = {
            "expected_columns": header_cols,
            "total_rows": len(rows),
            "inconsistent_rows": inconsistent_rows[:10],  # First 10
            "inconsistent_count": len(inconsistent_rows),
        }
        return passed, metadata
    except Exception as e:
        return False, {"error": str(e)}


def check_csv_minimum_records(
    csv_bytes: bytes, min_records: int
) -> Tuple[bool, Dict[str, Any]]:
    """Ensure CSV has minimum expected number of data records."""
    try:
        csv_text = csv_bytes.decode("utf-8")
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        data_rows = len(rows) - 1  # Exclude header

        passed = data_rows >= min_records
        metadata = {
            "data_rows": data_rows,
            "min_expected": min_records,
        }
        return passed, metadata
    except Exception as e:
        return False, {"error": str(e)}


def check_csv_has_header(csv_bytes: bytes) -> Tuple[bool, Dict[str, Any]]:
    """Verify CSV has a header row with column names."""
    try:
        csv_text = csv_bytes.decode("utf-8")
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)

        if len(rows) == 0:
            return False, {"error": "No rows found"}

        header = rows[0]
        # Check header doesn't look like data (e.g., all numeric)
        looks_like_header = any(
            not cell.replace(".", "").replace("-", "").isdigit()
            for cell in header
            if cell
        )

        passed = looks_like_header and len(header) > 0
        metadata = {
            "header_columns": len(header),
            "first_few_columns": header[:5],
        }
        return passed, metadata
    except Exception as e:
        return False, {"error": str(e)}
