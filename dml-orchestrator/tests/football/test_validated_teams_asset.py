import pytest
from unittest.mock import patch, MagicMock
from dagster import asset, build_asset_context, materialize_to_memory

from dml_orchestrator.defs.football.ingestion import validated_teams_data


class FakeMinio:
    def __init__(self):
        self.put_calls = []

    def put_object(self, bucket, key, body):
        self.put_calls.append({"bucket": bucket, "key": key, "body": body})


class FakeDataFrame:
    height = 2

    def head(self, n):
        return self

    def to_pandas(self):
        import pandas as pd

        return pd.DataFrame(
            [
                {"team": "Team A", "city": "City A"},
                {"team": "Team B", "city": "City B"},
            ]
        )


@pytest.fixture
def fake_minio():
    return FakeMinio()


@pytest.fixture
def raw_csv_bytes():
    return b"team,city\nTeam A,City A\nTeam B,City B"


@pytest.fixture
def mock_validation_and_parquet():
    """Provides patched validate_csv and parquet converter."""
    validate_patch = patch(
        "dml_orchestrator.defs.football.ingestion.validate_csv",
        return_value=FakeDataFrame(),
    )
    parquet_patch = patch(
        "dml_orchestrator.defs.football.ingestion.dataframe_to_parquet_bytes",
        return_value=b"PARQUET_BYTES",
    )

    with validate_patch as m1, parquet_patch as m2:
        yield m1, m2


def test_validated_teams_data_success(
    fake_minio, raw_csv_bytes, mock_validation_and_parquet
):
    mock_validate, mock_parquet = mock_validation_and_parquet

    context = build_asset_context(resources={"minio": fake_minio})

    result = validated_teams_data(
        context=context,
        raw_teams_data=raw_csv_bytes,
    )

    assert result == b"PARQUET_BYTES"
    mock_validate.assert_called_once()
    mock_parquet.assert_called_once()

    # Check Minio upload
    assert len(fake_minio.put_calls) == 1
    upload = fake_minio.put_calls[0]
    assert upload["body"] == b"PARQUET_BYTES"

    # Check metadata
    metadata = context.get_output_metadata("result")
    assert metadata["teams_count"] == 2
    assert "teams_preview" in metadata


def test_validated_teams_data_validation_failure(raw_csv_bytes):
    context = build_asset_context(resources={"minio": MagicMock()})

    with patch(
        "dml_orchestrator.defs.football.ingestion.validate_csv",
        side_effect=Exception("Invalid CSV"),
    ):
        with pytest.raises(Exception):
            validated_teams_data(context=context, raw_teams_data=raw_csv_bytes)
