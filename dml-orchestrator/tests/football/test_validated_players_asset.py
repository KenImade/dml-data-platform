import pytest
from unittest.mock import patch, MagicMock
from dagster import build_asset_context

from dml_orchestrator.defs.football.ingestion import validated_players_data


class FakeMinio:
    def __init__(self):
        self.put_calls = []

    def put_object(self, bucket, key, body):
        self.put_calls.append({"bucket": bucket, "key": key, "body": body})


class FakeDataFrame:
    height = 3  # players count for test

    def head(self, n):
        return self

    def to_pandas(self):
        import pandas as pd

        return pd.DataFrame(
            [
                {"player": "Player A", "team": "Team X"},
                {"player": "Player B", "team": "Team Y"},
                {"player": "Player C", "team": "Team Z"},
            ]
        )


@pytest.fixture
def fake_minio():
    return FakeMinio()


@pytest.fixture
def raw_players_csv_bytes():
    return b"player,team\nPlayer A,Team X\nPlayer B,Team Y\nPlayer C,Team Z"


@pytest.fixture
def mock_validation_and_parquet():
    """Patch validate_csv and parquet converter for all tests needing them."""
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


def test_validated_players_data_success(
    fake_minio, raw_players_csv_bytes, mock_validation_and_parquet
):
    context = build_asset_context(resources={"minio": fake_minio})
    mock_validate, mock_parquet = mock_validation_and_parquet

    output = validated_players_data(
        context=context,
        raw_players_data=raw_players_csv_bytes,
    )

    assert output == b"PARQUET_BYTES"
    mock_validate.assert_called_once()
    mock_parquet.assert_called_once()

    # Verify MinIO upload
    assert len(fake_minio.put_calls) == 1
    call = fake_minio.put_calls[0]
    assert call["body"] == b"PARQUET_BYTES"

    # Verify metadata
    metadata = context.get_output_metadata("result")
    assert metadata["players_count"] == 3
    assert "players_preview" in metadata


def test_validated_players_data_validation_failure(raw_players_csv_bytes):
    context = build_asset_context(resources={"minio": MagicMock()})

    with patch(
        "dml_orchestrator.defs.football.ingestion.validate_csv",
        side_effect=Exception("Invalid CSV"),
    ):
        with pytest.raises(Exception):
            validated_players_data(
                context=context,
                raw_players_data=raw_players_csv_bytes,
            )


def test_minio_upload_parameters(
    fake_minio, raw_players_csv_bytes, mock_validation_and_parquet
):
    context = build_asset_context(resources={"minio": fake_minio})

    validated_players_data(context, raw_players_csv_bytes)

    call = fake_minio.put_calls[0]
    assert "players.parquet" in call["key"]
    assert call["bucket"] is not None
    assert call["body"] == b"PARQUET_BYTES"
