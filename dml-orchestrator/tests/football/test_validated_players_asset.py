import pytest
from dagster import build_asset_context
from unittest.mock import MagicMock, patch
import polars as pl


from dml_orchestrator.defs.football.ingestion import validated_players_data


class FakeMinIO:
    def put_object(self, bucket, key, body):
        pass


@pytest.fixture
def context():
    return build_asset_context(resources={"minio": FakeMinIO()})


def test_validated_players_success(context):
    fake_csv_bytes = b"id,name\n1,Messi\n2,Ronaldo"
    fake_df = pl.DataFrame({"id": [1, 2], "name": ["Messi", "Ronaldo"]})
    fake_parquet = b"PARQUET_BYTES"

    with patch(
        "dml_orchestrator.defs.football.ingestion.validate_players_csv",
        return_value=fake_df,
    ):
        with patch(
            "dml_orchestrator.defs.football.ingestion.dataframe_to_parquet_bytes",
            return_value=fake_parquet,
        ):
            result = validated_players_data(context, fake_csv_bytes)

    assert result == fake_parquet


def test_validated_players_validation_failure(context):
    with patch(
        "dml_orchestrator.defs.football.ingestion.validate_players_csv",
        side_effect=Exception("Bad data"),
    ):
        with pytest.raises(Exception):
            validated_players_data(context, b"bad_data")


def test_validated_players_upload_failure(context):
    fake_df = pl.DataFrame({"id": [1], "name": ["Messi"]})

    with patch(
        "dml_orchestrator.defs.football.ingestion.validate_players_csv",
        return_value=fake_df,
    ):
        with patch(
            "dml_orchestrator.defs.football.ingestion.dataframe_to_parquet_bytes",
            return_value=b"parquet",
        ):
            with patch.object(
                FakeMinIO, "put_object", side_effect=Exception("MinIO failed")
            ):
                with pytest.raises(Exception):
                    validated_players_data(context, b"data")
