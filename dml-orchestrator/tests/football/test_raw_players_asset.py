import pytest
from dagster import build_asset_context
from unittest.mock import MagicMock, patch

from dml_orchestrator.defs.football.ingestion import raw_players_data


class FakeMinIO:
    def put_object(self, bucket, key, body):
        pass


@pytest.fixture
def context():
    return build_asset_context(resources={"minio": FakeMinIO()})


def test_raw_players_success(context):
    fake_csv = b"id,name\n1,Messi\n2,Ronaldo"

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = fake_csv
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = raw_players_data(context)

    assert result == fake_csv


def test_raw_players_empty_file_raises(context):
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = b""
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(ValueError):
            raw_players_data(context)


def test_raw_players_http_error(context):
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP failed")
        mock_get.return_value = mock_response

        with pytest.raises(Exception):
            raw_players_data(context)
