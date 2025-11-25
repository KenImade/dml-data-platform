import pytest
import requests
from unittest.mock import patch, MagicMock
from dml_orchestrator.defs.utils.ingest_csv import get_csv


def test_get_csv_success():
    fake_content = b"id,name\n1,Messi"

    mock_response = MagicMock()
    mock_response.content = fake_content
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        result = get_csv("http://fake-url.com/test.csv")

    assert result == fake_content


def test_get_csv_empty_file():
    mock_response = MagicMock()
    mock_response.content = b""
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(ValueError):
            get_csv("http://fake-url.com/empty.csv")


def test_get_csv_request_failure():
    with patch("requests.get", side_effect=requests.exceptions.Timeout):
        with pytest.raises(requests.exceptions.RequestException):
            get_csv("http://fake-url.com/fail.csv")
