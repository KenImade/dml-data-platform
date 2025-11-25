import requests
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_csv(url: str) -> bytes:
    """
    Downloads csv file from specified url.

    Args:
        url: link to csv
    """
    session = requests.Session()
    retries = Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if not response.content:
            raise ValueError("CSV file is empty")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading CSV: {e}")
        raise

    return response.content
