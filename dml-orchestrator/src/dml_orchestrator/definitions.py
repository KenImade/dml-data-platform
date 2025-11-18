import os
from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv

# Import your resource
from .defs.resources import MinIOResource, get_dbt_resource

# Import your assets
from .defs import assets

# Load environment variables
load_dotenv()

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "minio": MinIOResource(
            endpoint_url=os.getenv("DAGSTER_MINIO_SERVER", "http://minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "dagster"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "password123456"),
            bucket="test-bucket",
            region_name=os.getenv("MINIO_REGION", "us-east-1"),
        ),
        "dbt": get_dbt_resource()
    },
)
