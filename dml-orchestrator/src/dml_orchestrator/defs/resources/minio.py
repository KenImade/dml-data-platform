from dagster import ConfigurableResource
import boto3
import logging
from botocore.exceptions import ClientError
from botocore.client import BaseClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MinIOResource(ConfigurableResource):

    endpoint: str
    access_key: str
    secret_key: str
    default_bucket: str | None = None
    region_name: str = "us-east-1"
    use_ssl: bool = False

    def get_client(self) -> BaseClient:
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            use_ssl=self.use_ssl,
        )

    def put_object(self, key: str, body: bytes, bucket: str | None = None):
        """Upload object to MinIO, specifying the bucket"""
        client = self.get_client()
        target_bucket = bucket or self.default_bucket
        if not target_bucket:
            raise ValueError(
                "Bucket name must be provided either as default or in the call"
            )
        try:
            client.put_object(Bucket=target_bucket, Key=key, Body=body)
            logger.info(f"Uploaded object to s3://{target_bucket}/{key}")
        except ClientError as e:
            logger.error(f"Failed to upload {key} to {target_bucket}: {e}")
            raise

    def get_object(self, key: str, bucket: str | None = None) -> bytes:
        """Download object from configured bucket"""
        client = self.get_client()
        target_bucket = bucket or self.default_bucket
        if not target_bucket:
            raise ValueError(
                "Bucket name must be provided either as default or in the call"
            )
        try:
            response = client.get_object(Bucket=target_bucket, Key=key)
            logger.info(f"Downloaded object from s3://{target_bucket}/key")
            return response["Body"].read()
        except ClientError as e:
            logger.error(f"Failed to download {key} from {target_bucket}: {e}")
            raise
