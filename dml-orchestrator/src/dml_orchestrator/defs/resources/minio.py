from dagster import ConfigurableResource
import boto3


class MinIOResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    region_name: str = "us-east-1"

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            use_ssl=False,
        )

    def put_object(self, key: str, body: bytes):
        """Helper that automatically uses the configured bucket"""
        client = self.get_client()
        return client.put_object(Bucket=self.bucket, Key=key, Body=body)
