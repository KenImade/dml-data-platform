from typing import Any, Optional
import pickle

from dagster import (
    InitResourceContext,
    InputContext,
    OutputContext,
    io_manager,
    ConfigurableIOManager,
)
from pydantic import Field
from minio import Minio
from minio.error import S3Error
import io


class MinioIOManager(ConfigurableIOManager):
    """
    IO Manager that stores data in MinIO with support for dynamic paths based on asset metadata.

    Configuration:
        endpoint: MinIO server endpoint (e.g., "localhost:9000")
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket_name: Default bucket name for storage
        secure: Use HTTPS (default: False)
        region: MinIO region (optional)

    Asset metadata for path customization:
        - path_prefix: Custom prefix for the object path
        - path_suffix: Custom suffix for the object path
        - file_extension: Custom file extension (default: "pickle")
    """

    endpoint: str = Field(description="MinIO server endpoint")
    access_key: str = Field(description="MinIO access key")
    secret_key: str = Field(description="MinIO secret key")
    bucket_name: str = Field(description="Default bucket name")
    secure: bool = Field(default=False, description="Use HTTPS connection")
    region: Optional[str] = Field(default=None, description="MinIO region")

    RAW_BYTES_EXTENSION: list[str] = ["csv", "parquet", "json", "txt", "xml"]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initialize MinIO client and ensure bucket exists"""
        self._client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            region=self.region,
        )

        try:
            if not self._client.bucket_exists(self.bucket_name):
                self._client.make_bucket(self.bucket_name)
                context.log.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            context.log.error(f"Error checking/creating bucket: {e}")
            raise

    def _get_object_path(self, context: OutputContext) -> str:
        """
        Construct the object path based on context and asset metadata.

        Supports custom path components via metadata:
        - metadata["path_prefix"]: Custom prefix (e.g., "processed/data")
        - metadata["path_suffix"]: Custom suffix (e.g., "v2")
        - metadata["file_extension"]: File extension (default: "pickle")
        """
        parts: list[str] = []

        metadata = context.definition_metadata or {}

        context.log.debug(f"Metadata getting to IO Manager: {metadata}")

        path_prefix_value = metadata.get("path_prefix", "")
        if callable(path_prefix_value):
            path_prefix = path_prefix_value(context)
            context.log.debug(f"Current path_prefix: {path_prefix}")
        else:
            path_prefix = path_prefix_value

        path_suffix_value = metadata.get("path_suffix", "")
        if callable(path_suffix_value):
            path_suffix = path_suffix_value(context)
        else:
            path_suffix = path_suffix_value

        file_extension = metadata.get("file_extension", "pickle")

        if path_prefix:
            parts.append(path_prefix.strip("/"))
        else:
            if context.asset_key:
                parts.extend(context.asset_key.path)
            else:
                parts.append(context.step_key)

        if context.has_asset_partitions and not path_prefix:
            parts.append(f"partition={context.partition_key}")

        if path_suffix:
            parts.append(path_suffix)

        path = "/".join(parts)
        return f"{path}.{file_extension}"

    def _get_bucket_name(self, context: OutputContext) -> str:
        """Get bucket name from metadata or use default"""
        metadata = context.metadata or {}
        return metadata.get("bucket_name", self.bucket_name)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store the output object in MinIO"""
        object_path = self._get_object_path(context)
        bucket_name = self._get_bucket_name(context)
        file_extension = context.definition_metadata.get("file_extension", "pickle")

        context.log.info(f"Writing to MinIO: {bucket_name}/{object_path}")

        try:
            if file_extension in self.RAW_BYTES_EXTENSION:
                if not isinstance(obj, (bytes, bytearray)):
                    raise ValueError(
                        f"Asset {context.asset_key} is marked as raw {file_extension} "
                        f"but the returned object is not bytes."
                    )
                serialized_data = obj
            else:
                serialized_data = pickle.dumps(obj)

            data_stream = io.BytesIO(serialized_data)

            self._client.put_object(
                bucket_name,
                object_path,
                data_stream,
                length=len(serialized_data),
                content_type="application/octet-stream",
            )

            context.log.info(
                f"Successfully wrote {len(serialized_data)} bytes to {bucket_name}/{object_path}"
            )

            # object metadata
            try:
                path_info = {
                    "object_path": object_path,
                    "bucket_name": bucket_name,
                }
                path_info_key = f"{object_path}.path_info"
                path_info_bytes = pickle.dumps(path_info)
                path_info_stream = io.BytesIO(path_info_bytes)

                self._client.put_object(
                    bucket_name,
                    path_info_key,
                    path_info_stream,
                    length=len(path_info_bytes),
                    content_type="application/octet-stream",
                )
                context.log.info(f"Stored path info at: {path_info_key}")
            except Exception as e:
                context.log.warning(f"Could not store path info file: {e}")

            metadata: dict[str, Any] = {
                "minio_path": object_path,
                "minio_bucket": bucket_name,
                "size_bytes": len(serialized_data),
            }
            context.log.info(f"Adding output metadata: {metadata}")

            context.add_output_metadata(metadata)

        except S3Error as e:
            context.log.error(f"Error writing to MinIO: {e}")
            raise

    def load_input(self, context: InputContext) -> Any:
        """Load the input object from MinIO"""
        context.log.info(f"=== Loading input: {context.name} ===")

        upstream_output = context.upstream_output

        if not upstream_output:
            raise ValueError(f"No upstream output found for input '{context.name}'")

        definition_metadata = upstream_output.metadata or {}

        context.log.info(f"Upstream definition metadata: {definition_metadata}")

        path_prefix_value = definition_metadata.get("path_prefix", "")
        path_suffix_value = definition_metadata.get("path_suffix", "")
        file_extension = definition_metadata.get("file_extension", "pickle")

        if callable(path_prefix_value):
            path_prefix = path_prefix_value(upstream_output)
            context.log.info(f"Computed path_prefix from lambda: {path_prefix}")
        else:
            path_prefix = path_prefix_value

        if callable(path_suffix_value):
            path_suffix = path_suffix_value(upstream_output)
            context.log.info(f"Computed path_suffix from lambda: {path_suffix}")
        else:
            path_suffix = path_suffix_value

        parts: list[str] = []
        if path_prefix:
            parts.append(path_prefix.strip("/"))
        else:
            if upstream_output.asset_key:
                parts.extend(upstream_output.asset_key.path)
            elif context.asset_key:
                parts.extend(context.asset_key.path)

            if upstream_output.partition_key:
                parts.append(f"partition={upstream_output.partition_key}")

        if path_suffix:
            parts.append(path_suffix)

        object_path = "/".join(parts) if parts else "data"
        object_path = f"{object_path}.{file_extension}"

        bucket_name = definition_metadata.get("bucket_name", self.bucket_name)

        context.log.info(f"Reading from MinIO: {bucket_name}/{object_path}")

        try:
            response = self._client.get_object(bucket_name, object_path)
            data = response.read()
            response.close()
            response.release_conn()

            if file_extension in self.RAW_BYTES_EXTENSION:
                obj = data
                context.log.info(
                    f"Successfully read {len(data)} bytes (raw) from {bucket_name}/{object_path}"
                )
            else:
                obj = pickle.loads(data)

                context.log.info(
                    f"Successfully read {len(data)} bytes from (unpickled) from {bucket_name}/{object_path}"
                )

            return obj
        except S3Error as e:
            context.log.error(f"Error reading from MinIO: {e}")
            context.log.error(f"Attempted path: {bucket_name}/{object_path}")
            raise


@io_manager(
    config_schema=MinioIOManager.to_config_schema(),
    description="IO Manager that stores data in MinIO with customizable paths",
)
def minio_io_manager(init_context) -> MinioIOManager:
    """Factory function for creating MinIO IO Manager instances."""
    return MinioIOManager.from_resource_context(init_context)
