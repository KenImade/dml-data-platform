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

    def _safe_call_path_prefix(self, value, context):
        """
        Safely call a path_prefix callable.

        If it attempts to read context.partition_key during a non-partitioned run,
        catch the error and return a fallback path.
        """
        if not callable(value):
            return value

        try:
            return value(context)
        except Exception as e:
            context.log.warning(
                f"path_prefix callable failed due to missing parition key: {e}"
                f"Using fallback path prefix"
            )

            return f"{context.asset_key.path[-1]}/_no_partition"

    def handle_output(self, context: OutputContext, obj: Any) -> None:

        metadata = context.definition_metadata or {}

        # SAFE prefix resolution
        raw_prefix_value = metadata.get("path_prefix", "")
        path_prefix = self._safe_call_path_prefix(raw_prefix_value, context)

        # SAFE suffix resolution
        raw_suffix_value = metadata.get("path_suffix", "")
        path_suffix = (
            self._safe_call_path_prefix(raw_suffix_value, context)
            if callable(raw_suffix_value)
            else raw_suffix_value
        )

        file_extension = metadata.get("file_extension", "pickle")

        # Build the object path
        parts = []
        if path_prefix:
            parts.append(path_prefix.strip("/"))
        else:
            parts.extend(context.asset_key.path)

        if context.has_asset_partitions and not path_prefix:
            parts.append(f"partition={context.partition_key}")

        if path_suffix:
            parts.append(path_suffix)

        object_path = "/".join(parts) + f".{file_extension}"
        bucket_name = metadata.get("bucket_name", self.bucket_name)

        # --- Write to MinIO ---
        context.log.info(f"Writing to MinIO: {bucket_name}/{object_path}")

        serialized = (
            obj if file_extension in self.RAW_BYTES_EXTENSION else pickle.dumps(obj)
        )

        self._client.put_object(
            bucket_name,
            object_path,
            io.BytesIO(serialized),
            length=len(serialized),
            content_type="application/octet-stream",
        )

        context.add_output_metadata(
            {
                "minio_path": object_path,
                "minio_bucket": bucket_name,
                "size_bytes": len(serialized),
            }
        )

    def load_input(self, context: InputContext) -> Any:

        upstream_output = context.upstream_output
        definition_metadata = upstream_output.metadata or {}

        context.log.info(f"=== Loading input: {context.name} ===")
        context.log.info(f"Upstream definition metadata: {definition_metadata}")

        # SAFE prefix resolution
        raw_prefix_value = definition_metadata.get("path_prefix", "")
        path_prefix = self._safe_call_path_prefix(raw_prefix_value, upstream_output)

        # SAFE suffix resolution
        raw_suffix_value = definition_metadata.get("path_suffix", "")
        path_suffix = (
            self._safe_call_path_prefix(raw_suffix_value, upstream_output)
            if callable(raw_suffix_value)
            else raw_suffix_value
        )

        file_extension = definition_metadata.get("file_extension", "pickle")

        # Build load path
        parts = []
        if path_prefix:
            parts.append(path_prefix.strip("/"))
        else:
            parts.extend(upstream_output.asset_key.path)

            if getattr(upstream_output, "has_asset_partitions", False):
                parts.append(f"partition={upstream_output.partition_key}")

        if path_suffix:
            parts.append(path_suffix)

        object_path = "/".join(parts) + f".{file_extension}"
        bucket_name = definition_metadata.get("bucket_name", self.bucket_name)

        context.log.info(f"Reading from MinIO: {bucket_name}/{object_path}")

        # --- Read from MinIO ---
        response = self._client.get_object(bucket_name, object_path)
        data = response.read()
        response.close()
        response.release_conn()

        if file_extension in self.RAW_BYTES_EXTENSION:
            return data

        return pickle.loads(data)


@io_manager(
    config_schema=MinioIOManager.to_config_schema(),
    description="IO Manager that stores data in MinIO with customizable paths",
)
def minio_io_manager(init_context) -> MinioIOManager:
    """Factory function for creating MinIO IO Manager instances."""
    return MinioIOManager.from_resource_context(init_context)
