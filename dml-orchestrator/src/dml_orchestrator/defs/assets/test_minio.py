from dagster import asset, OpExecutionContext
from ..resources.minio import MinIOResource


@asset
def write_test_file(context: OpExecutionContext, minio: MinIOResource):
    minio.put_object("dagster_test.txt", b"Hello from Dagster!")
    context.log.info("Uploaded dagster_test.txt to MinIO")
