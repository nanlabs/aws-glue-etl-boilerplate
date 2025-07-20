from awsglue.context import GlueContext, DynamicFrame

from ..io.writer import write_from_options
from ..config import Config


def load_to_s3(ddf: DynamicFrame, config: Config, path: str = None) -> None:
    connection_params = dict(config.s3_vars)  # copia para evitar mutar el original

    bucket_name = connection_params.pop("bucket_name", None)
    if not bucket_name:
        raise ValueError("Missing 'bucket_name' in config.s3_vars")

    if not path:
        raise ValueError("Missing destination path for S3 write")

    full_path = f"s3://{bucket_name}/{path}"

    write_from_options(ddf, engine="s3", path=full_path, mode="overwrite", **connection_params)
