from awsglue.context import DynamicFrame, GlueContext

from jobs.io.writer import write_from_options
from libs.config import Config


def load_to_s3(ddf: DynamicFrame, config: Config, path: str = None) -> None:
    connection_params = config.s3_vars
    bucket_name = connection_params.pop("bucket_name")

    full_path = f"s3://{bucket_name}/{path}"

    write_from_options(ddf, engine="s3", path=full_path, **connection_params)
