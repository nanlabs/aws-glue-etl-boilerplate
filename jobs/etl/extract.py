from typing import List

from awsglue.context import GlueContext

from jobs.io import read_from_options
from libs.aws import AwsS3Client
from libs.config import Config


def extract(glueContext: GlueContext, config: Config):
    client = AwsS3Client(**config.s3_vars)

    connection_params = config.aws_client_vars
    connection_params["engine"] = "s3"
    connection_params["paths"] = [
        f"s3://{o.bucket_name}/{o.key}" for o in client.get_objects()
    ]
    ddf = read_from_options(glueContext, **connection_params)
    return ddf
