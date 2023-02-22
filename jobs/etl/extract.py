from typing import List
from awsglue.context import GlueContext

from jobs.io import read_from_options
from libs.config.config import Config


def extract(glueContext: GlueContext, paths: List[str], config: Config):
    connection_params = config.aws_client_vars
    connection_params["engine"] = "s3"

    ddf = read_from_options(glueContext, paths=paths, **connection_params)
    return ddf
