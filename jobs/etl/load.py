from awsglue.context import DynamicFrame

from jobs.io.writer import write_from_options
from libs.config.config import Config


def load(ddf: DynamicFrame, config: Config):
    connection_params: dict = config.postgredb
    write_from_options(ddf, options=connection_params)
