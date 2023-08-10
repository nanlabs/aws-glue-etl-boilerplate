from awsglue.context import DynamicFrame

from jobs.io.writer import write_from_options
from libs.config import Config


def load_to_postgresql_db(ddf: DynamicFrame, config: Config, table: str):
    connection_params: dict = config.postgresdb_vars
    connection_params["engine"] = "postgres"
    connection_params["dbtable"] = table
    write_from_options(ddf, mode="overwrite", **connection_params)
