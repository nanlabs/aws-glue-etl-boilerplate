from awsglue.context import DynamicFrame

from jobs.io.writer import write_from_options
from libs.config import Config


def load_to_document_db(ddf: DynamicFrame, config: Config, collection: str):
    connection_params: dict = config.documentdb_vars
    print(config.documentdb_vars)
    connection_params["collection"] = collection
    write_from_options(ddf, mode="overwrite", **connection_params)
