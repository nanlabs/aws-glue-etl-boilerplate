from awsglue.context import DynamicFrame

from ..io.writer import write_from_options
from ..config import Config


def load_to_document_db(ddf: DynamicFrame, config: Config, collection: str):
    connection_params: dict = config.documentdb_vars
    connection_params["collection"] = collection
    write_from_options(ddf, mode="overwrite", **connection_params)
