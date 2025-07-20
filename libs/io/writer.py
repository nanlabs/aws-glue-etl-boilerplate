from awsglue.context import DynamicFrame, GlueContext

from .helpers import get_connection_options


def write_from_options(
    ddf: DynamicFrame,
    mode: str = "overwrite",
    **options: str,
) -> None:
    """
    Returns the dataframe writer for the engine

    :param context: The Glue Context
    :param options: The options to pass to the writer. These options are
    specific to the engine. Read the documentation for the function
    `get_connection_options` to see the options for each engine.
    :return: The dataframe writer for the engine
    """
    format, connection_type, connection_options = get_connection_options(**options)

    ddf.toDF().write.format(format).mode(mode).options(**connection_options).save()
