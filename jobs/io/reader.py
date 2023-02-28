from awsglue.context import GlueContext, DynamicFrame
from .helpers import get_connection_options


def read_from_options(
    context: GlueContext,
    **options: dict,
) -> DynamicFrame:
    """
    Returns the dataframe reader for the engine

    :param context: The Glue Context
    :param options: The options to pass to the reader. These options are
    specific to the engine. Read the documentation for the function
    `get_connection_options` to see the options for each engine.
    :return: The dataframe reader for the engine
    """
    format, connection_type, connection_options = get_connection_options(**options)

    paths = None

    if connection_type == "s3a":
        paths = connection_options.pop("paths", None)

        if paths:
            connection_options.pop("path", None)

    df = (
        context.spark_session.read.format(format)
        .options(**connection_options)
        .load(paths)
    )
    return DynamicFrame.fromDF(df, context, f"dynamic_frame_{format}_{connection_type}")
