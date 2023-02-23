from typing import Dict, Tuple
from urllib.parse import quote
from libs.common import load_tls_ca_bundle


def get_url_for_engine(
    host: str,
    port: int,
    database: str,
    engine: str,
    user: str = None,
    password: str = None,
    ssl: bool = False,
) -> str:
    """
    Returns the url for the engine. Will use the AWS credentials only if the
    engine is s3.

    :param host: The host to connect to
    :param port: The port to connect to
    :param database: The database to connect to
    :param engine: The engine to get the url for
    :param user: The user to connect with
    :param password: The password to connect with
    :return: The url for the engine
    """
    if engine is None:
        raise ValueError("engine is required. Please provide a valid engine.")

    urls = {
        "sqlserver": f"jdbc:sqlserver://{host}:{port};database={database}",
        "postgres": f"jdbc:postgresql://{host}:{port}/{database}",
        "mongo": _build_mongodb_connection_string(
            host, port, database, engine, user, password, ssl
        ),
        "s3": None,
    }

    return urls[engine]


def _build_mongodb_connection_string(
    host: str,
    port: int,
    database: str,
    engine: str,
    user: str = None,
    password: str = None,
    ssl: bool = False,
):
    encoded_user = _encode_mongodb_auth_special_chars(user)
    encoded_pass = _encode_mongodb_auth_special_chars(password)
    return (
        f"mongodb://{encoded_user}:{encoded_pass}@{host}:{port}/"
        f"{database}?tls={'true' if ssl else 'false'}&tlsCaFile="
        f"{load_tls_ca_bundle() if ssl else ''}"
    )


def _encode_mongodb_auth_special_chars(authchars: str):
    """
    This is a helper function to encode special characters
    that might be present on user, password, tokens, etc:
        : / ? # [ ] @

    See the following resources:
        * https://www.mongodb.com/docs/manual/reference/connection-string/
            #connection-string-uri-format
        * https://www.rfc-editor.org/rfc/rfc3986#section-2.1
        * https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote

    """
    if isinstance(authchars, str):
        encoded_authchars = authchars
        for c in ":/?#[]@":
            encoded_authchars = encoded_authchars.replace(c, quote(c))
        return encoded_authchars
    return authchars


def get_connection_type(engine: str, ssl: bool = False) -> str:
    """
    Returns the connection type for the engine

    :param engine: The engine to get the connection type for
    :param ssl: Whether to use ssl or not
    :return: The connection type for the engine
    """
    if engine is None:
        raise ValueError("engine is required. Please provide a valid engine.")

    enginesConnectionTypes = {
        "sqlserver": "jdbc",
        "postgres": "jdbc",
        "mongo": "mongodb",
        "s3": "s3a",
    }

    connection_type = enginesConnectionTypes[engine]

    if ssl and connection_type == "mongodb":
        connection_type = "documentdb"

    return connection_type


def get_driver_for_engine(engine: str = None) -> str:
    """
    Returns the driver for the engine

    :param engine: The engine to get the driver for
    :return: The driver for the engine
    """
    if engine is None:
        raise ValueError("engine is required. Please provide a valid engine.")

    drivers = {
        "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "postgres": "org.postgresql.Driver",
        "mongo": None,
        "s3": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }

    return drivers[engine]


def get_format_for_engine(engine: str = None) -> str:
    """
    Returns the format for the engine

    :param engine: The engine to get the format for
    :return: The format for the engine
    """
    if engine is None:
        raise ValueError("engine is required. Please provide a valid engine.")

    enginesFormats = {
        "sqlserver": "jdbc",
        "postgres": "jdbc",
        "mongo": "mongodb",
        "s3": "json",
    }

    return enginesFormats[engine]


def get_connection_options(
    engine: str,
    host: str = None,
    port: int = None,
    database: str = None,
    user: str = None,
    password: str = None,
    ssl: bool = False,
    dbtable: str = None,
    collection: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region_name: str = None,
    aws_session_token: str = None,
    aws_endpoint_url: str = None,
    paths: str = None,
    path: str = None,
) -> Tuple[str, str, Dict[str, str]]:
    """
    Returns the format and options for the engine. Will use the AWS credentials
    only if the engine is s3.

    :param host: The host to connect to
    :param port: The port to connect to
    :param database: The database to connect to
    :param engine: The engine to get the format and options for
    :param user: The user to connect with
    :param password: The password to connect with
    :param ssl: Whether to use ssl or not
    :param dbtable: The table to connect to
    :param collection: The collection to connect to
    :param aws_access_key_id: The aws access key id to connect with
    :param aws_secret_access_key: The aws secret access key to connect with
    :param aws_region_name: The aws region name to connect with
    :param aws_session_token: The aws session token to connect with
    :param aws_endpoint_url: The aws endpoint url to connect with
    :param path: The path to connect to
    :return: The format and options for the engine
    """
    if engine is None:
        raise ValueError("engine is required. Please provide a valid engine.")

    format = get_format_for_engine(engine)

    connection_type = get_connection_type(engine=engine, ssl=ssl)

    url = get_url_for_engine(
        host=host,
        port=port,
        database=database,
        engine=engine,
        user=user,
        password=password,
        ssl=ssl,
    )

    driver = get_driver_for_engine(engine=engine)

    connection_options = {
        "sqlserver": {
            "url": url,
            "driver": driver,
            "user": user,
            "password": password,
            "dbtable": dbtable,
        },
        "postgres": {
            "url": url,
            "driver": driver,
            "user": user,
            "password": password,
            "dbtable": dbtable,
        },
        "mongo": {
            "connection.uri": url,
            "database": database,
            "username": user,
            "password": password,
            "collection": collection,
            "partitioner": "MongoSamplePartitioner",
            "partitionerOptions.partitionSizeMB": "10",
            "partitionerOptions.partitionKey": "_id",
        },
        "s3": {
            "fs.s3a.impl": driver,
            "fs.s3a.access.key": aws_access_key_id,
            "fs.s3a.secret.key": aws_secret_access_key,
            "fs.s3a.session.token": aws_session_token,
            "fs.s3a.endpoint": aws_endpoint_url,
            "fs.s3a.region": aws_region_name,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": "false",
            "com.amazonaws.services.s3a.enableV4": "true",
            "paths": paths,
            "path": path,
        },
    }

    return format, connection_type, connection_options[engine]
