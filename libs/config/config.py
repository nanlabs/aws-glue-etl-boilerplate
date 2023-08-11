from ..common import cached_property
from .env import get_envs
from .secrets import get_secrets_resolver


class Config:
    """
    Get the credentials from the AWS Secret Manager or from the environment variables
    """

    def __init__(self, args: dict = None) -> None:
        if args is None:
            args = dict()
        self.env = get_envs(args)
        self.sr = get_secrets_resolver()

    @cached_property
    def aws_client_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client.

        :return: A json with the parameters.
        """
        return {
            "aws_access_key_id": self.env.get_var("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": self.env.get_var("AWS_SECRET_ACCESS_KEY"),
            "aws_region_name": self.env.get_var("AWS_REGION"),
            "aws_session_token": self.env.get_var("AWS_SESSION_TOKEN"),
            "aws_endpoint_url": self.env.get_var("AWS_ENDPOINT_URL"),
        }

    @cached_property
    def s3_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client and the scan's bucket name.

        :return: A json with the parameters.
        """
        return {
            **self.aws_client_vars,
            "bucket_name": self.env.get_var("S3_BUCKET_NAME"),
        }

    @cached_property
    def documentdb_vars(self) -> dict:
        """
        Get connection parameters for the Database.

        :return: A json with the connection parameters.
        """

        # TODO: Use the secrets manager to get the connection parameters for the database.

        return {
            "engine": self.env.get_var("DOCUMENT_DB_ENGINE"),
            "host": self.env.get_var("DOCUMENT_DB_HOST"),
            "port": self.env.get_var("DOCUMENT_DB_PORT"),
            "database": self.env.get_var("DOCUMENT_DB_DBNAME"),
            "user": self.env.get_var("DOCUMENT_DB_USERNAME"),
            "password": self.env.get_var("DOCUMENT_DB_PASSWORD"),
            "ssl": self.env.get_var("DOCUMENT_DB_SSL") == "true",
        }

    @cached_property
    def postgresdb_vars(self) -> dict:
        """
        Get connection parameters for the Database.

        :return: A json with the connection parameters.
        """

        # TODO: Use the secrets manager to get the connection parameters for the database.

        return {
            "engine": self.env.get_var("POSTGRES_DB_ENGINE"),
            "host": self.env.get_var("POSTGRES_DB_HOST"),
            "port": self.env.get_var("POSTGRES_DB_PORT"),
            "database": self.env.get_var("POSTGRES_DB_DBNAME"),
            "user": self.env.get_var("POSTGRES_DB_USERNAME"),
            "password": self.env.get_var("POSTGRES_DB_PASSWORD"),
        }


config_instance = None


def get_config(args: dict = None) -> Config:
    """
    Get the config instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the config.
    :return: The config class instance.
    """
    global config_instance
    if args is None:
        args = dict()
    if config_instance is None:
        config_instance = Config(args)
    return config_instance
