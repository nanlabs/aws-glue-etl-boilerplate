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
        self.sr = get_secrets_resolver(
            documentdb_secret_name=self.documentdb_secret_name,
            postgresdb_secret_name=self.postgresdb_secret_name,
        )

    @cached_property
    def aws_client_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client.

        :return: A json with the parameters.
        """
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_region_name": self.aws_region,
            "aws_session_token": self.aws_session_token,
            "aws_endpoint_url": self.aws_endpoint_url,
        }

    @cached_property
    def s3_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client and the scan's bucket name.

        :return: A json with the parameters.
        """
        return {
            **self.aws_client_vars,
            "bucket_name": self.s3_bucket_name,
        }

    @cached_property
    def documentdb_vars(self) -> dict:
        """
        Get connection parameters for the Unified Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.documentdb_engine,
            "host": self.documentdb_host,
            "port": self.documentdb_port,
            "database": self.documentdb_database,
            "user": self.documentdb_user,
            "password": self.documentdb_password,
            "ssl": self.documentdb_ssl,
            "authdb": self.documentdb_authdb,
        }

    @cached_property
    def postgresdb_vars(self) -> dict:
        """
        Get connection parameters for the Unified Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.postgresdb_engine,
            "host": self.postgresdb_host,
            "port": self.postgresdb_port,
            "database": self.postgresdb_database,
            "user": self.postgresdb_user,
            "password": self.postgresdb_password,
        }

    @cached_property
    def aws_access_key_id(self) -> str:
        """
        Get Access Key Id for the AWS client connection or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_aws_access_key_id()

    @cached_property
    def aws_secret_access_key(self) -> str:
        """
        Get the Access Key Id for the AWS client connection or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_aws_secret_access_key()

    @cached_property
    def aws_region(self) -> str:
        """
        Get the Region for the AWS client connection or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_aws_region()

    @cached_property
    def aws_session_token(self) -> str:
        """
        Get the Session Token for the AWS client connection or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_aws_session_token()

    @cached_property
    def aws_endpoint_url(self) -> str:
        """
        Get the Endpoint Url for the AWS client connection or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_aws_endpoint_url()

    @cached_property
    def documentdb_engine(self) -> str:
        """
        Get the Engine for the documentdb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_engine() or self.sr.get_documentdb_engine()

    @cached_property
    def documentdb_ssl(self) -> str:
        """
        Get the SSL for the documentdb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_ssl() or self.sr.get_documentdb_ssl()

    @cached_property
    def documentdb_secret_name(self) -> str:
        """
        Get the Secret Name for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_secret_name() or ""

    @cached_property
    def documentdb_database(self) -> str:
        """
        Get the Name for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_database() or self.sr.get_documentdb_database()

    @cached_property
    def documentdb_host(self) -> str:
        """
        Get the Host for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_host() or self.sr.get_documentdb_host()

    @cached_property
    def documentdb_port(self) -> str:
        """
        Get the Port for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_port() or self.sr.get_documentdb_port()

    @cached_property
    def documentdb_user(self) -> str:
        """
        Get the User for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_user() or self.sr.get_documentdb_user()

    @cached_property
    def documentdb_authdb(self) -> str:
        """
        Get the User for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_authdb() or self.sr.get_documentdb_authdb()

    @cached_property
    def documentdb_password(self) -> str:
        """
        Get the Password for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_documentdb_password() or self.sr.get_documentdb_password()

    @cached_property
    def postgresdb_engine(self) -> str:
        """
        Get the Engine for the postgresdb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_engine() or self.sr.get_postgresdb_engine()

    @cached_property
    def postgresdb_secret_name(self) -> str:
        """
        Get the Secret Name for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_secret_name() or ""

    @cached_property
    def postgresdb_database(self) -> str:
        """
        Get the Name for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_database() or self.sr.get_postgresdb_database()

    @cached_property
    def postgresdb_host(self) -> str:
        """
        Get the Host for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_host() or self.sr.get_postgresdb_host()

    @cached_property
    def postgresdb_port(self) -> str:
        """
        Get the Port for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_port() or self.sr.get_postgresdb_port()

    @cached_property
    def postgresdb_user(self) -> str:
        """
        Get the User for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_user() or self.sr.get_postgresdb_user()

    @cached_property
    def postgresdb_password(self) -> str:
        """
        Get the Password for the DocumentDB or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_postgresdb_password() or self.sr.get_postgresdb_password()

    @cached_property
    def s3_bucket_name(self) -> str:
        """
        Get the Bucket name for the s3 S3 object or None if it is not set.

        :return: A string with the the value or a default value if it is not set.
        """
        return self.env.get_s3_bucket_name()


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
