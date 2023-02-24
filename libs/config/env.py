import os
from typing import Any


class EnvironmentVariable:
    """
    Get the credentials from the Environment Variables
    """

    args = {}
    cache = {}

    def __init__(self, args: dict = None) -> None:
        if args is None:
            args = dict()
        self.args = args
        self.load_env()

    def load_env(self) -> None:
        # Load env if dotenv is installed
        try:
            from dotenv import load_dotenv, find_dotenv

            load_dotenv(
                find_dotenv(".env.local", raise_error_if_not_found=True, usecwd=True)
            )
            print("dotenv loaded")
        except ImportError:
            print("dotenv is not installed. Ignoring .env files now")

    def get_var(self, key, default: Any = None, throw_error: bool = False) -> Any:
        """
        Get the value of a variable from the environment variables.

        :param key: The key of the variable
        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: The value of the variable.
        """
        if key in self.args and self.args[key] is not None and self.args[key] != "":
            self.cache[key] = self.args[key]
        elif os.getenv(key) is not None:
            self.cache[key] = os.getenv(key)
        elif throw_error:
            raise Exception("Environment variable {} is not set".format(key))
        else:
            self.cache[key] = default
        return self.cache[key]

    def get_aws_access_key_id(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get Access Key Id for the AWS client connection or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "AWS_ACCESS_KEY_ID", default=default, throw_error=throw_error
        )

    def get_aws_secret_access_key(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Access Key Id for the AWS client connection or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "AWS_SECRET_ACCESS_KEY", default=default, throw_error=throw_error
        )

    def get_aws_region(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Region for the AWS client connection or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("AWS_REGION", default=default, throw_error=throw_error)

    def get_aws_session_token(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get Access Key Id for the AWS client connection or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "AWS_SESSION_TOKEN", default=default, throw_error=throw_error
        )

    def get_aws_endpoint_url(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Session Token for the AWS client connection or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "AWS_ENDPOINT_URL", default=default, throw_error=throw_error
        )

    def get_documentdb_secret_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Secret Name for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "DOCUMENTDB_SECRET_NAME", default=default, throw_error=throw_error
        )

    def get_documentdb_ssl(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the SSL Mode for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("DOCUMENTDB_SSL", default=default, throw_error=throw_error)

    def get_documentdb_database(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Name for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("DOCUMENTDB_NAME", default=default, throw_error=throw_error)

    def get_documentdb_host(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Host for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("DOCUMENTDB_HOST", default=default, throw_error=throw_error)

    def get_documentdb_port(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Port for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("DOCUMENTDB_PORT", default=default, throw_error=throw_error)

    def get_documentdb_user(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the User for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "DOCUMENTDB_USERNAME", default=default, throw_error=throw_error
        )

    def get_documentdb_password(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Password for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "DOCUMENTDB_PASSWORD", default=default, throw_error=throw_error
        )

    def get_documentdb_engine(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the engine for the DocumentDB database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "DOCUMENTDB_ENGINE", default=default, throw_error=throw_error
        )

    def get_postgresdb_secret_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Secret Name for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_SECRET_NAME", default=default, throw_error=throw_error
        )

    def get_postgresdb_database(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Name for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_NAME", default=default, throw_error=throw_error
        )

    def get_postgresdb_host(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Host for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_HOST", default=default, throw_error=throw_error
        )

    def get_postgresdb_port(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Port for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_PORT", default=default, throw_error=throw_error
        )

    def get_postgresdb_user(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the User for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_USERNAME", default=default, throw_error=throw_error
        )

    def get_postgresdb_password(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Password for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_PASSWORD", default=default, throw_error=throw_error
        )

    def get_postgresdb_engine(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the engine for the postgres database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "POSTGRES_DB_ENGINE", default=default, throw_error=throw_error
        )

    def get_s3_bucket_name(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Bucket name for the s3 S3 object or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("S3_BUCKET_NAME", default=default, throw_error=throw_error)


envs_instance = None


def get_envs(args: dict = None) -> EnvironmentVariable:
    """
    Get the envs instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the envs.
    :return: The envs.
    """
    global envs_instance
    if args is None:
        args = dict()
    if envs_instance is None:
        envs_instance = EnvironmentVariable(args)
    return envs_instance
