from __future__ import annotations
import boto3
import base64
import json
from botocore.exceptions import ClientError

secrets_resolver_instance = None


def get_secrets_resolver(
    documentdb_secret_name: str = None, postgresdb_secret_name: str = None
) -> SecretsResolver:
    """
    Get the secrets manager instance. This is a singleton.

    :param documentdb_secret_name: The name of the unified data base.
    :return: The secrets manager instance.
    """
    global secrets_resolver_instance
    if secrets_resolver_instance is None:
        secrets_resolver_instance = SecretsResolver(
            documentdb_secret_name=documentdb_secret_name,
            postgresdb_secret_name=postgresdb_secret_name,
        )
    return secrets_resolver_instance


class SecretsResolver:
    # The secret cache is a dictionary that stores the secrets in memory.
    secret_cache = {}

    def __init__(
        self,
        documentdb_secret_name: str,
        postgresdb_secret_name: str,
    ) -> None:
        self.documentdb_secret_name = documentdb_secret_name
        self.postgresdb_secret_name = postgresdb_secret_name

    def get_secret_key(
        self,
        data_source: str,
        key: str,
        secret_name: str = None,
        default: str = None,
        json_decode: dict = True,
    ) -> str:
        """
        Get a secret from Secrets Manager by key.
        If the secret does not exist, return the default value.

        :param data_source: The name of the data source.
        :param key: The key to get the secret for.
        :param secret_name: The name of the secret in AWS.
        :param default: The default value to return if the secret does not exist.
        :param json_decode: Whether to JSON decode the secret.
        :return: The secret value, or the default value if the secret does not exist.
        """
        secret = self.get_secret(data_source, secret_name=secret_name)
        if secret is None:
            return default
        if json_decode:
            secret = json.loads(secret)
        return secret[key] if key in secret else default

    def get_secret(self, data_source: str, secret_name: str = None) -> bytes:
        """
        Get a secret from Secrets Manager.

        :param data_source: The name of the data source.
        :param secret_name: The name of the secret in AWS.
        :return: The secret value, or None if the secret does not exist.
        """

        if data_source in self.secret_cache:
            return self.secret_cache[data_source]

        sources = {
            # Postgress - Submissions
            "submissions": {
                "secret_name": "rds/production",
                "region_name": "us-east-1",
            },
            # S3 - Scans
            "scans": {
                "secret_name": "AmazonSageMaker-sagemaker-bitbucket",
                "region_name": "us-east-1",
            },
            # Mssql - Quotes
            "quotes": {"secret_name": "ims/db/read-only", "region_name": "us-east-1"},
            # UnifiedDB - DocumentDB
            "documentdb": {
                "secret_name": self.documentdb_secret_name,
                "region_name": "us-east-1",
            },
            # AnalyticsDB - Postgres
            "postgresdb": {
                "secret_name": self.postgresdb_secret_name,
                "region_name": "us-east-1",
            },
        }

        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name=sources[data_source]["region_name"],
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=sources[data_source]["secret_name"]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text
                # using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current
                # state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary,
            # one of these fields will be populated.
            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]
                self.secret_cache[data_source] = secret
                return secret
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                )
                self.secret_cache[data_source] = decoded_binary_secret
                return decoded_binary_secret

    def get_documentdb_database(self) -> str:
        """
        Get the Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "database", default="UnifiedDB")

    def get_documentdb_host(self) -> str:
        """
        Get the Host for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "host")

    def get_documentdb_port(self) -> str:
        """
        Get the Port for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "port")

    def get_documentdb_user(self) -> str:
        """
        Get the User for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "username")

    def get_documentdb_password(self) -> str:
        """
        Get the Password for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "password")

    def get_documentdb_engine(self) -> str:
        """
        Get the engine for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "engine", default="mongo")

    def get_documentdb_ssl(self) -> bool:
        """
        Get the ssl for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("documentdb", "ssl", default=False)

    def get_postgresdb_database(self) -> str:
        """
        Get the Name for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "database", default="AnalyticsDB")

    def get_postgresdb_host(self) -> str:
        """
        Get the Host for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "host")

    def get_postgresdb_port(self) -> str:
        """
        Get the Port for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "port")

    def get_postgresdb_user(self) -> str:
        """
        Get the User for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "username")

    def get_postgresdb_password(self) -> str:
        """
        Get the Password for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "password")

    def get_postgresdb_engine(self) -> str:
        """
        Get the engine for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("postgresdb", "engine", default="postgres")
