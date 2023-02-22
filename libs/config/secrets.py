from __future__ import annotations
import boto3
import base64
import json
from botocore.exceptions import ClientError

secrets_resolver_instance = None


def get_secrets_resolver(
    udb_secret_name: str = None, analyticsdb_secret_name: str = None
) -> SecretsResolver:
    """
    Get the secrets manager instance. This is a singleton.

    :param udb_secret_name: The name of the unified data base.
    :return: The secrets manager instance.
    """
    global secrets_resolver_instance
    if secrets_resolver_instance is None:
        secrets_resolver_instance = SecretsResolver(
            udb_secret_name=udb_secret_name,
            analyticsdb_secret_name=analyticsdb_secret_name,
        )
    return secrets_resolver_instance


class SecretsResolver:
    # The secret cache is a dictionary that stores the secrets in memory.
    secret_cache = {}

    def __init__(
        self,
        udb_secret_name: str = "measured/UnifiedDatabase",
        analyticsdb_secret_name: str = "measured/AnalyticsPostgresDB",
    ) -> None:
        self.udb_secret_name = udb_secret_name
        self.analyticsdb_secret_name = analyticsdb_secret_name

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
            "measured_unifieddb": {
                "secret_name": self.udb_secret_name,
                "region_name": "us-east-1",
            },
            # AnalyticsDB - Postgres
            "measured_analyticsdb": {
                "secret_name": self.analyticsdb_secret_name,
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
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
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
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
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

    def get_submission_database(self) -> str:
        """
        Get the Name for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return "workflow"

    def get_submission_engine(self) -> str:
        """
        Get the Engine for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("submissions", "engine", default="postgres")

    def get_submission_host(self) -> str:
        """
        Get the Host for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("submissions", "host")

    def get_submission_port(self) -> str:
        """
        Get the Port for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("submissions", "port")

    def get_submission_user(self) -> str:
        """
        Get the User for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("submissions", "username")

    def get_submission_password(self) -> str:
        """
        Get the Password for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("submissions", "password")

    def get_ims_engine(self) -> str:
        """
        Get the Engine for the ims database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "engine", default="sqlserver")

    def get_ims_host(self) -> str:
        """
        Get the Host for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "host")

    def get_ims_port(self) -> str:
        """
        Get the Port for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "port")

    def get_ims_user(self) -> str:
        """
        Get the User for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "username")

    def get_ims_password(self) -> str:
        """
        Get the Pasword for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "password")

    def get_ims_database(self) -> str:
        """
        Get the Name for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("quotes", "database")

    def get_udb_database(self) -> str:
        """
        Get the Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key(
            "measured_unifieddb", "database", default="UnifiedDB"
        )

    def get_udb_host(self) -> str:
        """
        Get the Host for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "host")

    def get_udb_port(self) -> str:
        """
        Get the Port for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "port")

    def get_udb_user(self) -> str:
        """
        Get the User for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "username")

    def get_udb_password(self) -> str:
        """
        Get the Password for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "password")

    def get_udb_engine(self) -> str:
        """
        Get the engine for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "engine", default="mongo")

    def get_udb_ssl(self) -> bool:
        """
        Get the ssl for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_unifieddb", "ssl", default=False)

    def get_analyticsdb_database(self) -> str:
        """
        Get the Name for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key(
            "measured_analyticsdb", "database", default="AnalyticsDB"
        )

    def get_analyticsdb_host(self) -> str:
        """
        Get the Host for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_analyticsdb", "host")

    def get_analyticsdb_port(self) -> str:
        """
        Get the Port for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_analyticsdb", "port")

    def get_analyticsdb_user(self) -> str:
        """
        Get the User for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_analyticsdb", "username")

    def get_analyticsdb_password(self) -> str:
        """
        Get the Password for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_analyticsdb", "password")

    def get_analyticsdb_engine(self) -> str:
        """
        Get the engine for the Analytics database or None if it is not set.

        :return: A string with the the value.
        """
        return self.get_secret_key("measured_analyticsdb", "engine", default="postgres")
