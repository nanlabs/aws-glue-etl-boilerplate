from __future__ import annotations

import base64
import json

import boto3
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
        key: str,
        secret_name: str = None,
        region_name: str = None,
        default: str = None,
        json_decode: dict = True,
    ) -> str:
        """
        Get a secret from Secrets Manager by key.
        If the secret does not exist, return the default value.

        :param key: The key to get the secret for.
        :param secret_name: The name of the secret in AWS.
        :param region_name: The AWS region to use.
        :param default: The default value to return if the secret does not exist.
        :param json_decode: Whether to JSON decode the secret.
        :return: The secret value, or the default value if the secret does not exist.
        """
        secret = self.get_secret(secret_name=secret_name, region_name=region_name)
        if secret is None:
            return default
        if json_decode:
            secret = json.loads(secret)
        return secret[key] if key in secret else default

    def get_secret(self, secret_name: str = None, region_name: str = None) -> str:
        """
        Get a secret from Secrets Manager.

        :param secret_name: The name of the secret in AWS.
        :param region_name: The AWS region to use.
        :return: The secret value, or None if the secret does not exist.
        """

        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name,
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
                self.secret_cache[secret_name] = secret
                return secret
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                )
                self.secret_cache[secret_name] = decoded_binary_secret
                return decoded_binary_secret
