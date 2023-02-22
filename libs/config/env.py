import os
from typing import Any


class EnvironmentVariable:
    """
    Get the credentials from the Environment Variables
    """

    args = {}
    cache = {}

    def __init__(self, args: dict = {}) -> None:
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

    def get_submission_database(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Name for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SUBMISSIONS_DB_NAME", default=default, throw_error=throw_error
        )

    def get_submission_host(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Host for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SUBMISSIONS_DB_HOST", default=default, throw_error=throw_error
        )

    def get_submission_port(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Port for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SUBMISSIONS_DB_PORT", default=default, throw_error=throw_error
        )

    def get_submission_user(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the User for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SUBMISSIONS_DB_USER", default=default, throw_error=throw_error
        )

    def get_submission_password(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Password for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SUBMISSIONS_DB_PASSWORD", default=default, throw_error=throw_error
        )

    def get_submission_engine(self, default: Any = None) -> str:
        """
        Get the Engine for the submission database or None if it is not set.

        :param default: The default value if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("SUBMISSIONS_DB_ENGINE", default=default)

    def get_ims_host(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Host for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_HOST", default=default, throw_error=throw_error)

    def get_ims_port(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Port for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_PORT", default=default, throw_error=throw_error)

    def get_ims_user(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the User for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_USER", default=default, throw_error=throw_error)

    def get_ims_password(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Pasword for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_PASSWORD", default=default, throw_error=throw_error)

    def get_ims_database(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Name for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_NAME", default=default, throw_error=throw_error)

    def get_ims_engine(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Engine for the IMS database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("IMS_DB_ENGINE", default=default, throw_error=throw_error)

    def get_udb_secret_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Secret Name for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "UNIFIED_DB_SECRET_NAME", default=default, throw_error=throw_error
        )

    def get_udb_ssl(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the SSL Mode for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("UNIFIED_DB_SSL", default=default, throw_error=throw_error)

    def get_udb_database(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Name for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("UNIFIED_DB_NAME", default=default, throw_error=throw_error)

    def get_udb_host(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Host for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("UNIFIED_DB_HOST", default=default, throw_error=throw_error)

    def get_udb_port(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Port for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("UNIFIED_DB_PORT", default=default, throw_error=throw_error)

    def get_udb_user(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the User for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "UNIFIED_DB_USERNAME", default=default, throw_error=throw_error
        )

    def get_udb_password(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the Password for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "UNIFIED_DB_PASSWORD", default=default, throw_error=throw_error
        )

    def get_udb_engine(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the engine for the Unified database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "UNIFIED_DB_ENGINE", default=default, throw_error=throw_error
        )

    def get_analyticsdb_secret_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Secret Name for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_SECRET_NAME", default=default, throw_error=throw_error
        )

    def get_analyticsdb_database(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Name for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_NAME", default=default, throw_error=throw_error
        )

    def get_analyticsdb_host(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Host for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_HOST", default=default, throw_error=throw_error
        )

    def get_analyticsdb_port(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Port for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_PORT", default=default, throw_error=throw_error
        )

    def get_analyticsdb_user(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the User for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_USERNAME", default=default, throw_error=throw_error
        )

    def get_analyticsdb_password(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Password for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_PASSWORD", default=default, throw_error=throw_error
        )

    def get_analyticsdb_engine(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the engine for the Analytics database or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "ANALYTICS_DB_ENGINE", default=default, throw_error=throw_error
        )

    def get_scans_bucket_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Bucket name for the scans S3 object or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "SCAN_BUCKET_NAME", default=default, throw_error=throw_error
        )

    def get_risk_model_version(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the current Risk Model Version.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "RISK_MODEL_VERSION", default=default, throw_error=throw_error
        )

    def get_datalake_bucket_name(
        self, default: Any = None, throw_error: bool = False
    ) -> str:
        """
        Get the Bucket name for the datalake's bucket S3 object or None if it is not set.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var(
            "DATALAKE_BUCKET_NAME", default=default, throw_error=throw_error
        )

    def get_scan_domain(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the domain that is received by the report generator.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("SCAN_DOMAIN", default=default, throw_error=throw_error)

    def get_scan_date(self, default: Any = None, throw_error: bool = False) -> str:
        """
        Get the date that is received by the report generator.

        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: A string with the the value.
        """
        return self.get_var("SCAN_DATE", default=default, throw_error=throw_error)


envs_instance = None


def get_envs(args: dict = {}) -> EnvironmentVariable:
    """
    Get the envs instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the envs.
    :return: The envs.
    """
    global envs_instance
    if envs_instance is None:
        envs_instance = EnvironmentVariable(args)
    return envs_instance
