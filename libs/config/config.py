from ..common import cached_property
from .env import get_envs
from .secrets import get_secrets_resolver


class Config:
    """
    Get the credentials from the AWS Secret Manager or from the environment variables
    """

    def __init__(self, args: dict = {}) -> None:
        self.env = get_envs(args)
        self.sr = get_secrets_resolver(
            udb_secret_name=self.udb_secret_name,
            analyticsdb_secret_name=self.analyticsdb_secret_name,
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
    def datalake_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client and the datalake's bucket name.

        :return: A json with the parameters.
        """
        return {
            **self.aws_client_vars,
            "bucket_name": self.datalake_bucket_name,
        }

    @cached_property
    def scans_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client and the scan's bucket name.

        :return: A json with the parameters.
        """
        return {
            **self.aws_client_vars,
            "bucket_name": self.scans_bucket_name,
        }

    @cached_property
    def ims_vars(self) -> dict:
        """
        Get connection parameters for the IMS Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.ims_engine,
            "host": self.ims_host,
            "port": self.ims_port,
            "user": self.ims_user,
            "password": self.ims_password,
            "database": self.ims_database,
        }

    @cached_property
    def udb_vars(self) -> dict:
        """
        Get connection parameters for the Unified Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.udb_engine,
            "host": self.udb_host,
            "port": self.udb_port,
            "database": self.udb_database,
            "user": self.udb_user,
            "password": self.udb_password,
            "ssl": self.udb_ssl,
        }

    @cached_property
    def analyticsdb_vars(self) -> dict:
        """
        Get connection parameters for the Unified Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.analyticsdb_engine,
            "host": self.analyticsdb_host,
            "port": self.analyticsdb_port,
            "database": self.analyticsdb_database,
            "user": self.analyticsdb_user,
            "password": self.analyticsdb_password,
        }

    @cached_property
    def workflow_submission_vars(self) -> dict:
        """
        Get connection parameters for Submission's Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.submission_engine,
            "database": self.submission_database,
            "host": self.submission_host,
            "port": self.submission_port,
            "user": self.submission_user,
            "password": self.submission_password,
        }

    @cached_property
    def quote_service_submission_vars(self) -> dict:
        """
        Get connection parameters for Submission's Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.submission_engine,
            "database": "quote-service",
            "host": self.submission_host,
            "port": self.submission_port,
            "user": self.submission_user,
            "password": self.submission_password,
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
    def submission_engine(self) -> str:
        """
        Get the Engine for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_engine() or self.sr.get_submission_engine()

    @cached_property
    def submission_database(self) -> str:
        """
        Get the Name for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_database() or self.sr.get_submission_database()

    @cached_property
    def submission_host(self) -> str:
        """
        Get the Host for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_host() or self.sr.get_submission_host()

    @cached_property
    def submission_port(self) -> str:
        """
        Get the Port for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_port() or self.sr.get_submission_port()

    @cached_property
    def submission_user(self) -> str:
        """
        Get the User for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_user() or self.sr.get_submission_user()

    @cached_property
    def submission_password(self) -> str:
        """
        Get the Password for the submission database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_submission_password() or self.sr.get_submission_password()

    @cached_property
    def ims_engine(self) -> str:
        """
        Get the Engine for the ims database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_engine() or self.sr.get_ims_engine()

    @cached_property
    def ims_host(self) -> str:
        """
        Get the Host for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_host() or self.sr.get_ims_host()

    @cached_property
    def ims_port(self) -> str:
        """
        Get the Port for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_port() or self.sr.get_ims_port()

    @cached_property
    def ims_user(self) -> str:
        """
        Get the User for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_user() or self.sr.get_ims_user()

    @cached_property
    def ims_password(self) -> str:
        """
        Get the Pasword for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_password() or self.sr.get_ims_password()

    @cached_property
    def ims_database(self) -> str:
        """
        Get the Name for the IMS database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_ims_database() or self.sr.get_ims_database()

    @cached_property
    def udb_engine(self) -> str:
        """
        Get the Engine for the udb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_engine() or self.sr.get_udb_engine()

    @cached_property
    def udb_ssl(self) -> str:
        """
        Get the SSL for the udb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_ssl() or self.sr.get_udb_ssl()

    @cached_property
    def udb_secret_name(self) -> str:
        """
        Get the Secret Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_secret_name() or ""

    @cached_property
    def udb_database(self) -> str:
        """
        Get the Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_database() or self.sr.get_udb_database()

    @cached_property
    def udb_host(self) -> str:
        """
        Get the Host for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_host() or self.sr.get_udb_host()

    @cached_property
    def udb_port(self) -> str:
        """
        Get the Port for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_port() or self.sr.get_udb_port()

    @cached_property
    def udb_user(self) -> str:
        """
        Get the User for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_user() or self.sr.get_udb_user()

    @cached_property
    def udb_password(self) -> str:
        """
        Get the Password for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_udb_password() or self.sr.get_udb_password()

    @cached_property
    def analyticsdb_engine(self) -> str:
        """
        Get the Engine for the analyticsdb database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_engine() or self.sr.get_analyticsdb_engine()

    @cached_property
    def analyticsdb_secret_name(self) -> str:
        """
        Get the Secret Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_secret_name() or ""

    @cached_property
    def analyticsdb_database(self) -> str:
        """
        Get the Name for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_database() or self.sr.get_analyticsdb_database()

    @cached_property
    def analyticsdb_host(self) -> str:
        """
        Get the Host for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_host() or self.sr.get_analyticsdb_host()

    @cached_property
    def analyticsdb_port(self) -> str:
        """
        Get the Port for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_port() or self.sr.get_analyticsdb_port()

    @cached_property
    def analyticsdb_user(self) -> str:
        """
        Get the User for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_user() or self.sr.get_analyticsdb_user()

    @cached_property
    def analyticsdb_password(self) -> str:
        """
        Get the Password for the Unified database or None if it is not set.

        :return: A string with the the value.
        """
        return self.env.get_analyticsdb_password() or self.sr.get_analyticsdb_password()

    @cached_property
    def scans_bucket_name(self) -> str:
        """
        Get the Bucket name for the scans S3 object or None if it is not set.

        :return: A string with the the value or a default value if it is not set.
        """
        return (
            self.env.get_scans_bucket_name()
            or "data-lake.analytics.measuredinsurance.com"
        )

    @cached_property
    def risk_model_version(self) -> str:
        """
        Get the current Risk Model Version.

        :return: A string with the the value or a default value if it is not set.
        """
        return self.env.get_risk_model_version() or "1.10.2.20211031-dev0"

    @cached_property
    def datalake_bucket_name(self) -> str:
        """
        Get the Bucket name for the datalake's bucket S3 object or None if it is not set.

        :return: A string with the the value or an exception if it is not set.
        """
        return self.env.get_datalake_bucket_name() or "data-lake.measuredinsurance.com"

    @cached_property
    def scan_domain(self) -> str:
        """
        Get the domain that is received by the report generator.

        :return: A string with the the value or an empty string if it is not set.
        """
        env = self.env.get_scan_domain()
        if env is not None:
            return env
        raise ValueError("The domain is not set.")

    @cached_property
    def scan_date(self) -> str:
        """
        Get the date that is received by the report generator.

        :return: A string with the the value or an empty string if it is not set.
        """
        env = self.env.get_scan_date()
        if env is not None:
            return env
        raise ValueError("The date is not set.")


config_instance = None


def get_config(args: dict = dict()) -> Config:
    """
    Get the config instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the config.
    :return: The config class instance.
    """
    global config_instance
    if config_instance is None:
        config_instance = Config(args)
    return config_instance
