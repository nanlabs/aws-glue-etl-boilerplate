"""
Spark Session Factory - Hybrid Local/Glue Support

This module provides a factory for creating Spark sessions that work in both
local development (LocalStack + Derby metastore) and AWS Glue (GlueContext + Glue Data Catalog) environments.

Features:
- Automatic environment detection (local vs Glue)
- Local mode: LocalStack S3A + Derby metastore + Iceberg HadoopCatalog
- Glue mode: GlueContext + Glue Data Catalog + Iceberg GlueCatalog
- Configurable via SessionConfig
- Returns SessionInfo with spark, is_local flag, and context

Usage:
    from libs.pyspark import SparkSessionFactory, SessionConfig

    # Simple usage (auto-detect environment)
    session_info = SparkSessionFactory.create_session()
    spark = session_info.spark
    is_local = session_info.is_local

    # With configuration
    config = SessionConfig(
        app_name="My Job",
        warehouse_path="s3://bucket/warehouse/",
        aws_region="us-west-2"
    )
    session_info = SparkSessionFactory.create_session(config)

    # Use the session
    if session_info.is_local:
        # Local development code
        pass
    else:
        # Glue production code
        glue_context = session_info.context['glue_context']
        job = session_info.context['job']

Author: NaNLABS Data Warehouse Team
Version: 1.0.0
"""

import logging
import os
import shutil
import sys
from collections import namedtuple
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pyspark.sql

logger = logging.getLogger(__name__)


# ==========================================
# CONFIGURATION
# ==========================================
@dataclass
class SessionConfig:
    """
    Configuration for Spark session creation.

    This configuration class provides all necessary parameters for creating
    both local and Glue Spark sessions.

    Attributes:
        app_name: Spark application name (default: "SparkApp")
        warehouse_path: S3 location for Iceberg warehouse (required for Iceberg)
        aws_region: AWS region (default: "us-east-1")
        s3_endpoint: LocalStack endpoint for local mode (default: "http://localstack:4566")
        aws_access_key_id: AWS access key for local mode (default: "test")
        aws_secret_access_key: AWS secret key for local mode (default: "test")
        enable_hive_support: Enable Hive catalog support (default: True)
        json_serde_jars_path: Path to JSON SerDe JARs (default: "/usr/share/java/Hive-JSON-Serde")
        derby_metastore_path: Derby metastore database path (default: "/tmp/metastore_db_local")
        spark_warehouse_dir: Spark warehouse directory (default: "/tmp/spark-warehouse-local")
        hadoop_conf_dir: Custom Hadoop config directory (default: "/tmp/hadoop-conf-local")
        spark_temp_dir: Spark temporary directory (default: "/tmp/spark-temp-local")
        iceberg_catalog_name: Iceberg catalog name (default: "glue_catalog")

    Example:
        config = SessionConfig(
            app_name="Team Tailor Bronze",
            warehouse_path="s3://my-bucket/warehouse/",
            aws_region="us-west-2"
        )
    """

    # Basic configuration
    app_name: str = "SparkApp"
    warehouse_path: Optional[str] = None
    aws_region: str = "us-east-1"

    # Local mode configuration (LocalStack)
    s3_endpoint: str = "http://localstack:4566"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    # Hive configuration
    enable_hive_support: bool = True
    json_serde_jars_path: str = "/usr/share/java/Hive-JSON-Serde"

    # Derby metastore configuration (local mode)
    derby_metastore_path: str = "/tmp/metastore_db_local"
    spark_warehouse_dir: str = "/tmp/spark-warehouse-local"
    hadoop_conf_dir: str = "/tmp/hadoop-conf-local"
    spark_temp_dir: str = "/tmp/spark-temp-local"

    # Iceberg configuration
    iceberg_catalog_name: str = "glue_catalog"

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.warehouse_path and not self.warehouse_path.startswith("s3"):
            raise ValueError(
                f"warehouse_path must start with s3://, got: {self.warehouse_path}"
            )


# SessionInfo named tuple for return value
SessionInfo = namedtuple("SessionInfo", ["spark", "is_local", "context"])


# ==========================================
# SPARK SESSION FACTORY
# ==========================================
class SparkSessionFactory:
    """
    Factory for creating Spark sessions in hybrid environments.

    This factory automatically detects the environment (local vs Glue) and creates
    the appropriate Spark session with all necessary configurations.

    Key features:
    - Environment auto-detection
    - Local mode: Custom HADOOP_CONF_DIR, Derby metastore, LocalStack S3A
    - Glue mode: GlueContext, Glue Data Catalog, Iceberg GlueCatalog
    - Iceberg catalog configuration for both modes
    - External table support (Hive catalog) for both modes

    Usage:
        # Auto-detect and create
        session_info = SparkSessionFactory.create_session()

        # With custom configuration
        config = SessionConfig(app_name="MyJob", warehouse_path="s3://bucket/")
        session_info = SparkSessionFactory.create_session(config)
    """

    @staticmethod
    def detect_environment() -> str:
        """
        Detect if running in AWS Glue or local environment.

        Detection logic:
        - Checks for --JOB_NAME in command line arguments (Glue jobs always have this)
        - Checks for GLUE_VERSION environment variable (set by Glue)

        Returns:
            str: 'glue' or 'local'

        Example:
            env = SparkSessionFactory.detect_environment()
            if env == 'glue':
                print("Running in AWS Glue")
            else:
                print("Running locally")
        """
        # Check for AWS Glue specific indicators
        if "--JOB_NAME" in sys.argv or os.environ.get("GLUE_VERSION"):
            return "glue"
        return "local"

    @staticmethod
    def create_session(config: Optional[SessionConfig] = None) -> SessionInfo:
        """
        Create or get existing Spark session based on environment.

        This is the main entry point for creating Spark sessions. It automatically
        detects the environment and creates the appropriate session. If a session
        already exists (e.g., created by workflow properties retrieval), it will
        be reused to avoid conflicts.

        Args:
            config: Optional SessionConfig instance. If not provided, uses defaults.

        Returns:
            SessionInfo: Named tuple with (spark, is_local, context)
                - spark: SparkSession instance
                - is_local: Boolean indicating if running locally
                - context: Dictionary with environment-specific context
                    - Local: {'type': 'local'}
                    - Glue: {'glue_context': GlueContext, 'job': Job, 'type': 'glue'}

        Example:
            # Simple usage
            session_info = SparkSessionFactory.create_session()
            spark = session_info.spark

            # With configuration
            config = SessionConfig(app_name="MyJob")
            session_info = SparkSessionFactory.create_session(config)

            # Access context
            if not session_info.is_local:
                job = session_info.context['job']
                job.commit()
        """
        # Use default config if not provided
        if config is None:
            config = SessionConfig()

        # Detect environment
        env = SparkSessionFactory.detect_environment()
        logger.info(f"🔍 Detected environment: {env}")

        # Try to reuse existing session if available (e.g., from workflow properties)
        existing_session = SparkSessionFactory._try_get_existing_session(env, config)
        if existing_session is not None:
            logger.info("♻️  Reusing existing Spark session")
            return existing_session

        # Create appropriate session
        if env == "glue":
            return SparkSessionFactory._create_glue_session(config)
        else:
            return SparkSessionFactory._create_local_session(config)

    @staticmethod
    def _try_get_existing_session(
        env: str, config: SessionConfig
    ) -> Optional[SessionInfo]:
        """
        Try to get an existing Spark session if one is already available.

        This is useful when a SparkContext was created earlier (e.g., by workflow
        properties retrieval) and we want to reuse it instead of creating a new one.

        Args:
            env: Environment type ('glue' or 'local')
            config: SessionConfig instance for configuring Iceberg if needed

        Returns:
            SessionInfo if an existing session is found, None otherwise
        """
        try:
            from pyspark.context import SparkContext

            # Check if a SparkContext already exists
            try:
                sc = SparkContext._active_spark_context  # pylint: disable=protected-access
                if sc is None:
                    return None
            except (AttributeError, RuntimeError, ValueError):
                return None

            # If we're in Glue environment and have a SparkContext, try to create GlueContext
            if env == "glue":
                try:
                    from awsglue.context import GlueContext
                    from awsglue.job import Job
                    from awsglue.utils import getResolvedOptions

                    # Get job parameters
                    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

                    # Create GlueContext from existing SparkContext
                    glue_context = GlueContext(sc)
                    spark = glue_context.spark_session
                    job = Job(glue_context)
                    job.init(args["JOB_NAME"], args)

                    logger.info(
                        f"   ✓ Reused existing SparkContext for job: {args['JOB_NAME']}"
                    )

                    # Configure Iceberg with GlueCatalog if warehouse_path is provided
                    if config.warehouse_path:
                        logger.info(
                            f"   ✓ Configuring Iceberg GlueCatalog: {config.warehouse_path}"
                        )
                        spark.conf.set(
                            f"spark.sql.catalog.{config.iceberg_catalog_name}",
                            "org.apache.iceberg.spark.SparkCatalog",
                        )
                        spark.conf.set(
                            f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse",
                            config.warehouse_path,
                        )
                        spark.conf.set(
                            f"spark.sql.catalog.{config.iceberg_catalog_name}.catalog-impl",
                            "org.apache.iceberg.aws.glue.GlueCatalog",
                        )
                        spark.conf.set(
                            f"spark.sql.catalog.{config.iceberg_catalog_name}.io-impl",
                            "org.apache.iceberg.aws.s3.S3FileIO",
                        )
                        logger.info(
                            f"   ✓ Iceberg catalog '{config.iceberg_catalog_name}' configured with GlueCatalog"
                        )

                    context = {"glue_context": glue_context, "job": job, "type": "glue"}
                    return SessionInfo(spark=spark, is_local=False, context=context)
                except (ImportError, AttributeError, ValueError, RuntimeError) as e:
                    logger.debug(
                        "Could not create GlueContext from existing SparkContext: %s", e
                    )
                    return None

            # For local environment, try to get existing SparkSession
            elif env == "local":
                try:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.getOrCreate()
                    if spark.sparkContext == sc:
                        logger.info("   ✓ Reused existing SparkSession")

                        # Configure Iceberg with HadoopCatalog if warehouse_path is provided
                        if config.warehouse_path:
                            SparkSessionFactory._configure_local_iceberg_catalog(
                                spark, config
                            )

                        context = {"type": "local"}
                        return SessionInfo(spark=spark, is_local=True, context=context)
                except (AttributeError, RuntimeError, ValueError):
                    return None

            return None
        except ImportError:
            # PySpark not available
            return None
        except Exception as e:
            logger.debug("Error checking for existing session: %s", e)
            return None

    @staticmethod
    def _create_glue_session(config: SessionConfig) -> SessionInfo:
        """
        Create AWS Glue session with GlueContext and Glue Data Catalog.

        Configuration:
        - GlueContext for AWS Glue integration
        - Glue Data Catalog as Hive metastore (for external tables via spark_catalog)
        - Iceberg GlueCatalog for Iceberg tables
        - Job initialization for proper job tracking

        Args:
            config: SessionConfig instance

        Returns:
            SessionInfo: Session info with Glue context

        Raises:
            ImportError: If AWS Glue libraries are not available
            Exception: If session creation fails
        """
        logger.info("🔧 Initializing AWS Glue session...")

        try:
            from awsglue.context import GlueContext
            from awsglue.job import Job
            from awsglue.utils import getResolvedOptions
            from pyspark.context import SparkContext
        except ImportError as e:
            logger.error(f"Failed to import AWS Glue libraries: {e}")
            raise ImportError(
                "AWS Glue libraries not available. "
                "This should only happen in local environment. "
                "If running in Glue, check your Glue version and Python environment."
            ) from e

        try:
            # Get job parameters (JOB_NAME is required for Glue jobs)
            args = getResolvedOptions(sys.argv, ["JOB_NAME"])

            # Initialize Glue context
            # Use getOrCreate() to reuse existing SparkContext if one was already created
            # (e.g., by workflow properties retrieval in glue_config_resolver)
            sc = SparkContext.getOrCreate()
            glue_context = GlueContext(sc)
            spark = glue_context.spark_session
            job = Job(glue_context)
            job.init(args["JOB_NAME"], args)

            logger.info(f"   ✓ GlueContext created for job: {args['JOB_NAME']}")

            # Configure Iceberg with GlueCatalog
            if config.warehouse_path:
                logger.info(
                    f"   ✓ Configuring Iceberg GlueCatalog: {config.warehouse_path}"
                )
                spark.conf.set(
                    f"spark.sql.catalog.{config.iceberg_catalog_name}",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                spark.conf.set(
                    f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse",
                    config.warehouse_path,
                )
                spark.conf.set(
                    f"spark.sql.catalog.{config.iceberg_catalog_name}.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog",
                )
                spark.conf.set(
                    f"spark.sql.catalog.{config.iceberg_catalog_name}.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
                logger.info(
                    f"   ✓ Iceberg catalog '{config.iceberg_catalog_name}' configured with GlueCatalog"
                )

            logger.info("✅ AWS Glue session created successfully")

            # Return context wrapper with job for commit
            context = {"glue_context": glue_context, "job": job, "type": "glue"}

            return SessionInfo(spark=spark, is_local=False, context=context)

        except Exception as e:
            logger.error(f"Failed to create AWS Glue session: {e}")
            raise

    @staticmethod
    def _create_local_session(config: SessionConfig) -> SessionInfo:
        """
        Create local Spark session with Derby metastore and LocalStack S3A.

        This creates a fully functional local development environment that mimics
        AWS Glue behavior but uses local resources.

        Configuration:
        - Custom HADOOP_CONF_DIR to override EMR's Glue Data Catalog settings
        - Derby embedded metastore for Hive catalog (external tables)
        - LocalStack for S3 access
        - Iceberg HadoopCatalog for Iceberg tables
        - JSON SerDe JARs for JSON external tables

        Key features:
        - External tables work via Derby metastore
        - Iceberg tables work via HadoopCatalog
        - S3 access via LocalStack
        - No AWS credentials required (uses 'test' credentials)

        Args:
            config: SessionConfig instance

        Returns:
            SessionInfo: Session info with local context

        Raises:
            Exception: If session creation fails

        Note:
            This method creates temporary directories for Hadoop config and metastore.
            These directories are not cleaned up automatically.
        """
        logger.info("🔧 Creating local Spark session with Derby metastore...")

        try:
            pass
        except ImportError as e:
            logger.error(f"Failed to import PySpark: {e}")
            raise ImportError(
                "PySpark not available. Please install PySpark: pip install pyspark"
            ) from e

        try:
            # ====== SETUP CUSTOM HIVE CONFIG TO OVERRIDE EMR'S GLUE SETTINGS ======
            SparkSessionFactory._setup_hadoop_config(config)

            # ====== SET ENVIRONMENT VARIABLES FOR AWS/LOCALSTACK ======
            SparkSessionFactory._setup_local_env_vars(config)

            # ====== CREATE SPARK SESSION WITH FULL CONFIGURATION ======
            spark = SparkSessionFactory._build_local_spark_session(config)

            logger.info("   ✓ SparkSession created with Hive support (Derby metastore)")

            # ====== CONFIGURE ICEBERG CATALOG FOR LOCALSTACK ======
            if config.warehouse_path:
                SparkSessionFactory._configure_local_iceberg_catalog(spark, config)

            logger.info("✅ Local Spark session ready (Hive + Iceberg configured)")

            context = {"type": "local"}

            return SessionInfo(spark=spark, is_local=True, context=context)

        except Exception as e:
            logger.error(f"Failed to create local Spark session: {e}")
            raise

    @staticmethod
    def _setup_hadoop_config(config: SessionConfig) -> None:
        """
        Setup custom Hadoop configuration to override EMR's Glue Data Catalog settings.

        This is critical for local development on EMR-based environments. EMR's default
        hive-site.xml configures Hive to use Glue Data Catalog, which won't work locally.

        Steps:
        1. Create custom Hadoop config directory
        2. Copy necessary Hadoop configs from EMR (if available)
        3. Create custom hive-site.xml with Derby metastore configuration
        4. Set HADOOP_CONF_DIR to use custom config

        Args:
            config: SessionConfig instance
        """
        hadoop_conf_dir = config.hadoop_conf_dir
        os.makedirs(hadoop_conf_dir, exist_ok=True)

        # Copy necessary Hadoop configs from EMR (if they exist)
        for conf_file in ["core-site.xml", "hdfs-site.xml"]:
            src = f"/usr/lib/spark/conf/{conf_file}"
            if os.path.exists(src):
                shutil.copy(src, hadoop_conf_dir)
                logger.info(f"   ✓ Copied {conf_file} to custom Hadoop config")

        # Create custom hive-site.xml (Derby, not Glue Data Catalog)
        hive_site_content = f"""<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value></value>
    <description>Empty = embedded Derby metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName={config.derby_metastore_path};create=true</value>
    <description>Derby embedded database connection</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
    <description>Derby JDBC driver</description>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>{config.spark_warehouse_dir}</value>
    <description>Hive warehouse directory</description>
  </property>
</configuration>"""

        hive_site_path = os.path.join(hadoop_conf_dir, "hive-site.xml")
        with open(hive_site_path, "w") as f:
            f.write(hive_site_content)

        # Set HADOOP_CONF_DIR to use our custom config
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf_dir
        logger.info(f"   ✓ Custom Hadoop config directory: {hadoop_conf_dir}")
        logger.info(f"   ✓ Derby metastore: {config.derby_metastore_path}")

    @staticmethod
    def _setup_local_env_vars(config: SessionConfig) -> None:
        """
        Setup environment variables for LocalStack and AWS SDK.

        These environment variables configure the AWS SDK to use LocalStack
        instead of real AWS services.

        Args:
            config: SessionConfig instance
        """
        # Use service-specific endpoint for S3 (not general AWS_ENDPOINT_URL)
        os.environ["AWS_ENDPOINT_URL_S3"] = config.s3_endpoint
        os.environ["AWS_ACCESS_KEY_ID"] = config.aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = config.aws_secret_access_key
        os.environ["AWS_REGION"] = config.aws_region
        logger.info("   ✓ AWS credentials configured for LocalStack (S3 endpoint)")

    @staticmethod
    def _build_local_spark_session(config: SessionConfig):
        """
        Build local Spark session with all necessary configurations.

        This method creates a SparkSession with comprehensive configuration for:
        - Local directories (temp, warehouse, etc.)
        - S3A filesystem for LocalStack
        - AWS SDK configuration
        - Hive catalog with Derby metastore
        - Parquet type conversion
        - JSON SerDe JARs

        Args:
            config: SessionConfig instance

        Returns:
            SparkSession: Configured Spark session
        """
        from pyspark.sql import SparkSession

        # Build list of JSON SerDe JARs
        json_serde_jars = [
            f"{config.json_serde_jars_path}/json-serde.jar",
            f"{config.json_serde_jars_path}/json.jar",
            f"{config.json_serde_jars_path}/json-udf.jar",
            f"{config.json_serde_jars_path}/hive-openx-serde.jar",
        ]
        json_serde_jars_str = ",".join(
            [jar for jar in json_serde_jars if os.path.exists(jar)]
        )

        if json_serde_jars_str:
            logger.info(
                f"   ✓ JSON SerDe JARs: {len(json_serde_jars_str.split(','))} files"
            )

        # Create SparkSession builder
        builder = SparkSession.builder.appName(config.app_name)

        # Add JSON SerDe JARs if available
        if json_serde_jars_str:
            builder = builder.config("spark.jars", json_serde_jars_str)

        # Local directories
        builder = (
            builder.config("spark.local.dir", config.spark_temp_dir)
            .config("spark.hadoop.hadoop.tmp.dir", config.spark_temp_dir)
            .config("spark.hadoop.fs.s3a.buffer.dir", config.spark_temp_dir)
            .config("spark.sql.warehouse.dir", config.spark_warehouse_dir)
        )

        # S3A configuration for LocalStack
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", config.aws_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

        # AWS SDK configuration
        builder = (
            builder.config("spark.hadoop.com.amazonaws.sdk.disableEc2Metadata", "true")
            .config("spark.hadoop.com.amazonaws.util.EC2MetadataUtils.disable", "true")
            .config("spark.hadoop.aws.java.v1.disableDeprecationAnnouncement", "true")
            .config("spark.hadoop.aws.region", config.aws_region)
            .config("spark.hadoop.aws.accessKeyId", config.aws_access_key_id)
            .config("spark.hadoop.aws.secretKey", config.aws_secret_access_key)
        )

        # Java options
        java_opts = (
            "-Dcom.amazonaws.sdk.disableEc2Metadata=true "
            "-Dcom.amazonaws.util.EC2MetadataUtils.disable=true "
            "-Daws.java.v1.disableDeprecationAnnouncement=true"
        )
        builder = builder.config("spark.driver.extraJavaOptions", java_opts).config(
            "spark.executor.extraJavaOptions", java_opts
        )

        # Hive catalog configuration (Derby metastore, not Glue)
        if config.enable_hive_support:
            builder = (
                builder.config("spark.sql.catalogImplementation", "hive")
                .config(
                    "spark.hadoop.hive.metastore.warehouse.dir",
                    config.spark_warehouse_dir,
                )
                .config("spark.hadoop.hive.metastore.uris", "")
                .config(
                    "spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={config.derby_metastore_path};create=true",
                )
                .config(
                    "spark.hadoop.javax.jdo.option.ConnectionDriverName",
                    "org.apache.derby.jdbc.EmbeddedDriver",
                )
            )

        # S3A committer configuration
        builder = builder.config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )

        # Parquet type conversion - handle INT vs INT64 mismatch
        builder = (
            builder.config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        )

        # Enable Hive support if requested
        if config.enable_hive_support:
            builder = builder.enableHiveSupport()

        # Create session
        return builder.getOrCreate()

    @staticmethod
    def _configure_local_iceberg_catalog(spark, config: SessionConfig) -> None:
        """
        Configure Iceberg catalog for LocalStack (HadoopCatalog).

        In local mode, we use Iceberg's HadoopCatalog instead of GlueCatalog
        because we don't have access to AWS Glue Data Catalog.

        Args:
            spark: SparkSession instance
            config: SessionConfig instance
        """
        logger.info("🔧 Configuring Iceberg catalog for LocalStack...")

        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.type", "hadoop"
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse",
            config.warehouse_path,
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.client.region",
            config.aws_region,
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.s3.endpoint",
            config.s3_endpoint,
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.s3.path-style-access",
            "true",
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.s3.access-key-id",
            config.aws_access_key_id,
        )
        spark.conf.set(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.s3.secret-access-key",
            config.aws_secret_access_key,
        )

        logger.info(
            f"   ✓ Iceberg catalog '{config.iceberg_catalog_name}' configured with HadoopCatalog"
        )


# ==========================================
# CONVENIENCE FUNCTIONS
# ==========================================
def create_hybrid_session(
    app_name: str = "SparkApp",
    warehouse_path: Optional[str] = None,
    aws_region: str = "us-east-1",
) -> SessionInfo:
    """
    Convenience function to create a hybrid Spark session with minimal configuration.

    This is a simplified wrapper around SparkSessionFactory.create_session()
    for common use cases.

    Args:
        app_name: Spark application name
        warehouse_path: S3 location for Iceberg warehouse
        aws_region: AWS region

    Returns:
        SessionInfo: Session info with spark, is_local, and context

    Example:
        session_info = create_hybrid_session(
            app_name="My Job",
            warehouse_path="s3://bucket/warehouse/"
        )
        spark = session_info.spark
    """
    config = SessionConfig(
        app_name=app_name, warehouse_path=warehouse_path, aws_region=aws_region
    )
    return SparkSessionFactory.create_session(config)


def get_spark_session(
    config: Optional[SessionConfig] = None,
) -> "pyspark.sql.SparkSession":
    """
    Get Spark session only (without context information).

    This is a convenience function for cases where you only need the SparkSession
    and don't care about the environment type or context.

    Args:
        config: Optional SessionConfig instance

    Returns:
        SparkSession: Configured Spark session

    Example:
        spark = get_spark_session()
        df = spark.read.parquet("s3://bucket/data/")
    """
    session_info = SparkSessionFactory.create_session(config)
    return session_info.spark
