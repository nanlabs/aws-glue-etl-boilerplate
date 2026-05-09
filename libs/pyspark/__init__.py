"""
PySpark-specific components for data warehouse jobs.

This module contains all PySpark-dependent code:
- MedallionJobBase: Base class for Bronze/Silver/Gold jobs (hybrid local/Glue support)
- SessionFactory: Spark session creation for local and Glue environments
- SessionConfig: Configuration for session factory

These components require PySpark and are used in AWS Glue PySpark jobs and local development.

Usage (Hybrid session):
    from libs.pyspark import SparkSessionFactory, SessionConfig, MedallionJobBase

    # Create session (auto-detects local vs Glue)
    session_info = SparkSessionFactory.create_session()

    # Create job
    if session_info.is_local:
        job = MyJob(session_info.spark, config)
    else:
        job = MyJob(session_info.context['glue_context'], config)
"""

from .layer_job_bases import BronzeJobBase, GoldJobBase, SilverJobBase
from .medallion_job_base import MedallionJobBase
from .session_factory import (
    SessionConfig,
    SessionInfo,
    SparkSessionFactory,
    create_hybrid_session,
    get_spark_session,
)

__all__ = [
    # Core components
    "MedallionJobBase",
    # Layer-specific base classes
    "BronzeJobBase",
    "SilverJobBase",
    "GoldJobBase",
    # Session factory (new hybrid approach)
    "SparkSessionFactory",
    "SessionConfig",
    "SessionInfo",
    "create_hybrid_session",
    "get_spark_session",
]
