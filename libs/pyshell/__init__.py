"""
PyShell-specific components for data warehouse jobs.

This module contains all PyShell-dependent code:
- PyShellJobBase: Base class for API ingestion jobs

These components use boto3 and requests, and are used in AWS Glue PyShell jobs.
"""

from .pyshell_job_base import PyShellJobBase

__all__ = [
    "PyShellJobBase",
]
