"""
Setup configuration for DWH Data Jobs shared libraries.

This setup.py is used to package the shared libraries (libs/) into a Python wheel
for deployment to AWS Glue environments.
"""

import os

from setuptools import find_packages, setup

# Read version from environment or default
version = os.environ.get("PACKAGE_VERSION", "1.0.0")


# Read the README file for long description
def read_readme():
    try:
        with open("README.md", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "Shared libraries for DWH Data Jobs Pipeline"


setup(
    name="nan-data-jobs-libs",
    version=version,
    description="Shared libraries for NaNLABS Data Jobs Pipeline - Medallion Architecture on AWS Glue",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="NaNLABS",
    author_email="engineering@nanlabs.com",
    url="https://github.com/nanlabs/internal-data-lake-jobs",
    # Package discovery
    packages=find_packages(include=["libs", "libs.*"]),
    # Python version requirement
    python_requires=">=3.11",
    # Dependencies (matching Pipfile for AWS Glue compatibility)
    install_requires=[
        # Core dependencies for AWS Glue 5.0
        "pydantic>=2.0.0,<3.0.0",  # Parameter validation and configuration
        "boto3>=1.34.0,<2.0.0",  # AWS SDK
        "botocore>=1.34.0,<2.0.0",  # AWS core library
        "python-dotenv>=1.0.0,<2.0.0",  # Environment variables management
        "pyiceberg>=0.5.0,<1.0.0",  # Apache Iceberg Python library
    ],
    # Optional dependencies for development
    extras_require={
        "dev": [
            "pytest>=7.4.0,<8.0.0",
            "pytest-cov>=4.1.0,<5.0.0",
            "pytest-mock>=3.12.0,<4.0.0",
            "black>=23.12.0,<24.0.0",
            "ruff>=0.1.0",
            "isort>=5.13.0,<6.0.0",
            "autoflake>=2.2.0,<3.0.0",
            "pre-commit>=3.6.0,<4.0.0",
            "flake8-black>=0.3.6",
            "flake8-bugbear>=23.12.0",
        ],
        "testing": [
            "pytest>=7.4.0,<8.0.0",
            "pytest-cov>=4.1.0,<5.0.0",
            "pytest-mock>=3.12.0,<4.0.0",
        ],
        "notebook": [
            "jupyterlab>=4.0.0,<5.0.0",
        ],
        "local-dev": [
            "localstack>=4.7.0,<5.0.0",
            "awscli-local>=0.21,<1.0.0",
        ],
    },
    # Package metadata
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Environment :: Console",
    ],
    # Package configuration
    include_package_data=True,
    zip_safe=False,
    # Keywords for package discovery
    keywords=[
        "aws-glue",
        "data-engineering",
        "etl",
        "medallion-architecture",
        "iceberg",
        "spark",
        "data-pipeline",
        "lakehouse",
    ],
    # Project URLs
    project_urls={
        "Bug Reports": "https://github.com/nanlabs/internal-data-lake-jobs/issues",
        "Source": "https://github.com/nanlabs/internal-data-lake-jobs",
        "Documentation": "https://github.com/nanlabs/internal-data-lake-jobs/tree/main/docs",
    },
)
