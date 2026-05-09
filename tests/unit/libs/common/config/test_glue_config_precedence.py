"""
Unit tests for AWS Glue configuration precedence with workflow properties.

Tests focus on:
- Four-tier parameter resolution (Workflow → CLI → ENV → Default)
- Precedence validation for all levels
- Error handling when workflow properties unavailable
- Fallback behavior in different scenarios

Fast execution without external dependencies (GlueContext, AWS services).
"""

import os
import sys
from unittest.mock import patch

import pytest
from pydantic import Field

from libs.common.config.config_base import ConfigBase


class SampleConfig(ConfigBase):
    """Sample configuration class for testing."""

    test_param: str = Field(default="default_value")


class SampleConfigRequired(ConfigBase):
    """Sample configuration class with required field for testing."""

    test_param: str = Field(...)


class TestGlueConfigPrecedence:
    """Test suite for AWS Glue configuration precedence."""

    @pytest.mark.unit
    def test_workflow_properties_highest_priority(self):
        """Test that workflow properties have highest priority."""
        # Setup workflow properties
        workflow_props = {"test_param": "workflow_value"}

        with patch(
            "libs.common.config.config_base.ConfigBase._get_workflow_properties",
            return_value=workflow_props,
        ):
            # Clear CLI args and env vars
            original_argv = sys.argv[:]
            sys.argv = ["test.py"]
            original_env = os.environ.pop("TEST_PARAM", None)

            try:
                config = SampleConfig.from_args(job_name="test_job")
                assert config.test_param == "workflow_value"
                assert "workflow_properties" in config.get_parameter_source(
                    "test_param"
                )
            finally:
                sys.argv = original_argv
                if original_env:
                    os.environ["TEST_PARAM"] = original_env

    @pytest.mark.unit
    def test_job_parameter_overrides_env_var(self):
        """Test that job parameters override environment variables."""
        # Setup env var
        original_env = os.environ.get("TEST_PARAM")
        os.environ["TEST_PARAM"] = "env_value"

        # Setup CLI arg
        original_argv = sys.argv[:]
        sys.argv = ["test.py", "--test_param", "cli_value"]

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value={},
            ):
                config = SampleConfig.from_args(job_name="test_job")
                assert config.test_param == "cli_value"
                assert "job_parameter" in config.get_parameter_source("test_param")
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env
            elif "TEST_PARAM" in os.environ:
                del os.environ["TEST_PARAM"]

    @pytest.mark.unit
    def test_env_var_overrides_default(self):
        """Test that environment variables override defaults."""
        # Setup env var
        original_env = os.environ.get("TEST_PARAM")
        os.environ["TEST_PARAM"] = "env_value"

        # Clear CLI args
        original_argv = sys.argv[:]
        sys.argv = ["test.py"]

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value={},
            ):
                config = SampleConfig.from_args(job_name="test_job")
                assert config.test_param == "env_value"
                assert "environment" in config.get_parameter_source("test_param")
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env
            elif "TEST_PARAM" in os.environ:
                del os.environ["TEST_PARAM"]

    @pytest.mark.unit
    def test_default_value_when_nothing_set(self):
        """Test that default value is used when nothing else is set."""
        # Clear everything
        original_argv = sys.argv[:]
        sys.argv = ["test.py"]
        original_env = os.environ.pop("TEST_PARAM", None)

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value={},
            ):
                config = SampleConfig.from_args(job_name="test_job")
                assert config.test_param == "default_value"
                assert config.get_parameter_source("test_param") == "default"
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env

    @pytest.mark.unit
    def test_workflow_properties_fallback_when_not_available(self):
        """Test that system falls back when workflow properties not available."""
        # Setup env var
        original_env = os.environ.get("TEST_PARAM")
        os.environ["TEST_PARAM"] = "env_value"

        # Clear CLI args
        original_argv = sys.argv[:]
        sys.argv = ["test.py"]

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value={},  # No workflow properties
            ):
                config = SampleConfig.from_args(job_name="test_job")
                # Should fall back to env var
                assert config.test_param == "env_value"
                assert "environment" in config.get_parameter_source("test_param")
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env
            elif "TEST_PARAM" in os.environ:
                del os.environ["TEST_PARAM"]

    @pytest.mark.unit
    def test_workflow_properties_overrides_cli_and_env(self):
        """Test that workflow properties override both CLI and env vars."""
        # Setup both CLI and env var
        original_env = os.environ.get("TEST_PARAM")
        os.environ["TEST_PARAM"] = "env_value"
        original_argv = sys.argv[:]
        sys.argv = ["test.py", "--test_param", "cli_value"]

        # Setup workflow properties
        workflow_props = {"test_param": "workflow_value"}

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value=workflow_props,
            ):
                config = SampleConfig.from_args(job_name="test_job")
                # Workflow should win
                assert config.test_param == "workflow_value"
                assert "workflow_properties" in config.get_parameter_source(
                    "test_param"
                )
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env
            elif "TEST_PARAM" in os.environ:
                del os.environ["TEST_PARAM"]

    @pytest.mark.unit
    def test_workflow_properties_partial_coverage(self):
        """Test that workflow properties only override specific fields."""
        # Setup workflow properties for one field only
        workflow_props = {"test_param": "workflow_value"}

        original_argv = sys.argv[:]
        sys.argv = ["test.py"]
        original_env = os.environ.pop("TEST_PARAM", None)

        try:
            with patch(
                "libs.common.config.config_base.ConfigBase._get_workflow_properties",
                return_value=workflow_props,
            ):
                config = SampleConfig.from_args(job_name="test_job")
                # Workflow property should be used
                assert config.test_param == "workflow_value"
                # Other fields should use defaults
                assert config.log_level == "INFO"  # Default from ConfigBase
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env

    @pytest.mark.unit
    def test_get_workflow_properties_handles_exceptions(self):
        """Test that _get_workflow_properties handles exceptions gracefully."""
        original_argv = sys.argv[:]
        sys.argv = ["test.py"]
        original_env = os.environ.pop("TEST_PARAM", None)

        try:
            # Mock an exception in get_workflow_run_properties (imported inside the method)
            with patch(
                "libs.common.config.glue_config_resolver.get_workflow_run_properties",
                side_effect=Exception("Workflow error"),
            ):
                # Should not raise, just return empty dict
                workflow_props = SampleConfig._get_workflow_properties()  # noqa: SLF001
                assert workflow_props == {}

                # Should fall back to default
                config = SampleConfig.from_args(job_name="test_job")
                assert config.test_param == "default_value"
        finally:
            sys.argv = original_argv
            if original_env:
                os.environ["TEST_PARAM"] = original_env
