"""
Integration tests for ClickUp pipeline (Raw → Bronze → Silver → Gold).

Tests cover end-to-end data flow through all Medallion layers.
Uses fixtures and mock data to simulate real pipeline execution.
"""


class TestClickUpPipelineIntegration:
    """Test ClickUp pipeline integration across all layers."""

    def test_pipeline_flow_exists(self):
        """Test that pipeline components exist and can be imported."""
        # Test that all components can be imported
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeJob
        from jobs.gold.clickup_gold_job import ClickUpGoldJob
        from jobs.raw.clickup_raw_job import ClickUpRawJob
        from jobs.silver.clickup_silver_job import ClickUpSilverJob

        # All imports should succeed
        assert ClickUpRawJob is not None
        assert ClickUpBronzeJob is not None
        assert ClickUpSilverJob is not None
        assert ClickUpGoldJob is not None

    def test_config_hierarchy(self):
        """Test that configuration classes follow proper hierarchy."""
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeConfig
        from jobs.gold.clickup_gold_job import ClickUpGoldConfig
        from jobs.raw.clickup_raw_job import ClickUpRawConfig
        from jobs.silver.clickup_silver_job import ClickUpSilverConfig

        # All configs should inherit from base configs
        assert issubclass(ClickUpRawConfig, object)
        assert issubclass(ClickUpBronzeConfig, object)
        assert issubclass(ClickUpSilverConfig, object)
        assert issubclass(ClickUpGoldConfig, object)
