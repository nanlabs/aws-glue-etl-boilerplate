"""
Unit tests for ClickUp API Client.

Tests cover:
- Client initialization
- Authentication handling
- Pagination logic
- Rate limiting
- Error handling
- Custom fields support
"""

from unittest.mock import Mock, patch

import pytest


class TestClickUpAPIClientInitialization:
    """Test client initialization."""

    def test_client_initializes_with_token(self):
        """Test that client initializes with API token."""
        from libs.common.clickup.client import ClickUpAPIClient

        client = ClickUpAPIClient(api_token="pk_test_token")

        assert client.api_token == "pk_test_token"
        assert client.base_url == "https://api.clickup.com"
        assert client.max_retries == 5
        assert client.rate_limit_per_minute == 100

    @patch("libs.common.clickup.client.urllib3.PoolManager")
    def test_client_sends_correct_headers(self, mock_pool_manager):
        """Test that client sends correct headers."""
        from libs.common.clickup.client import ClickUpAPIClient

        # Mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'{"tasks": []}'
        mock_response.headers = {}

        # Mock request method
        mock_request = Mock(return_value=mock_response)
        mock_pool_manager.return_value.request = mock_request

        client = ClickUpAPIClient(api_token="pk_test_token")
        list(client.get_tasks(list_id="12345678"))

        # Verify request was called with correct headers
        assert mock_request.called
        call_args = mock_request.call_args
        headers = call_args.kwargs.get("headers", {})

        assert headers["Authorization"] == "pk_test_token"
        assert headers["Content-Type"] == "application/json"

    def test_client_uses_custom_base_url(self):
        """Test that client can use custom base URL."""
        from libs.common.clickup.client import ClickUpAPIClient

        custom_url = "https://api.clickup.com"
        client = ClickUpAPIClient(api_token="pk_test_token", base_url=custom_url)

        assert client.base_url == custom_url

    def test_client_configures_rate_limit(self):
        """Test that client configures rate limit per minute."""
        from libs.common.clickup.client import ClickUpAPIClient

        client = ClickUpAPIClient(api_token="pk_test_token", rate_limit_per_minute=50)

        assert client.rate_limit_per_minute == 50
        # Should calculate min_request_interval
        assert client.min_request_interval == 60.0 / 50


class TestClickUpAPIClientMethods:
    """Test client methods for different operations."""

    @patch("libs.common.clickup.client.ClickUpAPIClient._get_tasks_paginated")
    def test_get_tasks_calls_paginated_method(self, mock_get_tasks_paginated):
        """Test that get_tasks calls _get_tasks_paginated."""
        from libs.common.clickup.client import ClickUpAPIClient

        client = ClickUpAPIClient(api_token="pk_test_token")
        mock_get_tasks_paginated.return_value = iter([])

        list(client.get_tasks(list_id="12345678"))

        mock_get_tasks_paginated.assert_called_once()

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_get_task_by_id(self, mock_make_request):
        """Test that get_task retrieves a single task."""
        from libs.common.clickup.client import ClickUpAPIClient

        mock_make_request.return_value = {
            "id": "86dyf02b2",
            "name": "Test task",
        }

        client = ClickUpAPIClient(api_token="pk_test_token")
        task = client.get_task(task_id="86dyf02b2")

        assert task["id"] == "86dyf02b2"
        mock_make_request.assert_called_once()

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_get_list_custom_fields(self, mock_make_request):
        """Test that get_list_custom_fields retrieves custom fields."""
        from libs.common.clickup.client import ClickUpAPIClient

        mock_make_request.return_value = {
            "fields": [
                {"id": "abc123", "name": "Custom Field 1"},
                {"id": "def456", "name": "Custom Field 2"},
            ]
        }

        client = ClickUpAPIClient(api_token="pk_test_token")
        fields = client.get_list_custom_fields(list_id="12345678")

        assert len(fields) == 2
        mock_make_request.assert_called_once()


class TestClickUpAPIClientPagination:
    """Test pagination logic."""

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_pagination_handles_multiple_pages(self, mock_make_request):
        """Test that pagination handles multiple pages."""
        from libs.common.clickup.client import ClickUpAPIClient

        # Mock paginated responses (ClickUp returns 100 tasks per page typically)
        mock_make_request.side_effect = [
            {
                "tasks": [{"id": str(i)} for i in range(100)],  # Full page
            },
            {
                "tasks": [{"id": str(i)} for i in range(100, 150)],  # Partial page
            },
        ]

        client = ClickUpAPIClient(api_token="pk_test_token")
        tasks = list(client.get_tasks(list_id="12345678"))

        assert len(tasks) == 150
        assert mock_make_request.call_count == 2

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_pagination_stops_when_less_than_100_tasks(self, mock_make_request):
        """Test that pagination stops when page has fewer than 100 tasks."""
        from libs.common.clickup.client import ClickUpAPIClient

        mock_make_request.return_value = {
            "tasks": [{"id": "1"}, {"id": "2"}],  # Less than 100
        }

        client = ClickUpAPIClient(api_token="pk_test_token")
        tasks = list(client.get_tasks(list_id="12345678"))

        assert len(tasks) == 2
        # Should only call once since page has < 100 tasks
        assert mock_make_request.call_count == 1


class TestClickUpAPIClientRateLimiting:
    """Test rate limiting logic."""

    @patch("time.sleep")
    @patch("time.time")
    def test_rate_limiting_enforces_minimum_interval(self, mock_time, mock_sleep):
        """Test that rate limiting enforces minimum interval between requests."""
        from libs.common.clickup.client import ClickUpAPIClient

        # Mock time to simulate rapid requests
        mock_time.side_effect = [0.0, 0.1]  # 0.1 seconds between requests

        client = ClickUpAPIClient(api_token="pk_test_token", rate_limit_per_minute=100)
        # min_request_interval = 60/100 = 0.6 seconds

        client._enforce_rate_limit()  # First call
        client._enforce_rate_limit()  # Second call (should sleep)

        # Should sleep for 0.5 seconds (0.6 - 0.1)
        mock_sleep.assert_called_once()
        assert abs(mock_sleep.call_args[0][0] - 0.5) < 0.01


class TestClickUpAPIClientErrorHandling:
    """Test error handling."""

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_handles_authentication_error(self, mock_make_request):
        """Test that authentication errors are handled."""
        from libs.common.clickup.client import (
            ClickUpAPIClient,
            ClickUpAuthenticationError,
        )

        # Mock 401 response
        mock_response = Mock()
        mock_response.status = 401
        mock_response.data = b'{"err": "Invalid token"}'
        mock_response.headers = {}

        mock_make_request.side_effect = ClickUpAuthenticationError("Invalid token")

        client = ClickUpAPIClient(api_token="pk_test_token")

        with pytest.raises(ClickUpAuthenticationError):
            client.get_task(task_id="86dyf02b2")

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_handles_not_found_error(self, mock_make_request):
        """Test that 404 errors are handled."""
        from libs.common.clickup.client import ClickUpAPIClient, ClickUpNotFoundError

        mock_make_request.side_effect = ClickUpNotFoundError("Task not found")

        client = ClickUpAPIClient(api_token="pk_test_token")

        with pytest.raises(ClickUpNotFoundError):
            client.get_task(task_id="nonexistent")

    @patch("libs.common.clickup.client.ClickUpAPIClient._make_request")
    def test_handles_rate_limit_error(self, mock_make_request):
        """Test that rate limit errors trigger retry logic."""
        from libs.common.clickup.client import ClickUpAPIClient, ClickUpRateLimitError

        mock_make_request.side_effect = ClickUpRateLimitError("Rate limit exceeded")

        client = ClickUpAPIClient(api_token="pk_test_token")

        with pytest.raises(ClickUpRateLimitError):
            client.get_task(task_id="86dyf02b2")
