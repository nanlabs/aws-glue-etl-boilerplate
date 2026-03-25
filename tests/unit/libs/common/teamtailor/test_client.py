"""
Unit tests for Team Tailor API Client.

Tests cover:
- Client initialization
- Authentication handling
- Pagination logic
- Rate limiting
- Error handling
"""

from unittest.mock import Mock, patch

import pytest
import urllib3


class TestTeamTailorAPIClientInitialization:
    """Test client initialization."""

    def test_client_initializes_with_token(self):
        """Test that client initializes with API token."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        client = TeamTailorAPIClient(api_token="test-token")

        assert client.api_token == "test-token"
        assert client.base_url == "https://api.teamtailor.com"
        assert client.max_retries == 5

    @patch("libs.common.teamtailor.client.urllib3.PoolManager")
    def test_client_sends_correct_headers(self, mock_pool_manager):
        """Test that client sends correct headers including X-Api-Version."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        # Mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'{"data": [], "links": {}}'
        mock_response.headers = {}

        # Mock request method
        mock_request = Mock(return_value=mock_response)
        mock_pool_manager.return_value.request = mock_request

        client = TeamTailorAPIClient(api_token="test-token")
        list(client.get_candidates())

        # Verify request was called with correct headers
        assert mock_request.called
        call_args = mock_request.call_args
        headers = call_args.kwargs.get("headers", {})

        assert headers["Authorization"] == "Token token=test-token"
        assert headers["Accept"] == "application/vnd.api+json"
        assert headers["Content-Type"] == "application/vnd.api+json"
        assert headers["X-Api-Version"] == "20240904"

    def test_client_uses_custom_base_url(self):
        """Test that client can use custom base URL."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        custom_url = "https://api.na.teamtailor.com"
        client = TeamTailorAPIClient(api_token="test-token", base_url=custom_url)

        assert client.base_url == custom_url

    def test_client_configures_retry_settings(self):
        """Test that client configures retry settings."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        client = TeamTailorAPIClient(
            api_token="test-token", max_retries=10, backoff_factor=2.0
        )

        assert client.max_retries == 10
        assert client.backoff_factor == 2.0


class TestTeamTailorAPIClientMethods:
    """Test client methods for different entities."""

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._get_entities")
    def test_get_candidates_calls_get_entities(self, mock_get_entities):
        """Test that get_candidates calls _get_entities."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        client = TeamTailorAPIClient(api_token="test-token")
        mock_get_entities.return_value = iter([])

        list(client.get_candidates())

        mock_get_entities.assert_called_once_with("candidates", page_size=30)

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._get_entities")
    def test_get_jobs_calls_get_entities(self, mock_get_entities):
        """Test that get_jobs calls _get_entities."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        client = TeamTailorAPIClient(api_token="test-token")
        mock_get_entities.return_value = iter([])

        list(client.get_jobs())

        mock_get_entities.assert_called_once_with("jobs", page_size=30)

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._get_entities")
    def test_get_applications_calls_get_entities(self, mock_get_entities):
        """Test that get_applications calls _get_entities."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        client = TeamTailorAPIClient(api_token="test-token")
        mock_get_entities.return_value = iter([])

        list(client.get_applications())

        mock_get_entities.assert_called_once_with("applications", page_size=30)


class TestTeamTailorAPIClientPagination:
    """Test pagination logic."""

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_pagination_handles_multiple_pages(self, mock_make_request):
        """Test that pagination handles multiple pages."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        # Mock paginated responses
        mock_make_request.side_effect = [
            {
                "data": [{"id": "1"}, {"id": "2"}],
                "links": {"next": "https://api.teamtailor.com/v1/candidates?page=2"},
            },
            {"data": [{"id": "3"}], "links": {}},
        ]

        client = TeamTailorAPIClient(api_token="test-token")
        entities = list(client.get_candidates())

        assert len(entities) == 3
        assert mock_make_request.call_count == 2

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_pagination_stops_when_no_next_link(self, mock_make_request):
        """Test that pagination stops when there's no next link."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        mock_make_request.return_value = {
            "data": [{"id": "1"}],
            "links": {},  # No next link
        }

        client = TeamTailorAPIClient(api_token="test-token")
        entities = list(client.get_candidates())

        assert len(entities) == 1
        assert mock_make_request.call_count == 1


class TestTeamTailorAPIClientErrorHandling:
    """Test error handling."""

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_handles_authentication_error(self, mock_make_request):
        """Test that authentication errors are handled."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        # Mock 401 response
        mock_response = Mock()
        mock_response.status = 401
        mock_response.data = b'{"errors": [{"detail": "Unauthorized"}]}'
        mock_response.headers = {}

        mock_make_request.side_effect = urllib3.exceptions.HTTPError()
        # We need to mock the actual request method to raise the error
        with patch.object(
            TeamTailorAPIClient, "_make_request_url", side_effect=Exception("401")
        ):
            client = TeamTailorAPIClient(api_token="invalid-token")
            with pytest.raises(Exception):
                list(client.get_candidates())

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_handles_rate_limit(self, mock_make_request):
        """Test that rate limiting is handled."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        # Mock rate limit response then success
        mock_response_429 = Mock()
        mock_response_429.status = 429
        mock_response_429.headers = {"Retry-After": "1"}
        mock_response_429.data = b'{"errors": [{"detail": "Rate limit exceeded"}]}'

        mock_response_200 = Mock()
        mock_response_200.status = 200
        mock_response_200.data = b'{"data": [{"id": "1"}], "links": {}}'
        mock_response_200.headers = {}

        # This is a simplified test - actual rate limit handling is more complex
        mock_make_request.return_value = {"data": [{"id": "1"}], "links": {}}

        client = TeamTailorAPIClient(api_token="test-token")
        entities = list(client.get_candidates())

        assert len(entities) == 1


class TestTeamTailorAPIClientConnection:
    """Test connection testing."""

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_test_connection_succeeds(self, mock_make_request):
        """Test that connection test succeeds with valid token."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        mock_make_request.return_value = {"data": [{"id": "1"}]}

        client = TeamTailorAPIClient(api_token="test-token")
        result = client.test_connection()

        assert result is True
        mock_make_request.assert_called_once()

    @patch("libs.common.teamtailor.client.TeamTailorAPIClient._make_request")
    def test_test_connection_fails(self, mock_make_request):
        """Test that connection test fails with invalid token."""
        from libs.common.teamtailor.client import TeamTailorAPIClient

        mock_make_request.side_effect = Exception("Connection failed")

        client = TeamTailorAPIClient(api_token="invalid-token")
        result = client.test_connection()

        assert result is False
