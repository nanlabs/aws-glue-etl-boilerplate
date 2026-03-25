"""
Team Tailor API Client Module

This module provides a client for interacting with the Team Tailor API,
specifically for retrieving talent management data (candidates, jobs, applications, etc.)
with proper authentication, pagination, rate limiting, and error handling.

Features implemented:
- Authentication using API tokens (Bearer token)
- JSON API Specification compliance
- Pagination support for large datasets
- Exponential backoff retry logic for rate limits and network errors
- Comprehensive error handling for authentication and API failures
- urllib3-based HTTP client for compatibility with existing environment

API Documentation: https://docs.teamtailor.com/
"""

import json
import time
from typing import Any, Dict, Iterator, Optional
from urllib.parse import urlencode

import urllib3
from urllib3.util.retry import Retry

from libs.common.utils.logger import get_logger


class TeamTailorAPIError(Exception):
    """Base exception for Team Tailor API errors"""


class TeamTailorAuthenticationError(TeamTailorAPIError):
    """Exception raised for authentication failures"""


class TeamTailorRateLimitError(TeamTailorAPIError):
    """Exception raised for rate limit errors"""


class TeamTailorNotFoundError(TeamTailorAPIError):
    """Exception raised for 404 Not Found errors (endpoint or resource not available)"""


class TeamTailorAPIClient:
    """
    Team Tailor API client with authentication, pagination, and rate limiting support.

    This client handles:
    - API authentication using Bearer tokens
    - JSON API Specification compliance
    - Pagination for large datasets
    - Rate limiting with exponential backoff
    - Comprehensive error handling

    API Documentation: https://docs.teamtailor.com/
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.teamtailor.com",
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        max_backoff: float = 60.0,
    ):
        """
        Initialize the Team Tailor API client.

        Args:
            api_token: Team Tailor API token (Bearer token)
            base_url: Base URL for Team Tailor API
                      Default: https://api.teamtailor.com (EU)
                      Alternative: https://api.na.teamtailor.com (US West)
            max_retries: Maximum number of retry attempts for failed requests
            backoff_factor: Base delay for exponential backoff (seconds)
            max_backoff: Maximum delay between retries (seconds)
        """
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        # Clean and store API token
        self.api_token = (api_token or "").strip()
        self.logger = get_logger(__name__)

        # Validate token is provided
        if not self.api_token:
            self.logger.warning("API token is empty - authentication will fail")

        # Configure HTTP session with retry strategy
        self.session = self._create_session()

        # Log initialization with masked token and base URL
        token_preview = (
            f"{self.api_token[:4]}...{self.api_token[-4:]}"
            if len(self.api_token) > 8
            else "****"
        )
        self.logger.info(
            f"Team Tailor API client initialized successfully "
            f"(base_url: {self.base_url}, token: {token_preview}, length: {len(self.api_token)})"
        )

    def _create_session(self) -> urllib3.PoolManager:
        """
        Create HTTP pool manager with retry configuration.

        Returns:
            urllib3.PoolManager: Configured pool manager
        """
        # Configure retry strategy for transient errors
        retry_kwargs = {
            "total": self.max_retries,
            "status_forcelist": [429, 500, 502, 503, 504],
            "backoff_factor": self.backoff_factor,
        }

        # Try newer parameter name first, fall back to older one for compatibility
        try:
            retry_strategy = Retry(
                allowed_methods=["HEAD", "GET", "OPTIONS"], **retry_kwargs
            )
        except TypeError:
            # Fallback for older urllib3 versions (< 1.26.0) that use method_whitelist
            try:
                retry_strategy = Retry(
                    method_whitelist=["HEAD", "GET", "OPTIONS"],  # type: ignore
                    **retry_kwargs,
                )
            except TypeError:
                # If both fail, create without method restrictions
                retry_strategy = Retry(**retry_kwargs)

        # Create pool manager with retry strategy
        pool_manager = urllib3.PoolManager(
            retries=retry_strategy, timeout=urllib3.Timeout(connect=10.0, read=30.0)
        )

        return pool_manager

    def get_candidates(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve candidates from Team Tailor API with pagination.

        Args:
            page_size: Number of candidates per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual candidate records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("candidates", page_size=page_size, **kwargs)

    def get_jobs(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve jobs from Team Tailor API with pagination.

        Args:
            page_size: Number of jobs per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual job records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("jobs", page_size=page_size, **kwargs)

    def get_applications(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve applications from Team Tailor API with pagination.

        Note: TeamTailor API uses 'job-applications' as the endpoint name.

        Args:
            page_size: Number of applications per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual application records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("job-applications", page_size=page_size, **kwargs)

    def get_interviews(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve interviews from Team Tailor API with pagination.

        Args:
            page_size: Number of interviews per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual interview records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("interviews", page_size=page_size, **kwargs)

    def get_users(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve users (recruiters/hiring managers) from Team Tailor API with pagination.

        Args:
            page_size: Number of users per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual user records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("users", page_size=page_size, **kwargs)

    def get_departments(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve departments from Team Tailor API with pagination.

        Args:
            page_size: Number of departments per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual department records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("departments", page_size=page_size, **kwargs)

    def get_stages(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve stages from Team Tailor API with pagination.

        Args:
            page_size: Number of stages per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual stage records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("stages", page_size=page_size, **kwargs)

    def get_nps_responses(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve NPS (Net Promoter Score) responses from Team Tailor API with pagination.

        This endpoint provides structured NPS survey responses directly from candidates,
        complementing the activity-based approach for NPS data collection.

        Args:
            page_size: Number of NPS responses per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual NPS response records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
            TeamTailorNotFoundError: If endpoint returns 404 (not available for account)

        Reference: https://docs.teamtailor.com/#55639fbf-f69a-4981-b775-05522a00f5fb
        """
        return self._get_entities("nps-responses", page_size=page_size, **kwargs)

    def get_custom_fields(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Retrieve custom field definitions from Team Tailor API with pagination.

        Custom field definitions provide metadata about custom fields including:
        - Field name
        - Field type (Select, MultiSelect, Number, etc.)
        - Field configuration

        Args:
            page_size: Number of custom fields per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual custom field definition records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        return self._get_entities("custom-fields", page_size=page_size, **kwargs)

    def get_custom_field_values(
        self,
        entity_type: str,
        entity_id: str,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve custom-field-values for a specific entity.

        Args:
            entity_type: Entity type (candidates, jobs, etc.)
            entity_id: Entity ID
            page_size: Number of custom field values per page (default: 30, max: 30)

        Yields:
            Dict: Individual custom-field-value records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorNotFoundError: If entity or custom fields not found
        """
        # Use relationship endpoint to get custom-field-values
        custom_fields_url = f"/v1/{entity_type}/{entity_id}/custom-field-values"
        return self._get_entities_from_url(
            f"{self.base_url}{custom_fields_url}",
            page_size=min(page_size, 30),  # TeamTailor max is 30
        )

    def get_activities(
        self,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        DEPRECATED: Retrieve activities from Team Tailor API with pagination.

        This method is deprecated because the /v1/activities endpoint may not be available
        in all TeamTailor accounts. Use get_activities_as_relationships() instead.

        Args:
            page_size: Number of activities per page (default: 30, max: 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual activity records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
            TeamTailorNotFoundError: If endpoint returns 404 (not available for account)
        """
        return self._get_entities("activities", page_size=page_size, **kwargs)

    def get_activities_as_relationships(
        self,
        parent_resource: str,
        parent_id: str,
        activity_types: Optional[list] = None,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve activities as relationships from a parent resource (candidate, application, job, user).

        This is the recommended approach for accessing activities, as it works even when
        the /v1/activities endpoint is not available. Activities are fetched using the
        JSON API include parameter or relationship links.

        Args:
            parent_resource: Parent resource type ('candidates', 'job-applications', 'jobs', 'users')
            parent_id: ID of the parent resource
            activity_types: Optional filter for specific activity types (e.g., ['stage-transition', 'note', 'email'])
            page_size: Number of activities per page (default: 30, max: 100)

        Yields:
            Dict: Individual activity records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        self.logger.debug(
            f"Fetching activities for {parent_resource}/{parent_id} "
            f"(activity_types: {activity_types or 'all'})"
        )

        # First, try to get activities from relationship link (most reliable)
        try:
            # Get parent resource to check for activities relationship
            response_data = self._make_request(
                f"/v1/{parent_resource}/{parent_id}", params={"include": "activities"}
            )

            parent_data = response_data.get("data", {})
            relationships = parent_data.get("relationships", {})
            activities_rel = relationships.get("activities", {})

            # Try relationship link first (supports pagination)
            if activities_rel.get("links", {}).get("related"):
                activities_url = activities_rel["links"]["related"]
                for activity in self._get_entities_from_url(
                    activities_url, page_size=page_size
                ):
                    # Filter by activity type if specified
                    if activity_types:
                        activity_type = activity.get("attributes", {}).get(
                            "activity-type"
                        )
                        if activity_type not in activity_types:
                            continue
                    yield activity
                return

            # Fallback: extract from included section
            included = response_data.get("included", [])
            activities = [item for item in included if item.get("type") == "activities"]

            for activity in activities:
                # Filter by activity type if specified
                if activity_types:
                    activity_type = activity.get("attributes", {}).get("activity-type")
                    if activity_type not in activity_types:
                        continue
                yield activity

        except TeamTailorNotFoundError:
            # Try direct relationship endpoint as fallback
            try:
                rel_url = f"/v1/{parent_resource}/{parent_id}/relationships/activities"
                for activity in self._get_entities_from_url(
                    f"{self.base_url}{rel_url}", page_size=page_size
                ):
                    # Filter by activity type if specified
                    if activity_types:
                        activity_type = activity.get("attributes", {}).get(
                            "activity-type"
                        )
                        if activity_type not in activity_types:
                            continue
                    yield activity
            except TeamTailorNotFoundError:
                self.logger.debug(
                    f"No activities relationship found for {parent_resource}/{parent_id}"
                )
                return

    def get_candidate_activities(
        self,
        candidate_id: str,
        activity_types: Optional[list] = None,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve activities for a specific candidate.

        Args:
            candidate_id: Candidate ID
            activity_types: Optional filter for specific activity types
            page_size: Number of activities per page (default: 30, max: 100)

        Yields:
            Dict: Individual activity records (JSON API format)
        """
        return self.get_activities_as_relationships(
            "candidates", candidate_id, activity_types, page_size
        )

    def get_application_activities(
        self,
        application_id: str,
        activity_types: Optional[list] = None,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve activities for a specific application.

        Args:
            application_id: Application ID
            activity_types: Optional filter for specific activity types
            page_size: Number of activities per page (default: 30, max: 100)

        Yields:
            Dict: Individual activity records (JSON API format)
        """
        return self.get_activities_as_relationships(
            "job-applications", application_id, activity_types, page_size
        )

    def get_job_activities(
        self,
        job_id: str,
        activity_types: Optional[list] = None,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve activities for a specific job.

        Args:
            job_id: Job ID
            activity_types: Optional filter for specific activity types
            page_size: Number of activities per page (default: 30, max: 100)

        Yields:
            Dict: Individual activity records (JSON API format)
        """
        return self.get_activities_as_relationships(
            "jobs", job_id, activity_types, page_size
        )

    def _get_entities_from_url(
        self,
        url: str,
        page_size: int = 30,
    ) -> Iterator[Dict]:
        """
        Retrieve entities from a full URL with pagination support.

        Used for relationship links that return full URLs.

        Args:
            url: Full URL to fetch from
            page_size: Number of entities per page (max: 100)

        Yields:
            Dict: Individual entity records (JSON API format)
        """
        next_url = url
        total_entities = 0

        while next_url:
            try:
                # Parse URL to add page size if not present
                if "page[size]" not in next_url:
                    separator = "&" if "?" in next_url else "?"
                    next_url = f"{next_url}{separator}page[size]={min(page_size, 100)}"

                response_data = self._make_request_url(next_url, params=None)

                # Extract entities from response
                entities = response_data.get("data", [])
                if not entities:
                    break

                for entity in entities:
                    total_entities += 1
                    yield entity

                # Check for next page
                links = response_data.get("links", {})
                next_url = links.get("next")

            except TeamTailorNotFoundError:
                self.logger.debug(f"Activities URL not found: {url}")
                break
            except Exception as e:
                self.logger.error(f"Error fetching activities from URL {url}: {str(e)}")
                raise TeamTailorAPIError(
                    f"Failed to retrieve activities: {str(e)}"
                ) from e

        self.logger.debug(f"Retrieved {total_entities} activities from {url}")

    def _get_entities(
        self,
        entity_type: str,
        page_size: int = 30,
        **kwargs,
    ) -> Iterator[Dict]:
        """
        Generic method to retrieve entities from Team Tailor API with pagination.

        Team Tailor API uses JSON API Specification with pagination via 'links.next'.

        Args:
            entity_type: Entity type (candidates, jobs, applications, etc.)
            page_size: Number of entities per page (max 100)
            **kwargs: Additional query parameters

        Yields:
            Dict: Individual entity records (JSON API format)

        Raises:
            TeamTailorAPIError: For API-related errors
            TeamTailorAuthenticationError: For authentication failures
        """
        next_url = None
        total_entities = 0

        self.logger.info(f"Starting {entity_type} retrieval - Page size: {page_size}")

        while True:
            try:
                # Build query parameters
                params = {
                    "page[size]": min(page_size, 100),  # API max is 100
                    **kwargs,
                }

                # Make API request
                if next_url:
                    # Use next URL directly (includes pagination params)
                    response_data = self._make_request_url(next_url)
                else:
                    # First request - build endpoint
                    response_data = self._make_request(f"/v1/{entity_type}", params)

                # Extract entities from response (JSON API format)
                entities = response_data.get("data", [])
                if not entities:
                    self.logger.info(f"No more {entity_type} to process")
                    break

                # Yield individual entities
                for entity in entities:
                    total_entities += 1
                    yield entity

                # Check for next page (JSON API links)
                links = response_data.get("links", {})
                next_url = links.get("next")

                if not next_url:
                    self.logger.info("Reached end of pagination")
                    break

                self.logger.debug(
                    f"Processed page with {len(entities)} {entity_type}. "
                    f"Total so far: {total_entities}. Next URL available."
                )

            except TeamTailorRateLimitError:
                # Rate limit handling is done in _make_request
                continue
            except Exception as e:
                self.logger.error(f"Error retrieving {entity_type}: {str(e)}")
                raise TeamTailorAPIError(
                    f"Failed to retrieve {entity_type}: {str(e)}"
                ) from e

        self.logger.info(
            f"{entity_type.capitalize()} retrieval completed. Total: {total_entities}"
        )

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to Team Tailor API with error handling and rate limiting.

        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters

        Returns:
            Dict: API response data (JSON API format)

        Raises:
            TeamTailorAuthenticationError: For authentication failures
            TeamTailorRateLimitError: For rate limit errors (after retries)
            TeamTailorAPIError: For other API errors
        """
        url = f"{self.base_url}{endpoint}"
        return self._make_request_url(url, params)

    def _make_request_url(self, url: str, params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to a full URL with error handling and rate limiting.

        Args:
            url: Full URL (for pagination)
            params: Optional query parameters (usually None for pagination URLs)

        Returns:
            Dict: API response data (JSON API format)

        Raises:
            TeamTailorAuthenticationError: For authentication failures
            TeamTailorRateLimitError: For rate limit errors (after retries)
            TeamTailorAPIError: For other API errors
        """
        retry_count = 0

        # Clean and validate API token
        api_token = (self.api_token or "").strip()
        if not api_token:
            raise TeamTailorAuthenticationError("API token is empty or not set")

        # Build headers (Team Tailor uses Bearer token authentication)
        headers = {
            "Authorization": f"Token token={api_token}",
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-Api-Version": "20240904",
        }

        # Log token info (masked for security)
        token_preview = (
            f"{api_token[:4]}...{api_token[-4:]}" if len(api_token) > 8 else "****"
        )
        self.logger.debug(
            f"Making request to {url} with token: {token_preview} (length: {len(api_token)})"
        )

        while retry_count <= self.max_retries:
            try:
                # Build URL with query parameters
                if params:
                    query_string = urlencode(params)
                    full_url = f"{url}?{query_string}"
                else:
                    full_url = url

                self.logger.debug(
                    f"Making request to {full_url} (base_url: {self.base_url})"
                )

                response = self.session.request("GET", full_url, headers=headers)

                # Handle rate limiting
                if response.status == 429:
                    retry_after = self._handle_rate_limit(response, retry_count)
                    if retry_after is not None:
                        retry_count += 1
                        continue
                    else:
                        raise TeamTailorRateLimitError(
                            "Rate limit exceeded and max retries reached"
                        )

                # Handle authentication errors
                if response.status == 401:
                    error_msg = self._extract_error_message(response)
                    self.logger.error(
                        f"Authentication failed (401) - {error_msg}. "
                        f"Token length: {len(api_token)}, "
                        f"Token preview: {token_preview}"
                    )
                    raise TeamTailorAuthenticationError(
                        f"Invalid API token or authentication failed: {error_msg}"
                    )

                # Handle 404 Not Found errors
                if response.status == 404:
                    error_msg = self._extract_error_message(response)
                    self.logger.error(
                        f"Resource not found (404) - {error_msg}. "
                        f"Endpoint may not exist or may not be available for this account."
                    )
                    raise TeamTailorNotFoundError(
                        f"Resource not found (404): {error_msg}. "
                        f"This endpoint may not be available or may require different permissions."
                    )

                # Handle other client errors
                if response.status >= 400:
                    error_msg = self._extract_error_message(response)
                    self.logger.error(
                        f"API request failed: {response.status} - {error_msg}"
                    )
                    raise TeamTailorAPIError(
                        f"API request failed: {response.status} - {error_msg}"
                    )

                # Log rate limit info for monitoring
                self._log_rate_limit_info(response)

                # Parse and return response (JSON API format)
                response_data = json.loads(response.data.decode("utf-8"))
                self.logger.debug(
                    f"Request successful. Response size: {len(str(response_data))}"
                )

                return response_data

            except urllib3.exceptions.HTTPError as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    self.logger.error(
                        f"Network error after {self.max_retries} retries: {str(e)}"
                    )
                    raise TeamTailorAPIError(f"Network error: {str(e)}") from e

                # Exponential backoff for network errors
                delay = min(
                    self.backoff_factor * (2 ** (retry_count - 1)), self.max_backoff
                )
                self.logger.warning(
                    f"Network error (attempt {retry_count}/{self.max_retries}): {str(e)}. "
                    f"Retrying in {delay} seconds..."
                )
                time.sleep(delay)

            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON response: {str(e)}")
                raise TeamTailorAPIError(f"Invalid JSON response: {str(e)}") from e

        # This should never be reached due to the retry logic and exceptions above
        raise TeamTailorAPIError("Request failed after maximum retries")

    def _handle_rate_limit(self, response: Any, retry_count: int) -> Optional[float]:
        """
        Handle rate limiting with exponential backoff.

        Args:
            response: HTTP response with rate limit error
            retry_count: Current retry attempt number

        Returns:
            float: Delay in seconds, or None if max retries exceeded
        """
        if retry_count >= self.max_retries:
            return None

        # Try to get retry-after header
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = self.backoff_factor * (2**retry_count)
        else:
            # Use exponential backoff
            delay = self.backoff_factor * (2**retry_count)

        # Cap the delay
        delay = min(delay, self.max_backoff)

        self.logger.warning(
            f"Rate limit hit (attempt {retry_count + 1}/{self.max_retries}). "
            f"Waiting {delay} seconds before retry..."
        )

        time.sleep(delay)
        return delay

    def _log_rate_limit_info(self, response: Any) -> None:
        """
        Log rate limit information from response headers.

        Args:
            response: HTTP response
        """
        rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
        rate_limit_reset = response.headers.get("X-RateLimit-Reset")

        if rate_limit_remaining or rate_limit_reset:
            self.logger.debug(
                f"Rate limit info - Remaining: {rate_limit_remaining}, "
                f"Reset: {rate_limit_reset}"
            )

    def _extract_error_message(self, response: Any) -> str:
        """
        Extract error message from API response (JSON API format).

        Args:
            response: HTTP response with error

        Returns:
            str: Error message
        """
        try:
            response_text = response.data.decode("utf-8")
            error_data = json.loads(response_text)
            # JSON API format: errors array
            errors = error_data.get("errors", [])
            if errors and isinstance(errors, list):
                return errors[0].get("detail", errors[0].get("title", "Unknown error"))
            return error_data.get("message", "Unknown error")
        except (json.JSONDecodeError, KeyError):
            return response.data.decode("utf-8") if response.data else "Unknown error"

    def test_connection(self) -> bool:
        """
        Test the API connection and authentication.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.logger.info("Testing Team Tailor API connection...")

            # Make a simple request to test authentication
            self._make_request("/v1/candidates", {"page[size]": 1})

            self.logger.info("Team Tailor API connection test successful")
            return True

        except Exception as e:
            self.logger.error(f"Team Tailor API connection test failed: {str(e)}")
            return False
