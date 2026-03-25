"""
ClickUp API Client Module

This module provides a client for interacting with the ClickUp API v2,
specifically for retrieving technology requests data (tasks, custom fields, etc.)
with proper authentication, pagination, rate limiting, and error handling.

Features implemented:
- Authentication using API tokens (Bearer token)
- Pagination support for large datasets (page-based)
- Rate limiting with exponential backoff (100 req/min for Business plan)
- Comprehensive error handling for authentication and API failures
- urllib3-based HTTP client for compatibility with existing environment
- Custom fields support

API Documentation: https://developer.clickup.com/docs
"""

import json
import time
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import quote

import urllib3
from urllib3.util.retry import Retry

from libs.common.utils.logger import get_logger


class ClickUpAPIError(Exception):
    """Base exception for ClickUp API errors"""


class ClickUpAuthenticationError(ClickUpAPIError):
    """Exception raised for authentication failures"""


class ClickUpRateLimitError(ClickUpAPIError):
    """Exception raised for rate limit errors"""


class ClickUpNotFoundError(ClickUpAPIError):
    """Exception raised for 404 Not Found errors (endpoint or resource not available)"""


class ClickUpAPIClient:
    """
    ClickUp API v2 client with authentication, pagination, and rate limiting support.

    This client handles:
    - API authentication using Bearer tokens
    - Page-based pagination for large datasets
    - Rate limiting with exponential backoff (100 req/min for Business plan)
    - Comprehensive error handling
    - Custom fields support

    API Documentation: https://developer.clickup.com/docs
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.clickup.com",
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        max_backoff: float = 60.0,
        rate_limit_per_minute: int = 100,
    ):
        """
        Initialize the ClickUp API client.

        Args:
            api_token: ClickUp API token (Bearer token)
            base_url: Base URL for ClickUp API (default: https://api.clickup.com)
            max_retries: Maximum number of retry attempts for failed requests
            backoff_factor: Base delay for exponential backoff (seconds)
            max_backoff: Maximum delay between retries (seconds)
            rate_limit_per_minute: Rate limit per minute (default: 100 for Business plan)
        """
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        self.rate_limit_per_minute = rate_limit_per_minute
        # Calculate minimum interval between requests (in seconds)
        self.min_request_interval = 60.0 / rate_limit_per_minute

        # Clean and store API token
        self.api_token = (api_token or "").strip()
        self.logger = get_logger(__name__)

        # Track last request time for rate limiting
        self.last_request_time = 0.0

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
            f"ClickUp API client initialized successfully "
            f"(base_url: {self.base_url}, token: {token_preview}, "
            f"length: {len(self.api_token)}, rate_limit: {rate_limit_per_minute}/min)"
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

    def _enforce_rate_limit(self) -> None:
        """
        Enforce rate limiting by sleeping if necessary.

        ClickUp Business plan allows 100 requests per minute.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_tasks(
        self,
        list_id: str,
        page: int = 0,
        include_closed: bool = True,
        custom_fields: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        reverse: bool = False,
    ) -> Iterator[Dict]:
        """
        Retrieve tasks from a ClickUp list with pagination.

        Args:
            list_id: ClickUp list ID
            page: Page number (0-indexed, default: 0)
            include_closed: Whether to include closed tasks (default: True)
            custom_fields: List of custom field IDs to include (optional)
            order_by: Field to order by (e.g., "created", "updated", "due_date")
            reverse: Reverse order (default: False)

        Yields:
            Dict: Individual task records

        Raises:
            ClickUpAPIError: For API-related errors
            ClickUpAuthenticationError: For authentication failures
        """
        return self._get_tasks_paginated(
            list_id=list_id,
            include_closed=include_closed,
            custom_fields=custom_fields,
            order_by=order_by,
            reverse=reverse,
        )

    def _get_tasks_paginated(
        self,
        list_id: str,
        include_closed: bool = True,
        custom_fields: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        reverse: bool = False,
    ) -> Iterator[Dict]:
        """
        Retrieve all tasks from a list with automatic pagination.

        Args:
            list_id: ClickUp list ID
            include_closed: Whether to include closed tasks
            custom_fields: List of custom field IDs to include
            order_by: Field to order by
            reverse: Reverse order

        Yields:
            Dict: Individual task records
        """
        page = 0
        total_tasks = 0

        self.logger.info(
            f"Starting tasks retrieval for list {list_id} - "
            f"include_closed: {include_closed}"
        )

        while True:
            try:
                # Build query parameters
                params = {
                    "page": page,
                    "include_closed": str(include_closed).lower(),
                }

                if order_by:
                    params["order_by"] = order_by
                if reverse:
                    params["reverse"] = "true"
                if custom_fields:
                    # ClickUp API expects custom_fields[] as array parameter
                    # Store as list to preserve all values
                    params["custom_fields[]"] = custom_fields

                # Make API request
                response_data = self._make_request(
                    f"/api/v2/list/{list_id}/task", params=params
                )

                # Extract tasks from response
                tasks = response_data.get("tasks", [])
                if not tasks:
                    self.logger.info(f"No more tasks to process (page {page})")
                    break

                # Yield individual tasks
                for task in tasks:
                    total_tasks += 1
                    yield task

                self.logger.debug(
                    f"Processed page {page} with {len(tasks)} tasks. "
                    f"Total so far: {total_tasks}"
                )

                # Check if there are more pages
                # ClickUp API doesn't always provide explicit pagination info
                # If we got fewer tasks than expected, assume we're done
                # Typical page size is 100, but can vary
                if len(tasks) < 100:
                    self.logger.info(
                        "Reached end of pagination (last page had < 100 tasks)"
                    )
                    break

                page += 1

            except ClickUpRateLimitError:
                # Rate limit handling is done in _make_request
                continue
            except Exception as e:
                self.logger.error(
                    f"Error retrieving tasks from list {list_id}: {str(e)}"
                )
                raise ClickUpAPIError(
                    f"Failed to retrieve tasks from list {list_id}: {str(e)}"
                ) from e

        self.logger.info(
            f"Tasks retrieval completed for list {list_id}. Total: {total_tasks}"
        )

    def get_task(self, task_id: str, custom_fields: Optional[List[str]] = None) -> Dict:
        """
        Retrieve a single task by ID with full details.

        Args:
            task_id: ClickUp task ID
            custom_fields: List of custom field IDs to include (optional)

        Returns:
            Dict: Task record with full details

        Raises:
            ClickUpAPIError: For API-related errors
            ClickUpAuthenticationError: For authentication failures
            ClickUpNotFoundError: If task not found
        """
        params = {}
        if custom_fields:
            # ClickUp API expects custom_fields[] as array parameter
            params["custom_fields[]"] = custom_fields

        response_data = self._make_request(f"/api/v2/task/{task_id}", params=params)
        return response_data

    def get_list_custom_fields(self, list_id: str) -> List[Dict]:
        """
        Retrieve custom fields for a list.

        Args:
            list_id: ClickUp list ID

        Returns:
            List[Dict]: List of custom field definitions

        Raises:
            ClickUpAPIError: For API-related errors
        """
        response_data = self._make_request(f"/api/v2/list/{list_id}/field")
        return response_data.get("fields", [])

    def get_lists(
        self, space_id: Optional[str] = None, folder_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Retrieve lists from ClickUp.

        Args:
            space_id: Optional space ID to filter lists
            folder_id: Optional folder ID to filter lists

        Returns:
            List[Dict]: List of list definitions

        Raises:
            ClickUpAPIError: For API-related errors
        """
        if folder_id:
            response_data = self._make_request(f"/api/v2/folder/{folder_id}/list")
        elif space_id:
            response_data = self._make_request(f"/api/v2/space/{space_id}/list")
        else:
            raise ValueError("Either space_id or folder_id must be provided")

        return response_data.get("lists", [])

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to ClickUp API with error handling and rate limiting.

        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters

        Returns:
            Dict: API response data

        Raises:
            ClickUpAuthenticationError: For authentication failures
            ClickUpRateLimitError: For rate limit errors (after retries)
            ClickUpAPIError: For other API errors
        """
        # Enforce rate limiting
        self._enforce_rate_limit()

        retry_count = 0

        # Clean and validate API token
        api_token = (self.api_token or "").strip()
        if not api_token:
            raise ClickUpAuthenticationError("API token is empty or not set")

        # Build headers (ClickUp uses Bearer token authentication)
        headers = {
            "Authorization": f"{api_token}",
            "Content-Type": "application/json",
        }

        # Log token info (masked for security)
        token_preview = (
            f"{api_token[:4]}...{api_token[-4:]}" if len(api_token) > 8 else "****"
        )
        self.logger.debug(
            f"Making request to {endpoint} with token: {token_preview} "
            f"(length: {len(api_token)})"
        )

        url = f"{self.base_url}{endpoint}"

        while retry_count <= self.max_retries:
            try:
                # Build URL with query parameters
                if params:
                    # Handle array parameters (custom_fields[])
                    query_parts = []
                    for key, value in params.items():
                        if key.endswith("[]"):
                            # Array parameter - add multiple values
                            if isinstance(value, list):
                                for v in value:
                                    query_parts.append(
                                        f"{key}={quote(str(v), safe='')}"
                                    )
                            else:
                                query_parts.append(
                                    f"{key}={quote(str(value), safe='')}"
                                )
                        else:
                            query_parts.append(f"{key}={quote(str(value), safe='')}")
                    query_string = "&".join(query_parts)
                    full_url = f"{url}?{query_string}"
                else:
                    full_url = url

                self.logger.debug(f"Making request to {full_url}")

                response = self.session.request("GET", full_url, headers=headers)

                # Handle rate limiting
                if response.status == 429:
                    retry_after = self._handle_rate_limit(response, retry_count)
                    if retry_after is not None:
                        retry_count += 1
                        continue
                    else:
                        raise ClickUpRateLimitError(
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
                    raise ClickUpAuthenticationError(
                        f"Invalid API token or authentication failed: {error_msg}"
                    )

                # Handle 404 Not Found errors
                if response.status == 404:
                    error_msg = self._extract_error_message(response)
                    self.logger.error(
                        f"Resource not found (404) - {error_msg}. "
                        f"Endpoint may not exist or may not be available."
                    )
                    raise ClickUpNotFoundError(f"Resource not found (404): {error_msg}")

                # Handle other client errors
                if response.status >= 400:
                    error_msg = self._extract_error_message(response)
                    self.logger.error(
                        f"API request failed: {response.status} - {error_msg}"
                    )
                    raise ClickUpAPIError(
                        f"API request failed: {response.status} - {error_msg}"
                    )

                # Log rate limit info for monitoring
                self._log_rate_limit_info(response)

                # Parse and return response
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
                    raise ClickUpAPIError(f"Network error: {str(e)}") from e

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
                raise ClickUpAPIError(f"Invalid JSON response: {str(e)}") from e

        # This should never be reached due to the retry logic and exceptions above
        raise ClickUpAPIError("Request failed after maximum retries")

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
            # Use exponential backoff with longer delay for rate limits
            # ClickUp rate limit is per minute, so wait at least 1 minute
            delay = max(60.0, self.backoff_factor * (2**retry_count))

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
        Extract error message from API response.

        Args:
            response: HTTP response with error

        Returns:
            str: Error message
        """
        try:
            response_text = response.data.decode("utf-8")
            error_data = json.loads(response_text)
            # ClickUp API error format
            if "err" in error_data:
                return error_data["err"]
            if "error" in error_data:
                return error_data["error"]
            if "message" in error_data:
                return error_data["message"]
            return str(error_data)
        except (json.JSONDecodeError, KeyError):
            return response.data.decode("utf-8") if response.data else "Unknown error"

    def test_connection(self) -> bool:
        """
        Test the API connection and authentication.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            self.logger.info("Testing ClickUp API connection...")

            # Make a simple request to test authentication
            # Try to get user info (requires valid token)
            self._make_request("/api/v2/user")

            self.logger.info("ClickUp API connection test successful")
            return True

        except Exception as e:
            self.logger.error(f"ClickUp API connection test failed: {str(e)}")
            return False
