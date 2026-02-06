# app/ingestion/http.py
"""
HTTP client with retry logic for external API calls.

Uses httpx for async-compatible HTTP and tenacity for retries.
"""

import httpx
from typing import Optional, Dict, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 10.0

# Default retry configuration
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_WAIT_MIN = 1
DEFAULT_WAIT_MAX = 10


class HttpClientError(Exception):
    """Base exception for HTTP client errors."""
    pass


class HttpTimeoutError(HttpClientError):
    """Raised when request times out."""
    pass


class HttpStatusError(HttpClientError):
    """Raised when response has non-2xx status."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


@retry(
    stop=stop_after_attempt(DEFAULT_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=1,
        min=DEFAULT_WAIT_MIN,
        max=DEFAULT_WAIT_MAX
    ),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,  # Re-raise the last exception after retries exhausted
)
def _fetch_with_retry_inner(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    """
    Inner retry function - lets exceptions bubble for tenacity to catch and retry.

    DO NOT catch exceptions here - that would prevent tenacity from retrying.
    """
    with httpx.Client(timeout=timeout) as client:
        response = client.request(
            method=method,
            url=url,
            params=params,
            headers=headers,
        )
        response.raise_for_status()
        return response


def fetch_with_retry(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> httpx.Response:
    """
    Fetch URL with automatic retry on transient failures.

    Args:
        url: URL to fetch
        method: HTTP method (GET, POST, etc.)
        params: Query parameters
        headers: HTTP headers
        timeout: Request timeout in seconds

    Returns:
        httpx.Response object

    Raises:
        HttpTimeoutError: After all retries exhausted due to timeout
        HttpStatusError: After all retries exhausted due to status error
    """
    try:
        # Call the inner function that has retry logic
        # Exceptions only reach here AFTER all retries are exhausted
        return _fetch_with_retry_inner(url, method, params, headers, timeout)
    except httpx.TimeoutException as e:
        # Convert to custom exception ONLY after retries exhausted
        raise HttpTimeoutError(f"Timeout fetching {url} after {DEFAULT_MAX_ATTEMPTS} attempts: {e}")
    except httpx.HTTPStatusError as e:
        # Convert to custom exception ONLY after retries exhausted
        raise HttpStatusError(e.response.status_code, f"HTTP error after {DEFAULT_MAX_ATTEMPTS} attempts: {e}")


class HttpClient:
    """
    HTTP client for external API calls.

    Provides a consistent interface for fetching data from
    external sources with retry logic and error handling.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize HTTP client.

        Args:
            base_url: Base URL for all requests
            timeout: Default timeout in seconds
            headers: Default headers for all requests
        """
        self.base_url = base_url or ""
        self.timeout = timeout
        self.headers = headers or {}

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        GET request with retry.

        Args:
            path: URL path (appended to base_url)
            params: Query parameters
            headers: Additional headers (merged with defaults)

        Returns:
            httpx.Response object
        """
        url = f"{self.base_url}{path}" if self.base_url else path
        merged_headers = {**self.headers, **(headers or {})}
        return fetch_with_retry(
            url=url,
            method="GET",
            params=params,
            headers=merged_headers,
            timeout=self.timeout,
        )

    def get_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """
        GET request returning JSON.

        Args:
            path: URL path
            params: Query parameters
            headers: Additional headers

        Returns:
            Parsed JSON response
        """
        response = self.get(path, params, headers)
        return response.json()

    def get_text(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        GET request returning text.

        Args:
            path: URL path
            params: Query parameters
            headers: Additional headers

        Returns:
            Response text
        """
        response = self.get(path, params, headers)
        return response.text
