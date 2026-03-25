"""
Team Tailor API client module.

This module provides a client for interacting with the Team Tailor API
for talent management data extraction.
"""

from .client import (
    TeamTailorAPIClient,
    TeamTailorAPIError,
    TeamTailorAuthenticationError,
    TeamTailorNotFoundError,
    TeamTailorRateLimitError,
)

__all__ = [
    "TeamTailorAPIClient",
    "TeamTailorAPIError",
    "TeamTailorAuthenticationError",
    "TeamTailorNotFoundError",
    "TeamTailorRateLimitError",
]
