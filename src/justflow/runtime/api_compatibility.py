"""Explicit compatibility contract for public API clients."""

from __future__ import annotations

from dataclasses import dataclass

PUBLIC_API_COMPATIBILITY_VERSION = 1
MINIMUM_SUPPORTED_CLIENT_VERSION = 1
MAXIMUM_SUPPORTED_CLIENT_VERSION = 1


@dataclass(frozen=True, kw_only=True)
class ApiCompatibility:
    current_version: int
    minimum_supported_client_version: int
    maximum_supported_client_version: int

    def public_dict(self) -> dict[str, int]:
        return {
            "current_version": self.current_version,
            "minimum_supported_client_version": self.minimum_supported_client_version,
            "maximum_supported_client_version": self.maximum_supported_client_version,
        }


PUBLIC_API_COMPATIBILITY = ApiCompatibility(
    current_version=PUBLIC_API_COMPATIBILITY_VERSION,
    minimum_supported_client_version=MINIMUM_SUPPORTED_CLIENT_VERSION,
    maximum_supported_client_version=MAXIMUM_SUPPORTED_CLIENT_VERSION,
)
