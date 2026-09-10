"""Public boundary for an explicitly composed administration panel."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

ADMIN_ASSET_CACHE_CONTROL = b"no-store"
ADMIN_ASSET_SECURITY_HEADERS = (
    (b"cache-control", ADMIN_ASSET_CACHE_CONTROL),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
)


@dataclass(frozen=True, kw_only=True)
class AdminPanelAsset:
    body: bytes
    content_type: bytes
    headers: tuple[tuple[bytes, bytes], ...] = field(default=ADMIN_ASSET_SECURITY_HEADERS)


class AdminPanel(Protocol):
    """Small serving facade implemented by the optional admin distribution."""

    def has_route(self, segments: tuple[str, ...]) -> bool: ...

    def resolve(self, segments: tuple[str, ...]) -> AdminPanelAsset | None: ...
