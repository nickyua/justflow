"""In-memory resources for local development and tests."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from types import MappingProxyType
from typing import Any


class MemoryStore:
    """Keeps written records in memory; implements the archival ``write`` protocol."""

    def __init__(
        self,
        retention_policies: dict[str, float],
        clock: Callable[[], float] | None = None,
    ) -> None:
        if not retention_policies:
            raise ValueError("MemoryStore requires at least one finite retention policy")
        if any(not name for name in retention_policies):
            raise ValueError("MemoryStore retention policy names must not be empty")
        if any(
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds <= 0
            for seconds in retention_policies.values()
        ):
            raise ValueError("MemoryStore retention values must be finite positive seconds")
        self._retention_policies = dict(retention_policies)
        self._clock = clock or time.monotonic
        self._records: dict[str, tuple[str, float]] = {}

    @property
    def records(self) -> dict[str, str]:
        self._expire()
        return {path: data for path, (data, _) in self._records.items()}

    async def write(self, path: str, data: str, *, retention_policy: str) -> None:
        try:
            retention_seconds = self._retention_policies[retention_policy]
        except KeyError as exc:
            raise ValueError(f"Unknown MemoryStore retention policy '{retention_policy}'") from exc
        self._records[path] = (data, self._clock() + retention_seconds)

    async def delete(self, path: str) -> None:
        self._records.pop(path, None)

    async def clear(self) -> None:
        self._records.clear()

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        await self.clear()

    def _expire(self) -> None:
        now = self._clock()
        expired = [path for path, (_, expires_at) in self._records.items() if expires_at <= now]
        for path in expired:
            del self._records[path]


class MemoryCache:
    """In-memory cache implementing the step-cache protocol (get/set with TTL)."""

    def __init__(self, clock: Callable[[], float] | None = None):
        self._clock = clock or time.time
        self._entries: dict[str, tuple[str, float | None]] = {}

    async def get(self, key: str) -> str | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._entries[key]
            return None
        return value

    async def set(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        expires_at = self._clock() + ttl_sec if ttl_sec is not None else None
        self._entries[key] = (value, expires_at)

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self._entries.clear()


class StaticConfig:
    """Static key-value application settings defined in YAML."""

    def __init__(self, **values: Any) -> None:
        self._values = MappingProxyType(dict(values))

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None
