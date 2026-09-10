"""Bounded in-process duplicate-delivery tracking."""

from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class DuplicateState(str, Enum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    CAPACITY_EXHAUSTED = "capacity_exhausted"


@dataclass(frozen=True, kw_only=True)
class DeliveryClaim:
    message_id: str


class BoundedDeduplicationStore:
    def __init__(
        self,
        *,
        capacity: int,
        retention_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity < 1:
            raise ValueError("Deduplication capacity must be positive")
        if (
            isinstance(retention_seconds, bool)
            or not isinstance(retention_seconds, (int, float))
            or not math.isfinite(retention_seconds)
            or retention_seconds <= 0
        ):
            raise ValueError("Deduplication retention must be finite positive seconds")
        self._capacity = capacity
        self._retention_seconds = retention_seconds
        self._clock = clock
        self._entries: OrderedDict[str, float] = OrderedDict()
        self._pending: dict[str, DeliveryClaim] = {}
        self._lock = asyncio.Lock()

    async def claim(self, message_id: str) -> DeliveryClaim | DuplicateState:
        async with self._lock:
            now = self._clock()
            self._expire(now)
            if message_id in self._entries:
                return DuplicateState.COMPLETED
            if message_id in self._pending:
                return DuplicateState.IN_FLIGHT
            if len(self._pending) >= self._capacity:
                return DuplicateState.CAPACITY_EXHAUSTED
            while len(self._entries) + len(self._pending) >= self._capacity:
                self._entries.popitem(last=False)
            claim = DeliveryClaim(message_id=message_id)
            self._pending[message_id] = claim
            return claim

    async def complete(self, claim: DeliveryClaim) -> None:
        async with self._lock:
            if self._pending.get(claim.message_id) is claim:
                del self._pending[claim.message_id]
                self._entries[claim.message_id] = self._clock() + self._retention_seconds

    async def release(self, claim: DeliveryClaim) -> None:
        async with self._lock:
            if self._pending.get(claim.message_id) is claim:
                del self._pending[claim.message_id]

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()
            self._pending.clear()

    def _expire(self, now: float) -> None:
        expired = [
            message_id for message_id, expires_at in self._entries.items() if expires_at <= now
        ]
        for message_id in expired:
            del self._entries[message_id]
