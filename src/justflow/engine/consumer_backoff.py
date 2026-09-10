"""Bounded reconnect backoff shared by broker consumers."""

from __future__ import annotations

import random
from collections.abc import Callable

from justflow.config.settings import ConsumerReconnectSettings


class ConsumerReconnectBackoff:
    def __init__(
        self,
        policy: ConsumerReconnectSettings,
        *,
        jitter_source: Callable[[], float] = random.random,
    ) -> None:
        self._policy = policy
        self._jitter_source = jitter_source
        self._next_base_delay = policy.initial_delay_seconds
        self._failure_count = 0

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def next_delay(self) -> float:
        sample = self._jitter_source() if self._policy.jitter_fraction else 0.5
        if not 0 <= sample <= 1:
            raise ValueError("Consumer reconnect jitter source must return a value from 0 to 1")
        jitter_factor = 1 + self._policy.jitter_fraction * (2 * sample - 1)
        delay = min(
            self._next_base_delay * jitter_factor,
            self._policy.max_delay_seconds,
        )
        self._next_base_delay = min(
            self._next_base_delay * self._policy.multiplier,
            self._policy.max_delay_seconds,
        )
        self._failure_count += 1
        return delay

    def reset(self) -> None:
        self._next_base_delay = self._policy.initial_delay_seconds
        self._failure_count = 0
