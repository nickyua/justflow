"""Tests for broker consumer reconnect backoff."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from justflow.config.settings import ConsumerReconnectSettings
from justflow.engine.consumer_backoff import ConsumerReconnectBackoff


@dataclass(frozen=True, kw_only=True)
class BackoffCase:
    id: str
    policy: ConsumerReconnectSettings
    samples: tuple[float, ...]
    expected_delays: tuple[float, ...]


BACKOFF_CASES = [
    BackoffCase(
        id="exponential-cap",
        policy=ConsumerReconnectSettings(
            initial_delay_seconds=1,
            max_delay_seconds=4,
            multiplier=2,
            jitter_fraction=0,
        ),
        samples=(),
        expected_delays=(1, 2, 4, 4),
    ),
    BackoffCase(
        id="lower-jitter",
        policy=ConsumerReconnectSettings(
            initial_delay_seconds=1,
            max_delay_seconds=4,
            multiplier=2,
            jitter_fraction=0.25,
        ),
        samples=(0, 0),
        expected_delays=(0.75, 1.5),
    ),
    BackoffCase(
        id="upper-jitter-remains-bounded",
        policy=ConsumerReconnectSettings(
            initial_delay_seconds=4,
            max_delay_seconds=5,
            multiplier=2,
            jitter_fraction=0.5,
        ),
        samples=(1, 1),
        expected_delays=(5, 5),
    ),
]


@pytest.mark.parametrize("case", BACKOFF_CASES, ids=lambda case: case.id)
def test_reconnect_backoff(case: BackoffCase) -> None:
    samples = iter(case.samples)
    backoff = ConsumerReconnectBackoff(case.policy, jitter_source=lambda: next(samples))

    delays = tuple(backoff.next_delay() for _ in case.expected_delays)

    assert delays == case.expected_delays
    assert backoff.failure_count == len(case.expected_delays)


def test_reconnect_backoff_resets_after_successful_poll() -> None:
    policy = ConsumerReconnectSettings(
        initial_delay_seconds=1,
        max_delay_seconds=4,
        multiplier=2,
        jitter_fraction=0,
    )
    backoff = ConsumerReconnectBackoff(policy)

    assert (backoff.next_delay(), backoff.next_delay()) == (1, 2)

    backoff.reset()

    assert backoff.next_delay() == 1
    assert backoff.failure_count == 1


def test_reconnect_backoff_rejects_invalid_jitter_sample() -> None:
    backoff = ConsumerReconnectBackoff(
        ConsumerReconnectSettings(),
        jitter_source=lambda: 1.1,
    )

    with pytest.raises(ValueError, match="jitter source"):
        backoff.next_delay()
