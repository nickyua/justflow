"""Bounded duplicate ownership remains valid through expiry and stale completions."""

import pytest

from justflow.engine.deduplication import BoundedDeduplicationStore, DeliveryClaim, DuplicateState

RETENTION_SECONDS = 10
CAPACITY = 1


async def test_pending_delivery_cannot_expire_or_be_evicted_under_pressure() -> None:
    now = 0.0
    store = BoundedDeduplicationStore(
        capacity=CAPACITY, retention_seconds=RETENTION_SECONDS, clock=lambda: now
    )
    owner = await store.claim("first")
    assert isinstance(owner, DeliveryClaim)
    now += RETENTION_SECONDS
    assert await store.claim("first") is DuplicateState.IN_FLIGHT
    assert await store.claim("second") is DuplicateState.CAPACITY_EXHAUSTED
    await store.complete(owner)
    assert await store.claim("first") is DuplicateState.COMPLETED
    now += RETENTION_SECONDS
    assert isinstance(await store.claim("first"), DeliveryClaim)


@pytest.mark.parametrize("stale_action", ["complete", "release"])
async def test_old_owner_cannot_change_a_reclaimed_delivery(stale_action: str) -> None:
    store = BoundedDeduplicationStore(capacity=CAPACITY, retention_seconds=RETENTION_SECONDS)
    first = await store.claim("message")
    assert isinstance(first, DeliveryClaim)
    await store.release(first)
    second = await store.claim("message")
    assert isinstance(second, DeliveryClaim)
    assert first is not second
    if stale_action == "complete":
        await store.complete(first)
    else:
        await store.release(first)
    assert await store.claim("message") is DuplicateState.IN_FLIGHT
    await store.complete(second)
    assert await store.claim("message") is DuplicateState.COMPLETED


async def test_capacity_reclaims_completed_deliveries_only() -> None:
    store = BoundedDeduplicationStore(capacity=CAPACITY, retention_seconds=RETENTION_SECONDS)
    first = await store.claim("first")
    assert isinstance(first, DeliveryClaim)
    await store.complete(first)
    second = await store.claim("second")
    assert isinstance(second, DeliveryClaim)
    assert await store.claim("first") is DuplicateState.CAPACITY_EXHAUSTED
