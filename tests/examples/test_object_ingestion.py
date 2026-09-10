"""Behavior checks for the object-ingestion example package."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from object_ingestion.actions import LocalObjectService
from object_ingestion.application import MAPPING_NAME, create_event_registry

FIXTURE_PATH = Path("examples/object_ingestion/fixtures/s3-object-created.json")


async def test_s3_mapper_keeps_identity_stable_across_eventbridge_redelivery() -> None:
    event = json.loads(FIXTURE_PATH.read_text())
    redelivery = deepcopy(event)
    redelivery["id"] = "fixture-delivery-2"
    mapper, _ = create_event_registry().resolve(MAPPING_NAME)

    identity = await mapper.event_identity(event)
    redelivery_identity = await mapper.event_identity(redelivery)
    request = await mapper.to_start_request(event, identity)

    assert identity == redelivery_identity
    assert request.input["key"] == "incoming/order-100.json"
    assert request.business_request_id


async def test_local_object_service_returns_non_looping_archive_key() -> None:
    service = LocalObjectService()
    reference = {
        "bucket": "example-ingestion",
        "key": "incoming/order-100.json",
        "sequencer": "00655AED6DCD90281E",
        "size": 128,
    }

    inspected = await service.inspect_object(reference)
    result = await service.archive_object(inspected)

    assert result == {
        "source_key": "incoming/order-100.json",
        "archive_key": "archive/order-100.json",
        "state": "archived",
    }
