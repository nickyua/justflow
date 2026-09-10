from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from justflow.configuration import lifecycle
from justflow.configuration.activation import control_identity_digest
from justflow.configuration.errors import ConfigurationLimitError
from justflow.configuration.lifecycle import (
    ConfigurationDeclarationKind,
    ConfigurationDiscardRecord,
    ConfigurationDiscardState,
    ConfigurationRelationshipState,
    build_configuration_relationships,
    configuration_declarations,
    discard_identity,
)
from justflow.configuration.models import RevisionIdentity
from justflow.provenance import provenance_digest
from justflow.scope import LOCAL_RUNTIME_SCOPE

ACTIVE_IDENTITY = "a" * 64
NOW = datetime(2026, 8, 5, 12, tzinfo=UTC)
TEST_RELATIONSHIP_LIMIT = 1


@dataclass(frozen=True, kw_only=True)
class RelationshipCase:
    id: str
    working: dict[str, object]
    active: dict[str, object]
    expected: tuple[tuple[str, ConfigurationRelationshipState], ...]


RELATIONSHIP_CASES = [
    RelationshipCase(
        id="active",
        working={"orders": {"description": "current"}},
        active={"orders": {"description": "current"}},
        expected=(("orders", ConfigurationRelationshipState.ACTIVE),),
    ),
    RelationshipCase(
        id="modified",
        working={"orders": {"description": "changed"}},
        active={"orders": {"description": "current"}},
        expected=(("orders", ConfigurationRelationshipState.MODIFIED),),
    ),
    RelationshipCase(
        id="new-pending-apply",
        working={"orders": {"description": "new"}},
        active={},
        expected=(("orders", ConfigurationRelationshipState.NEW_PENDING_APPLY),),
    ),
    RelationshipCase(
        id="removed-pending-apply",
        working={},
        active={"orders": {"description": "current"}},
        expected=(("orders", ConfigurationRelationshipState.REMOVED_PENDING_APPLY),),
    ),
]


@pytest.mark.parametrize("case", RELATIONSHIP_CASES, ids=lambda case: case.id)
def test_configuration_relationship_state_matrix(case: RelationshipCase) -> None:
    relationships = build_configuration_relationships(
        working_version=7,
        active_identity=ACTIVE_IDENTITY,
        working=configuration_declarations(workflows=case.working, triggers={}),
        active=configuration_declarations(workflows=case.active, triggers={}),
    )

    assert relationships.working_version == 7
    assert relationships.active_identity == ACTIVE_IDENTITY
    assert (
        tuple(
            (relationship.name, relationship.state) for relationship in relationships.relationships
        )
        == case.expected
    )


def test_configuration_relationships_cover_trigger_declarations_without_values() -> None:
    relationships = build_configuration_relationships(
        working_version=1,
        active_identity=None,
        working=configuration_declarations(
            workflows={},
            triggers={
                "event": {"kind": "webhook", "component": "webhook@1"},
                "hourly": {"kind": "schedule", "workflow": "orders"},
            },
        ),
        active={},
    )

    assert tuple(
        (relationship.kind, relationship.name, relationship.state)
        for relationship in relationships.relationships
    ) == (
        (
            ConfigurationDeclarationKind.TRIGGER,
            "event",
            ConfigurationRelationshipState.NEW_PENDING_APPLY,
        ),
        (
            ConfigurationDeclarationKind.TRIGGER,
            "hourly",
            ConfigurationRelationshipState.NEW_PENDING_APPLY,
        ),
    )


@dataclass(frozen=True, kw_only=True)
class DiscardRecordRaises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class DiscardRecordValidationCase:
    id: str
    overrides: dict[str, object]
    outcome: DiscardRecordRaises


DISCARD_RECORD_VALIDATION_CASES = [
    DiscardRecordValidationCase(
        id="applied-without-result",
        overrides={"state": ConfigurationDiscardState.APPLIED},
        outcome=DiscardRecordRaises(exc=ValidationError, match="Applied discard"),
    ),
    DiscardRecordValidationCase(
        id="failed-without-error",
        overrides={"state": ConfigurationDiscardState.FAILED},
        outcome=DiscardRecordRaises(exc=ValidationError, match="Failed discard"),
    ),
    DiscardRecordValidationCase(
        id="pending-with-result",
        overrides={"result_draft_version": 2},
        outcome=DiscardRecordRaises(exc=ValidationError, match="Pending discard"),
    ),
    DiscardRecordValidationCase(
        id="timestamps-out-of-order",
        overrides={"updated_at": NOW - timedelta(seconds=1)},
        outcome=DiscardRecordRaises(exc=ValidationError, match="timestamps are inconsistent"),
    ),
]


@pytest.mark.parametrize(
    "case",
    DISCARD_RECORD_VALIDATION_CASES,
    ids=lambda case: case.id,
)
def test_discard_record_rejects_illegal_outcomes(
    case: DiscardRecordValidationCase,
) -> None:
    key = "discard-orders"
    values: dict[str, object] = {
        "discard_id": discard_identity(LOCAL_RUNTIME_SCOPE.digest, key),
        "scope_digest": LOCAL_RUNTIME_SCOPE.digest,
        "idempotency_key_digest": control_identity_digest("discard-key", key),
        "request_digest": provenance_digest({"request": "discard"}),
        "actor_digest": control_identity_digest("discard-actor", "operator"),
        "correlation_digest": control_identity_digest("discard-correlation", "request"),
        "expected_draft_version": 1,
        "expected_active_identity": RevisionIdentity(ACTIVE_IDENTITY),
        "active_document_digest": provenance_digest({"configuration": "active"}),
        "created_at": NOW,
        "updated_at": NOW,
        **case.overrides,
    }

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        ConfigurationDiscardRecord.model_validate(values)


def test_configuration_relationships_enforce_projection_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifecycle, "MAX_CONFIGURATION_RELATIONSHIPS", TEST_RELATIONSHIP_LIMIT)

    with pytest.raises(ConfigurationLimitError, match="item bound"):
        build_configuration_relationships(
            working_version=1,
            active_identity=None,
            working=configuration_declarations(
                workflows={"orders": {}, "payments": {}},
                triggers={},
            ),
            active={},
        )
