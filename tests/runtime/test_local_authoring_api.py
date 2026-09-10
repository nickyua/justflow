from __future__ import annotations

import json
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

import pytest

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.configuration.local_authoring import (
    MAX_LOCAL_AUTHORING_YAML_BYTES,
    LocalAuthoringConfigurationSource,
)
from justflow.definitions.catalog import CatalogStore
from justflow.resources.builtins import builtin_resource_registry
from justflow.runtime.auth import AuthorizationAction
from justflow.runtime.configuration_api import (
    ConfigurationApiRequestError,
    ConfigurationAuthoringMode,
    ConfigurationRouteKind,
)
from justflow.runtime.local_authoring_api import LocalAuthoringConfigurationApi
from justflow.scope import RuntimeScope, ScopeBindingKind, TrustedScopeBinding
from justflow.transports.builtins import builtin_transport_registry

SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="local",
)


@dataclass(frozen=True, kw_only=True)
class RouteCase:
    id: str
    method: str
    suffix: tuple[str, ...]
    kind: ConfigurationRouteKind
    action: AuthorizationAction


ROUTE_CASES = [
    RouteCase(
        id="create",
        method="POST",
        suffix=("draft",),
        kind=ConfigurationRouteKind.CREATE_DRAFT,
        action=AuthorizationAction.CONFIGURATION_EDIT,
    ),
    RouteCase(
        id="read",
        method="GET",
        suffix=("draft",),
        kind=ConfigurationRouteKind.READ_DRAFT,
        action=AuthorizationAction.CONFIGURATION_VIEW,
    ),
    RouteCase(
        id="update",
        method="PUT",
        suffix=("draft",),
        kind=ConfigurationRouteKind.UPDATE_DRAFT,
        action=AuthorizationAction.CONFIGURATION_EDIT,
    ),
    RouteCase(
        id="import",
        method="POST",
        suffix=("draft", "import"),
        kind=ConfigurationRouteKind.IMPORT_DRAFT,
        action=AuthorizationAction.CONFIGURATION_EDIT,
    ),
    RouteCase(
        id="export",
        method="GET",
        suffix=("draft", "export"),
        kind=ConfigurationRouteKind.EXPORT_DRAFT,
        action=AuthorizationAction.CONFIGURATION_VIEW,
    ),
    RouteCase(
        id="validate",
        method="POST",
        suffix=("draft", "validate"),
        kind=ConfigurationRouteKind.VALIDATE_DRAFT,
        action=AuthorizationAction.CONFIGURATION_VALIDATE,
    ),
    RouteCase(
        id="relationships",
        method="GET",
        suffix=("relationships",),
        kind=ConfigurationRouteKind.READ_RELATIONSHIPS,
        action=AuthorizationAction.CONFIGURATION_VIEW,
    ),
    RouteCase(
        id="apply",
        method="POST",
        suffix=("apply",),
        kind=ConfigurationRouteKind.APPLY,
        action=AuthorizationAction.CONFIGURATION_APPLY,
    ),
    RouteCase(
        id="discard",
        method="POST",
        suffix=("draft", "discard"),
        kind=ConfigurationRouteKind.DISCARD,
        action=AuthorizationAction.CONFIGURATION_DISCARD,
    ),
    RouteCase(
        id="read-discard",
        method="GET",
        suffix=("discards", "a" * 64),
        kind=ConfigurationRouteKind.READ_DISCARD,
        action=AuthorizationAction.CONFIGURATION_VIEW,
    ),
]


def configuration_api(
    tmp_path: Path,
    *,
    definition_publication: bool = True,
) -> LocalAuthoringConfigurationApi:
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True)
    (config_dir / "resources.yaml").write_text("resources: {}\n")
    (config_dir / "services.yaml").write_text("services: {}\n")
    (config_dir / "triggers.yaml").write_text("triggers: {}\n")
    workflows = config_dir / "workflows"
    workflows.mkdir()
    (workflows / "example.yaml").write_text(
        "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    )
    source = LocalAuthoringConfigurationSource(
        config_dir,
        scope=SCOPE,
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=RuntimeLimits(),
        definition_catalog_store=(
            CatalogStore(tmp_path / "definitions") if definition_publication else None
        ),
    )
    return LocalAuthoringConfigurationApi(source)


def scope_binding() -> TrustedScopeBinding:
    return TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE,
        binding_id="principal",
    )


async def body(value: bytes) -> bytes:
    return value


@pytest.mark.parametrize("case", ROUTE_CASES, ids=lambda case: case.id)
def test_local_authoring_exposes_only_draft_routes(tmp_path: Path, case: RouteCase) -> None:
    api = configuration_api(tmp_path)

    route = api.resolve_route(case.method, ("v1", "configuration", *case.suffix))

    assert api.authoring_mode is ConfigurationAuthoringMode.LOCAL_SOURCE
    assert api.supported_actions == frozenset(
        {
            AuthorizationAction.CONFIGURATION_VIEW,
            AuthorizationAction.CONFIGURATION_EDIT,
            AuthorizationAction.CONFIGURATION_VALIDATE,
            AuthorizationAction.CONFIGURATION_APPLY,
            AuthorizationAction.CONFIGURATION_DISCARD,
        }
    )
    assert route.kind is case.kind
    assert route.action is case.action


def test_local_authoring_rejects_release_routes(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)

    with pytest.raises(ConfigurationApiRequestError) as raised:
        api.resolve_route("POST", ("v1", "configuration", "publications"))

    assert raised.value.status is HTTPStatus.NOT_FOUND

    with pytest.raises(ConfigurationApiRequestError) as oversized:
        api.resolve_route("GET", ("v1", "configuration", "a", "b", "c", "d", "e"))
    assert oversized.value.status is HTTPStatus.REQUEST_URI_TOO_LONG


async def test_local_authoring_dispatches_bounded_update_export_and_validation(
    tmp_path: Path,
) -> None:
    api = configuration_api(tmp_path)
    read_route = api.resolve_route("GET", ("v1", "configuration", "draft"))
    initial = await api.dispatch(
        read_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert initial.payload is not None
    version = initial.payload["version"]
    replacement = WorkflowConfig(
        workflow="replacement",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    update_payload = json.dumps(
        {
            "expected_version": version,
            "configuration": {
                "workflows": {"replacement": replacement.model_dump(mode="json")},
                "triggers": {},
            },
        },
        separators=(",", ":"),
    ).encode()
    update_route = api.resolve_route("PUT", ("v1", "configuration", "draft"))

    updated = await api.dispatch(
        update_route,
        query={},
        headers=(),
        read_body=lambda: body(update_payload),
        scope_binding=scope_binding(),
    )
    export_route = api.resolve_route("GET", ("v1", "configuration", "draft", "export"))
    exported = await api.dispatch(
        export_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    validate_route = api.resolve_route("POST", ("v1", "configuration", "draft", "validate"))
    validation = await api.dispatch(
        validate_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )

    assert updated.status is HTTPStatus.OK
    assert updated.payload is not None
    assert updated.payload["restart_required"] is True
    assert exported.raw_body is not None
    assert b"replacement" in exported.raw_body
    assert validation.payload == {
        "valid": True,
        "issues": [
            {
                "category": "semantic",
                "location": ["workflows", "replacement"],
                "message": ("Workflow is internal-only and is not referenced by another workflow"),
                "severity": "warning",
            }
        ],
    }


async def test_local_trigger_fragment_round_trip(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    binding = scope_binding()
    route = api.resolve_route(
        "GET",
        ("v1", "configuration", "draft", "triggers"),
    )
    initial = await api.dispatch(
        route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )
    assert isinstance(initial.payload, dict)
    assert initial.payload["document"] == "triggers: {}\n"

    updated = await api.dispatch(
        api.resolve_route(
            "PUT",
            ("v1", "configuration", "draft", "triggers"),
        ),
        query={"expected_version": [str(initial.payload["version"])]},
        headers=(),
        read_body=lambda: body(
            b"triggers:\n  hourly:\n    kind: schedule\n    workflow: example\n    input: {}\n"
            b"    spec:\n      kind: interval\n      every_seconds: 3600\n"
        ),
        scope_binding=binding,
    )
    reread = await api.dispatch(
        route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )

    assert updated.status is HTTPStatus.OK
    assert isinstance(reread.payload, dict)
    assert "hourly" in reread.payload["document"]


async def test_local_apply_and_discard_report_truthful_stages(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    binding = scope_binding()
    initial_response = await api.dispatch(
        api.resolve_route("GET", ("v1", "configuration", "draft")),
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )
    relationships_response = await api.dispatch(
        api.resolve_route("GET", ("v1", "configuration", "relationships")),
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )
    assert isinstance(initial_response.payload, dict)
    assert isinstance(relationships_response.payload, dict)
    initial_version = initial_response.payload["version"]
    active_identity = relationships_response.payload["active_identity"]
    replacement = WorkflowConfig(
        workflow="replacement",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    update_payload = json.dumps(
        {
            "expected_version": initial_version,
            "configuration": {
                "workflows": {"replacement": replacement.model_dump(mode="json")},
                "triggers": {},
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")
    saved = await api.dispatch(
        api.resolve_route("PUT", ("v1", "configuration", "draft")),
        query={},
        headers=(),
        read_body=lambda: body(update_payload),
        scope_binding=binding,
    )
    assert isinstance(saved.payload, dict)
    saved_version = saved.payload["version"]
    operation_headers = (
        (b"x-idempotency-key", b"operation-1"),
        (b"x-correlation-id", b"request-1"),
    )

    applied = await api.dispatch(
        api.resolve_route("POST", ("v1", "configuration", "apply")),
        query={},
        headers=operation_headers,
        read_body=lambda: body(
            json.dumps({"expected_draft_version": saved_version}).encode("utf-8")
        ),
        scope_binding=binding,
    )

    assert isinstance(applied.payload, dict)
    assert applied.payload["restart_required"] is True
    assert applied.payload["running_process_changed"] is False
    assert applied.payload["stages"][-1] == {
        "kind": "process_restart",
        "state": "restart_required",
    }

    discard_body = json.dumps(
        {
            "expected_draft_version": saved_version,
            "expected_active_identity": active_identity,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    discard_route = api.resolve_route(
        "POST",
        ("v1", "configuration", "draft", "discard"),
    )
    discarded = await api.dispatch(
        discard_route,
        query={},
        headers=operation_headers,
        read_body=lambda: body(discard_body),
        scope_binding=binding,
    )
    repeated = await api.dispatch(
        discard_route,
        query={},
        headers=operation_headers,
        read_body=lambda: body(discard_body),
        scope_binding=binding,
    )

    assert discarded.payload == repeated.payload
    assert isinstance(discarded.payload, dict)
    assert discarded.payload["restart_required"] is False
    assert discarded.payload["running_process_changed"] is False
    audit = await api.dispatch(
        api.resolve_route(
            "GET",
            ("v1", "configuration", "discards", discarded.payload["discard_id"]),
        ),
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )
    assert isinstance(audit.payload, dict)
    assert audit.payload["state"] == "applied"
    assert audit.payload["actor_digest"] != binding.binding_id


async def test_local_apply_fails_closed_when_definition_publication_is_unavailable(
    tmp_path: Path,
) -> None:
    api = configuration_api(tmp_path, definition_publication=False)
    binding = scope_binding()
    draft = await api.dispatch(
        api.resolve_route("GET", ("v1", "configuration", "draft")),
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=binding,
    )
    assert isinstance(draft.payload, dict)

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            api.resolve_route("POST", ("v1", "configuration", "apply")),
            query={},
            headers=((b"x-idempotency-key", b"apply-unavailable"),),
            read_body=lambda: body(
                json.dumps({"expected_draft_version": draft.payload["version"]}).encode("utf-8")
            ),
            scope_binding=binding,
        )

    assert raised.value.status is HTTPStatus.SERVICE_UNAVAILABLE
    assert raised.value.code == "configuration_unavailable"
    assert str(raised.value) == "Local authoring storage is unavailable"


async def test_local_authoring_dispatches_create_and_compare_and_swap_import(
    tmp_path: Path,
) -> None:
    api = configuration_api(tmp_path)
    configuration = {
        "workflows": {
            "example": {
                "workflow": "example",
                "steps": {},
                "flow": [{"name": "done", "terminal": True}],
            }
        },
        "triggers": {},
    }
    create_route = api.resolve_route("POST", ("v1", "configuration", "draft"))
    created = await api.dispatch(
        create_route,
        query={},
        headers=(),
        read_body=lambda: body(
            json.dumps({"configuration": configuration}, separators=(",", ":")).encode()
        ),
        scope_binding=scope_binding(),
    )
    assert created.payload is not None
    version = created.payload["version"]
    import_route = api.resolve_route("POST", ("v1", "configuration", "draft", "import"))

    imported = await api.dispatch(
        import_route,
        query={"expected_version": [str(version)]},
        headers=(),
        read_body=lambda: body(
            b"workflows:\n  replacement:\n    workflow: replacement\n    steps: {}\n"
            b"    flow:\n      - name: done\n        terminal: true\ntriggers: {}\n"
        ),
        scope_binding=scope_binding(),
    )

    assert created.status is HTTPStatus.CREATED
    assert imported.status is HTTPStatus.OK
    assert imported.payload is not None
    assert tuple(imported.payload["bundle"]["workflows"]) == ("replacement",)


async def test_local_authoring_translates_stale_version_without_leaking_documents(
    tmp_path: Path,
) -> None:
    api = configuration_api(tmp_path)
    route = api.resolve_route("PUT", ("v1", "configuration", "draft"))
    payload = json.dumps(
        {
            "expected_version": 1,
            "configuration": {
                "workflows": {
                    "example": {
                        "workflow": "example",
                        "steps": {},
                        "flow": [{"name": "done", "terminal": True}],
                    }
                },
                "triggers": {},
            },
        },
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            route,
            query={},
            headers=(),
            read_body=lambda: body(payload),
            scope_binding=scope_binding(),
        )

    assert raised.value.status is HTTPStatus.CONFLICT
    assert raised.value.code == "configuration_conflict"
    assert "workflow" not in str(raised.value).lower()


async def test_workflow_fragment_round_trip_and_conflicts(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    read_route = api.resolve_route("GET", ("v1", "configuration", "draft", "workflows", "example"))
    assert read_route.kind is ConfigurationRouteKind.READ_WORKFLOW_FRAGMENT
    assert read_route.action is AuthorizationAction.CONFIGURATION_VIEW

    fragment = await api.dispatch(
        read_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert fragment.payload is not None
    assert fragment.payload["workflow"] == "example"
    assert "workflow: example" in fragment.payload["document"]
    version = fragment.payload["version"]

    update_route = api.resolve_route(
        "PUT", ("v1", "configuration", "draft", "workflows", "example")
    )
    updated_yaml = (
        b"workflow: example\ndescription: updated\nsteps: {}\n"
        b"flow:\n  - name: done\n    terminal: true\n"
    )
    updated = await api.dispatch(
        update_route,
        query={"expected_version": [str(version)]},
        headers=(),
        read_body=lambda: body(updated_yaml),
        scope_binding=scope_binding(),
    )
    assert updated.status is HTTPStatus.OK
    assert updated.payload is not None
    assert updated.payload["version"] != version

    reread = await api.dispatch(
        read_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert reread.payload is not None
    assert "description: updated" in reread.payload["document"]

    with pytest.raises(ConfigurationApiRequestError) as stale:
        await api.dispatch(
            update_route,
            query={"expected_version": [str(version)]},
            headers=(),
            read_body=lambda: body(updated_yaml),
            scope_binding=scope_binding(),
        )
    assert stale.value.status is HTTPStatus.CONFLICT
    assert stale.value.code == "configuration_conflict"

    current_version = reread.payload["version"]
    mismatched = b"workflow: renamed\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    with pytest.raises(ConfigurationApiRequestError) as mismatch:
        await api.dispatch(
            update_route,
            query={"expected_version": [str(current_version)]},
            headers=(),
            read_body=lambda: body(mismatched),
            scope_binding=scope_binding(),
        )
    assert mismatch.value.status is HTTPStatus.UNPROCESSABLE_ENTITY
    assert mismatch.value.code == "invalid_workflow_fragment"

    missing_route = api.resolve_route(
        "GET", ("v1", "configuration", "draft", "workflows", "absent")
    )
    with pytest.raises(ConfigurationApiRequestError) as absent:
        await api.dispatch(
            missing_route,
            query={},
            headers=(),
            read_body=lambda: body(b""),
            scope_binding=scope_binding(),
        )
    assert absent.value.status is HTTPStatus.NOT_FOUND

    with pytest.raises(ConfigurationApiRequestError) as invalid_name:
        api.resolve_route("GET", ("v1", "configuration", "draft", "workflows", "bad name!"))
    assert invalid_name.value.status is HTTPStatus.NOT_FOUND


async def test_workflow_fragment_preview_is_stateless_and_discriminated(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    preview_route = api.resolve_route(
        "POST", ("v1", "configuration", "draft", "workflows", "example", "preview")
    )
    assert preview_route.kind is ConfigurationRouteKind.PREVIEW_WORKFLOW_FRAGMENT
    assert preview_route.action is AuthorizationAction.CONFIGURATION_VALIDATE

    ready = await api.dispatch(
        preview_route,
        query={},
        headers=(),
        read_body=lambda: body(
            b"workflow: example\ndescription: previewed\nsteps: {}\n"
            b"flow:\n  - name: done\n    terminal: true\n"
        ),
        scope_binding=scope_binding(),
    )
    assert ready.payload is not None
    assert ready.payload["status"] == "graph_ready"
    assert ready.payload["draft_validated"] is False
    assert ready.payload["diagnostics"] == []
    assert [node["node_id"] for node in ready.payload["graph_nodes"]] == ["done"]

    hostile_marker = "hostile-secret-value"
    invalid = await api.dispatch(
        preview_route,
        query={},
        headers=(),
        read_body=lambda: body(
            f"workflow: example\nsteps: {{}}\nflow: {hostile_marker}\n".encode()
        ),
        scope_binding=scope_binding(),
    )
    assert invalid.payload is not None
    assert invalid.payload["status"] == "invalid_fragment"
    assert invalid.payload["draft_validated"] is False
    assert len(invalid.payload["diagnostics"]) >= 1
    assert all(hostile_marker not in issue["message"] for issue in invalid.payload["diagnostics"])

    mismatch = await api.dispatch(
        preview_route,
        query={},
        headers=(),
        read_body=lambda: body(
            b"workflow: renamed\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
        ),
        scope_binding=scope_binding(),
    )
    assert mismatch.payload is not None
    assert mismatch.payload["status"] == "invalid_fragment"
    assert "does not match" in mismatch.payload["diagnostics"][0]["message"]

    # Preview never persists: the draft is unchanged afterwards.
    draft = await api.dispatch(
        api.resolve_route("GET", ("v1", "configuration", "draft")),
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert draft.payload is not None
    assert "previewed" not in json.dumps(draft.payload)


async def test_workflow_fragment_removal_rejects_dependent_triggers(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    read_route = api.resolve_route("GET", ("v1", "configuration", "draft"))
    initial = await api.dispatch(
        read_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert initial.payload is not None
    update_payload = json.dumps(
        {
            "expected_version": initial.payload["version"],
            "configuration": {
                "workflows": {
                    "example": {
                        "workflow": "example",
                        "steps": {},
                        "flow": [{"name": "done", "terminal": True}],
                    },
                    "spare": {
                        "workflow": "spare",
                        "steps": {},
                        "flow": [{"name": "done", "terminal": True}],
                    },
                },
                "triggers": {
                    "nightly": {
                        "kind": "schedule",
                        "workflow": "example",
                        "input": {},
                        "spec": {"kind": "interval", "every_seconds": 3600},
                    }
                },
            },
        },
        separators=(",", ":"),
    ).encode()
    updated = await api.dispatch(
        api.resolve_route("PUT", ("v1", "configuration", "draft")),
        query={},
        headers=(),
        read_body=lambda: body(update_payload),
        scope_binding=scope_binding(),
    )
    assert updated.payload is not None
    version = updated.payload["version"]

    delete_example = api.resolve_route(
        "DELETE", ("v1", "configuration", "draft", "workflows", "example")
    )
    with pytest.raises(ConfigurationApiRequestError) as dependent:
        await api.dispatch(
            delete_example,
            query={"expected_version": [str(version)]},
            headers=(),
            read_body=lambda: body(b""),
            scope_binding=scope_binding(),
        )
    assert dependent.value.status is HTTPStatus.CONFLICT
    assert dependent.value.code == "dependent_triggers"
    assert "nightly" in str(dependent.value)

    delete_spare = api.resolve_route(
        "DELETE", ("v1", "configuration", "draft", "workflows", "spare")
    )
    removed = await api.dispatch(
        delete_spare,
        query={"expected_version": [str(version)]},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert removed.status is HTTPStatus.OK
    assert removed.payload is not None
    assert "spare" not in removed.payload["bundle"]["workflows"]


async def test_local_authoring_validation_projects_warnings_with_severity(
    tmp_path: Path,
) -> None:
    api = configuration_api(tmp_path)
    read_route = api.resolve_route("GET", ("v1", "configuration", "draft"))
    initial = await api.dispatch(
        read_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )
    assert initial.payload is not None
    update_payload = json.dumps(
        {
            "expected_version": initial.payload["version"],
            "configuration": {
                "workflows": {
                    "example": {
                        "workflow": "example",
                        "steps": {"unused": {"workflow": "helper"}},
                        "flow": [{"name": "done", "terminal": True}],
                    },
                    "helper": {
                        "workflow": "helper",
                        "steps": {},
                        "flow": [{"name": "done", "terminal": True}],
                    },
                },
                "triggers": {},
            },
        },
        separators=(",", ":"),
    ).encode()
    update_route = api.resolve_route("PUT", ("v1", "configuration", "draft"))
    updated = await api.dispatch(
        update_route,
        query={},
        headers=(),
        read_body=lambda: body(update_payload),
        scope_binding=scope_binding(),
    )
    assert updated.status is HTTPStatus.OK

    validate_route = api.resolve_route("POST", ("v1", "configuration", "draft", "validate"))
    validation = await api.dispatch(
        validate_route,
        query={},
        headers=(),
        read_body=lambda: body(b""),
        scope_binding=scope_binding(),
    )

    assert validation.payload is not None
    assert validation.payload["valid"] is True
    issues = validation.payload["issues"]
    assert len(issues) == 2
    assert all(issue["severity"] == "warning" for issue in issues)
    assert any("unused" in issue["message"] for issue in issues)


async def test_local_authoring_rejects_validation_request_bodies(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    route = api.resolve_route("POST", ("v1", "configuration", "draft", "validate"))

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            route,
            query={},
            headers=(),
            read_body=lambda: body(b"{}"),
            scope_binding=scope_binding(),
        )

    assert raised.value.status is HTTPStatus.UNPROCESSABLE_ENTITY
    assert raised.value.code == "invalid_configuration"


async def test_local_authoring_rejects_oversized_yaml_imports(tmp_path: Path) -> None:
    api = configuration_api(tmp_path)
    route = api.resolve_route("POST", ("v1", "configuration", "draft", "import"))

    with pytest.raises(ConfigurationApiRequestError) as raised:
        await api.dispatch(
            route,
            query={},
            headers=(),
            read_body=lambda: body(b"x" * (MAX_LOCAL_AUTHORING_YAML_BYTES + 1)),
            scope_binding=scope_binding(),
        )

    assert raised.value.status is HTTPStatus.REQUEST_ENTITY_TOO_LARGE
    assert raised.value.code == "configuration_too_large"
