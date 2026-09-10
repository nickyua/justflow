"""Generated OpenAPI, runtime-route, packaging, and export contracts."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate

from justflow.__main__ import main
from justflow.openapi import (
    OPENAPI_FILE_NAME,
    OpenApiExportConflictError,
    build_openapi_document,
    export_openapi_document,
    load_bundled_openapi_document,
    public_api_routes,
)
from justflow.runtime.configuration_api import ConfigurationApi, ConfigurationRouteKind
from justflow.runtime.local_authoring_api import LocalAuthoringConfigurationApi
from justflow.runtime.operations_api import OperationsApi, OperationsRouteKind


@dataclass(frozen=True, kw_only=True)
class RouteResolutionCase:
    id: str
    owner: str
    method: str
    path: str
    expected: str


ROUTE_RESOLUTION_CASES = [
    RouteResolutionCase(
        id="operations-overview",
        owner="operations",
        method="GET",
        path="/v1/operations",
        expected=OperationsRouteKind.OVERVIEW.value,
    ),
    RouteResolutionCase(
        id="operations-workflow-definition",
        owner="operations",
        method="GET",
        path="/v1/operations/workflows/example/definition",
        expected=OperationsRouteKind.WORKFLOW_DEFINITION.value,
    ),
    RouteResolutionCase(
        id="operations-scheduled-start",
        owner="operations",
        method="GET",
        path="/v1/operations/scheduled-starts/opaque",
        expected=OperationsRouteKind.SCHEDULED_START_DETAIL.value,
    ),
    RouteResolutionCase(
        id="managed-publication",
        owner="managed_configuration",
        method="POST",
        path="/v1/configuration/publications",
        expected=ConfigurationRouteKind.PUBLISH.value,
    ),
    RouteResolutionCase(
        id="managed-readiness",
        owner="managed_configuration",
        method="GET",
        path="/v1/configuration/activations/opaque/readiness",
        expected=ConfigurationRouteKind.READ_READINESS.value,
    ),
    RouteResolutionCase(
        id="local-workflow-fragment",
        owner="local_configuration",
        method="PUT",
        path="/v1/configuration/draft/workflows/example",
        expected=ConfigurationRouteKind.UPDATE_WORKFLOW_FRAGMENT.value,
    ),
    RouteResolutionCase(
        id="local-trigger-fragment",
        owner="local_configuration",
        method="GET",
        path="/v1/configuration/draft/triggers",
        expected=ConfigurationRouteKind.READ_TRIGGERS_FRAGMENT.value,
    ),
]


def test_bundled_openapi_matches_runtime_generation() -> None:
    assert load_bundled_openapi_document() == build_openapi_document()


def test_openapi_is_valid_versioned_and_complete() -> None:
    document = load_bundled_openapi_document()

    validate(document)
    assert document["openapi"] == "3.1.0"
    assert document["info"]["version"] == "0.1.0"
    assert document["x-justflow-api-compatibility"] == 1
    assert len(document["paths"]) == 55
    assert {
        "/v1/configuration/activations/{activation_id}/readiness",
        "/v1/operations/capabilities",
        "/v1/scheduled-starts",
        "/v1/triggers/{trigger_name}/run",
        "/v1/workflows",
    }.issubset(document["paths"])


def test_openapi_operations_have_stable_unique_identities_and_auth_contracts() -> None:
    document = load_bundled_openapi_document()
    operations = [operation for path in document["paths"].values() for operation in path.values()]
    operation_ids = [operation["operationId"] for operation in operations]

    assert len(operation_ids) == len(set(operation_ids)) == len(public_api_routes())
    assert all("security" in operation for operation in operations)
    assert all("responses" in operation for operation in operations)
    assert all("example" not in json.dumps(operation) for operation in operations)


@pytest.mark.parametrize(
    ("fixture_name", "schema_name"),
    [
        pytest.param("capabilities", "CapabilitiesApiResponse", id="capabilities"),
        pytest.param(
            "scheduled_start",
            "ScheduledStartDescription",
            id="scheduled-start",
        ),
    ],
)
def test_admin_response_fixture_matches_openapi_schema(
    fixture_name: str,
    schema_name: str,
) -> None:
    fixture_path = Path("ui/admin/tests/fixtures/api-responses.json")
    fixtures = json.loads(fixture_path.read_text(encoding="utf-8"))
    document = load_bundled_openapi_document()
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": f"#/components/schemas/{schema_name}",
        "components": document["components"],
    }

    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(fixtures[fixture_name])


@pytest.mark.parametrize(
    "case",
    ROUTE_RESOLUTION_CASES,
    ids=lambda case: case.id,
)
def test_openapi_route_resolves_through_runtime_router(case: RouteResolutionCase) -> None:
    segments = tuple(part for part in case.path.split("/") if part)
    if case.owner == "operations":
        api = object.__new__(OperationsApi)
    elif case.owner == "managed_configuration":
        api = object.__new__(ConfigurationApi)
    else:
        api = object.__new__(LocalAuthoringConfigurationApi)

    route = api.resolve_route(case.method, segments)

    kind = route.kind
    assert kind.value == case.expected


def test_openapi_export_is_offline_idempotent_and_conflict_safe(tmp_path: Path) -> None:
    destination = tmp_path / "api"

    first = export_openapi_document(destination)
    second = export_openapi_document(destination)

    assert first == second == destination / OPENAPI_FILE_NAME
    assert json.loads(first.read_text(encoding="utf-8")) == load_bundled_openapi_document()

    first.write_text("{}\n", encoding="utf-8")
    with pytest.raises(OpenApiExportConflictError, match="overwrite different file"):
        export_openapi_document(destination)


def test_cli_exports_bundled_openapi_document(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    destination = tmp_path / "public-api"
    monkeypatch.setattr(
        sys,
        "argv",
        ["justflow", "api", "schema", "export", "--output", str(destination)],
    )

    main()

    exported = destination / OPENAPI_FILE_NAME
    assert capsys.readouterr().out.strip() == str(exported)
    assert json.loads(exported.read_text(encoding="utf-8")) == load_bundled_openapi_document()
