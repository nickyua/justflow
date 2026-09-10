"""Local-only draft API for file-backed workflow and schedule declarations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from http import HTTPStatus
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from justflow.config.diagnostics import (
    MAX_DIAGNOSTIC_MESSAGE_LENGTH,
    DiagnosticCategory,
    DiagnosticSeverity,
)
from justflow.config.grammar import WorkflowName
from justflow.config.loader import ConfigLoadError, load_bounded_yaml
from justflow.config.models import WorkflowConfig
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
)
from justflow.configuration.local_authoring import (
    LOCAL_AUTHORING_SOURCE,
    MAX_LOCAL_AUTHORING_YAML_BYTES,
    LocalAuthoringConfigurationSource,
    LocalAuthoringDocument,
    WorkflowFragmentDependencyError,
    WorkflowFragmentError,
)
from justflow.configuration.publication import (
    MAX_VALIDATION_ISSUES,
    ConfigurationValidationCategory,
    ConfigurationValidationIssue,
    ConfigurationValidationReport,
    ConfigurationValidationSeverity,
)
from justflow.runtime.api_models import (
    TriggersFragmentApiResponse,
    WorkflowFragmentApiResponse,
)
from justflow.runtime.auth import AuthorizationAction
from justflow.runtime.configuration_api import (
    CORRELATION_HEADER,
    IDEMPOTENCY_HEADER,
    MAX_CONFIGURATION_ROUTE_SEGMENTS,
    YAML_CONTENT_TYPE,
    ApplyConfigurationRequest,
    ConfigurationApiRequestError,
    ConfigurationApiResponse,
    ConfigurationAuthoringMode,
    ConfigurationRoute,
    ConfigurationRouteKind,
    DiscardConfigurationRequest,
)
from justflow.runtime.operations_query import (
    MAX_WORKFLOW_GRAPH_EDGES,
    MAX_WORKFLOW_GRAPH_NODES,
    WorkflowGraphEdge,
    WorkflowGraphNode,
    bounded_node_metadata,
    graph_edge_label,
    graph_node_label,
)
from justflow.scope import TrustedScopeBinding
from justflow.visualization.graph import build_graph

_WORKFLOW_NAME_ADAPTER: TypeAdapter[str] = TypeAdapter(WorkflowName)


GraphReadyStatus = Literal["graph_ready"]
InvalidFragmentStatus = Literal["invalid_fragment"]
FragmentOnly = Literal[False]


class _StrictPreviewModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class WorkflowFragmentPreviewReady(_StrictPreviewModel):
    """A parseable fragment: its graph, with whole-draft validation still pending."""

    status: GraphReadyStatus = "graph_ready"
    draft_validated: FragmentOnly = False
    workflow: str
    diagnostics: tuple[ConfigurationValidationIssue, ...] = ()
    graph_nodes: tuple[WorkflowGraphNode, ...] = Field(max_length=MAX_WORKFLOW_GRAPH_NODES)
    graph_edges: tuple[WorkflowGraphEdge, ...] = Field(max_length=MAX_WORKFLOW_GRAPH_EDGES)


class WorkflowFragmentPreviewInvalid(_StrictPreviewModel):
    status: InvalidFragmentStatus = "invalid_fragment"
    draft_validated: FragmentOnly = False
    workflow: str
    diagnostics: tuple[ConfigurationValidationIssue, ...] = Field(
        min_length=1, max_length=MAX_VALIDATION_ISSUES
    )


def _preview_issue(message: str, location: tuple[str, ...] = ()) -> ConfigurationValidationIssue:
    return ConfigurationValidationIssue(
        severity=ConfigurationValidationSeverity.ERROR,
        category=ConfigurationValidationCategory.DECLARATION,
        location=location,
        message=message[:MAX_DIAGNOSTIC_MESSAGE_LENGTH] or "Workflow fragment is invalid",
    )


def preview_workflow_fragment(
    name: str,
    payload: bytes,
) -> WorkflowFragmentPreviewReady | WorkflowFragmentPreviewInvalid:
    """Stateless fragment preview: no persistence, no Temporal, fragment-only validity."""
    if len(payload) > MAX_LOCAL_AUTHORING_YAML_BYTES:
        raise ConfigurationLimitError("Workflow fragment exceeds its byte limit")
    try:
        data = load_bounded_yaml(payload, source=LOCAL_AUTHORING_SOURCE)
    except ConfigLoadError as exc:
        return WorkflowFragmentPreviewInvalid(
            workflow=name,
            diagnostics=(_preview_issue(str(exc)),),
        )
    try:
        workflow = WorkflowConfig.model_validate(data)
    except ValidationError as exc:
        issues = tuple(
            _preview_issue(
                str(error.get("msg", "Invalid value")),
                tuple(str(part) for part in error.get("loc", ())),
            )
            for error in exc.errors(include_url=False, include_input=False)[:MAX_VALIDATION_ISSUES]
        )
        return WorkflowFragmentPreviewInvalid(workflow=name, diagnostics=issues)
    if workflow.workflow != name:
        return WorkflowFragmentPreviewInvalid(
            workflow=name,
            diagnostics=(
                _preview_issue(
                    "Workflow fragment declaration name does not match the requested workflow",
                    ("workflow",),
                ),
            ),
        )
    graph = build_graph(workflow)
    if len(graph.nodes) > MAX_WORKFLOW_GRAPH_NODES or len(graph.edges) > MAX_WORKFLOW_GRAPH_EDGES:
        return WorkflowFragmentPreviewInvalid(
            workflow=name,
            diagnostics=(_preview_issue("Workflow graph exceeds its preview bound"),),
        )
    return WorkflowFragmentPreviewReady(
        workflow=name,
        graph_nodes=tuple(
            WorkflowGraphNode(
                node_id=node.id,
                label=graph_node_label(node.id, node.node_type),
                kind=node.node_type,
                group=node.group,
                metadata=bounded_node_metadata(node.metadata),
            )
            for node in graph.nodes
        ),
        graph_edges=tuple(
            WorkflowGraphEdge(
                source=edge.source,
                target=edge.target,
                label=graph_edge_label(edge.label),
                dashed=edge.style == "dashed",
            )
            for edge in graph.edges
        ),
    )


class LocalAuthoringRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    configuration: LocalAuthoringDocument


class UpdateLocalAuthoringRequest(LocalAuthoringRequest):
    expected_version: int = Field(ge=1)


class LocalAuthoringConfigurationApi:
    def __init__(self, source: LocalAuthoringConfigurationSource) -> None:
        self._source = source

    @property
    def authoring_mode(self) -> ConfigurationAuthoringMode:
        return ConfigurationAuthoringMode.LOCAL_SOURCE

    @property
    def supported_actions(self) -> frozenset[AuthorizationAction]:
        return frozenset(
            {
                AuthorizationAction.CONFIGURATION_VIEW,
                AuthorizationAction.CONFIGURATION_EDIT,
                AuthorizationAction.CONFIGURATION_VALIDATE,
                AuthorizationAction.CONFIGURATION_APPLY,
                AuthorizationAction.CONFIGURATION_DISCARD,
            }
        )

    def resolve_route(self, method: str, segments: tuple[str, ...]) -> ConfigurationRoute:
        if len(segments) > MAX_CONFIGURATION_ROUTE_SEGMENTS:
            raise ConfigurationApiRequestError(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                "path_too_large",
                "Configuration route exceeds its segment bound",
            )
        suffix = segments[2:]
        if suffix == ("draft",):
            routes = {
                "POST": ConfigurationRoute(
                    kind=ConfigurationRouteKind.CREATE_DRAFT,
                    action=AuthorizationAction.CONFIGURATION_EDIT,
                ),
                "GET": ConfigurationRoute(
                    kind=ConfigurationRouteKind.READ_DRAFT,
                    action=AuthorizationAction.CONFIGURATION_VIEW,
                ),
                "PUT": ConfigurationRoute(
                    kind=ConfigurationRouteKind.UPDATE_DRAFT,
                    action=AuthorizationAction.CONFIGURATION_EDIT,
                ),
            }
            route = routes.get(method)
            if route is not None:
                return route
        if suffix == ("draft", "import") and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.IMPORT_DRAFT,
                action=AuthorizationAction.CONFIGURATION_EDIT,
            )
        if suffix == ("draft", "export") and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.EXPORT_DRAFT,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            )
        if suffix == ("draft", "validate") and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.VALIDATE_DRAFT,
                action=AuthorizationAction.CONFIGURATION_VALIDATE,
            )
        if suffix == ("relationships",) and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.READ_RELATIONSHIPS,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            )
        if suffix == ("apply",) and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.APPLY,
                action=AuthorizationAction.CONFIGURATION_APPLY,
            )
        if suffix == ("draft", "discard") and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.DISCARD,
                action=AuthorizationAction.CONFIGURATION_DISCARD,
            )
        if len(suffix) == 2 and suffix[0] == "discards" and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.READ_DISCARD,
                action=AuthorizationAction.CONFIGURATION_VIEW,
                resource=suffix[1],
            )
        if (
            len(suffix) == 4
            and suffix[:2] == ("draft", "workflows")
            and suffix[3] == "preview"
            and method == "POST"
        ):
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.PREVIEW_WORKFLOW_FRAGMENT,
                action=AuthorizationAction.CONFIGURATION_VALIDATE,
                resource=_workflow_resource(suffix[2]),
            )
        if suffix == ("draft", "triggers"):
            trigger_routes = {
                "GET": ConfigurationRoute(
                    kind=ConfigurationRouteKind.READ_TRIGGERS_FRAGMENT,
                    action=AuthorizationAction.CONFIGURATION_VIEW,
                ),
                "PUT": ConfigurationRoute(
                    kind=ConfigurationRouteKind.UPDATE_TRIGGERS_FRAGMENT,
                    action=AuthorizationAction.CONFIGURATION_EDIT,
                ),
            }
            trigger_route = trigger_routes.get(method)
            if trigger_route is not None:
                return trigger_route
        if len(suffix) == 3 and suffix[:2] == ("draft", "workflows"):
            fragment_routes = {
                "GET": ConfigurationRoute(
                    kind=ConfigurationRouteKind.READ_WORKFLOW_FRAGMENT,
                    action=AuthorizationAction.CONFIGURATION_VIEW,
                    resource=_workflow_resource(suffix[2]),
                ),
                "PUT": ConfigurationRoute(
                    kind=ConfigurationRouteKind.UPDATE_WORKFLOW_FRAGMENT,
                    action=AuthorizationAction.CONFIGURATION_EDIT,
                    resource=_workflow_resource(suffix[2]),
                ),
                "DELETE": ConfigurationRoute(
                    kind=ConfigurationRouteKind.DELETE_WORKFLOW_FRAGMENT,
                    action=AuthorizationAction.CONFIGURATION_EDIT,
                    resource=_workflow_resource(suffix[2]),
                ),
            }
            fragment_route = fragment_routes.get(method)
            if fragment_route is not None:
                return fragment_route
        raise ConfigurationApiRequestError(
            HTTPStatus.NOT_FOUND,
            "not_found",
            "Configuration route is not available for local source authoring",
        )

    async def dispatch(
        self,
        route: ConfigurationRoute,
        *,
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        read_body: Callable[[], Awaitable[bytes]],
        scope_binding: TrustedScopeBinding,
    ) -> ConfigurationApiResponse:
        try:
            if route.kind is ConfigurationRouteKind.READ_DRAFT:
                _require_empty_query(query)
                draft = self._source.read_draft(scope_binding.scope)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.CREATE_DRAFT:
                _require_empty_query(query)
                request = LocalAuthoringRequest.model_validate_json(await read_body())
                draft = self._source.create_draft(
                    scope_binding.scope,
                    request.configuration,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.CREATED,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.UPDATE_DRAFT:
                _require_empty_query(query)
                request = UpdateLocalAuthoringRequest.model_validate_json(await read_body())
                draft = self._source.update_draft(
                    scope_binding.scope,
                    request.configuration,
                    expected_version=request.expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.IMPORT_DRAFT:
                _only_query_fields(query, {"expected_version"})
                expected_version = _expected_version(query)
                document = self._source.parse_yaml(await read_body())
                draft = (
                    self._source.create_draft(scope_binding.scope, document)
                    if expected_version is None
                    else self._source.update_draft(
                        scope_binding.scope,
                        document,
                        expected_version=expected_version,
                    )
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.EXPORT_DRAFT:
                _only_query_fields(query, {"expected_version"})
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    content_type=YAML_CONTENT_TYPE,
                    raw_body=self._source.export_draft_yaml(
                        scope_binding.scope, expected_version=_expected_version(query)
                    ),
                )
            if route.kind is ConfigurationRouteKind.READ_TRIGGERS_FRAGMENT:
                _require_empty_query(query)
                triggers_fragment, triggers_version = self._source.read_triggers_fragment(
                    scope_binding.scope
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=TriggersFragmentApiResponse(
                        scope_digest=scope_binding.scope.digest,
                        version=triggers_version,
                        document=triggers_fragment.decode("utf-8"),
                    ).model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.UPDATE_TRIGGERS_FRAGMENT:
                _only_query_fields(query, {"expected_version"})
                expected_version = _expected_version(query)
                if expected_version is None:
                    raise ValueError("Triggers fragment updates require an expected version")
                draft = self._source.update_triggers_fragment(
                    scope_binding.scope,
                    await read_body(),
                    expected_version=expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_WORKFLOW_FRAGMENT:
                _require_empty_query(query)
                fragment, version = self._source.read_workflow_fragment(
                    scope_binding.scope,
                    _required_resource(route),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=WorkflowFragmentApiResponse(
                        scope_digest=scope_binding.scope.digest,
                        workflow=_required_resource(route),
                        version=version,
                        document=fragment.decode("utf-8"),
                    ).model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.UPDATE_WORKFLOW_FRAGMENT:
                _only_query_fields(query, {"expected_version"})
                expected_version = _expected_version(query)
                if expected_version is None:
                    raise ValueError("Workflow fragment updates require an expected version")
                draft = self._source.update_workflow_fragment(
                    scope_binding.scope,
                    _required_resource(route),
                    await read_body(),
                    expected_version=expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.DELETE_WORKFLOW_FRAGMENT:
                _only_query_fields(query, {"expected_version"})
                expected_version = _expected_version(query)
                if expected_version is None:
                    raise ValueError("Workflow fragment removal requires an expected version")
                if await read_body():
                    raise ValueError("Workflow fragment removal must not carry a body")
                draft = self._source.delete_workflow_fragment(
                    scope_binding.scope,
                    _required_resource(route),
                    expected_version=expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.PREVIEW_WORKFLOW_FRAGMENT:
                _require_empty_query(query)
                preview = preview_workflow_fragment(
                    _required_resource(route),
                    await read_body(),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=preview.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.VALIDATE_DRAFT:
                _require_empty_query(query)
                if await read_body():
                    raise ValueError("Validation request body must be empty")
                result = self._source.validate_draft(scope_binding.scope)
                issues = tuple(
                    ConfigurationValidationIssue(
                        severity=_validation_severity(diagnostic.severity),
                        category=_validation_category(diagnostic.category),
                        location=tuple(str(part) for part in diagnostic.location),
                        message=diagnostic.message,
                    )
                    for diagnostic in result.diagnostics[:MAX_VALIDATION_ISSUES]
                )
                report = ConfigurationValidationReport(
                    valid=result.is_valid,
                    issues=issues,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=report.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_RELATIONSHIPS:
                _require_empty_query(query)
                relationships = self._source.relationships(scope_binding.scope)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=relationships.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.APPLY:
                _require_empty_query(query)
                apply_request = ApplyConfigurationRequest.model_validate_json(await read_body())
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                apply_result = self._source.apply(
                    scope_binding.scope,
                    expected_version=apply_request.expected_draft_version,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=apply_result.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.DISCARD:
                _require_empty_query(query)
                discard_request = DiscardConfigurationRequest.model_validate_json(await read_body())
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                discard_result = self._source.discard(
                    scope_binding.scope,
                    expected_version=discard_request.expected_draft_version,
                    expected_active_identity=discard_request.expected_active_identity,
                    idempotency_key=idempotency_key,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=discard_result.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_DISCARD:
                _require_empty_query(query)
                record = self._source.read_discard(
                    scope_binding.scope,
                    _required_resource(route),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=record.model_dump(mode="json"),
                )
            raise RuntimeError("Unsupported local authoring route")
        except ConfigurationNotFoundError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Workflow is not present in the draft",
            ) from exc
        except WorkflowFragmentDependencyError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.CONFLICT,
                "dependent_triggers",
                str(exc),
            ) from exc
        except WorkflowFragmentError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_workflow_fragment",
                str(exc),
            ) from exc
        except ConfigurationConflictError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.CONFLICT,
                "configuration_conflict",
                "Local authoring draft changed concurrently",
            ) from exc
        except ConfigurationLimitError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "configuration_too_large",
                str(exc),
            ) from exc
        except ConfigurationUnavailableError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "configuration_unavailable",
                "Local authoring storage is unavailable",
            ) from exc
        except (ConfigurationError, ValidationError, ValueError) as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_configuration",
                "Local authoring declaration is invalid",
            ) from exc


def _workflow_resource(candidate: str) -> str:
    try:
        return _WORKFLOW_NAME_ADAPTER.validate_python(candidate)
    except ValidationError as exc:
        raise ConfigurationApiRequestError(
            HTTPStatus.NOT_FOUND,
            "not_found",
            "Configuration route is not available for local source authoring",
        ) from exc


def _required_resource(route: ConfigurationRoute) -> str:
    if route.resource is None:
        raise RuntimeError("Workflow fragment route requires a resource identity")
    return route.resource


def _validation_category(category: DiagnosticCategory) -> ConfigurationValidationCategory:
    if category is DiagnosticCategory.DECLARATION:
        return ConfigurationValidationCategory.DECLARATION
    return ConfigurationValidationCategory.SEMANTIC


def _validation_severity(severity: DiagnosticSeverity) -> ConfigurationValidationSeverity:
    if severity is DiagnosticSeverity.WARNING:
        return ConfigurationValidationSeverity.WARNING
    return ConfigurationValidationSeverity.ERROR


def _expected_version(query: Mapping[str, list[str]]) -> int | None:
    values = query.get("expected_version")
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError("Expected version must contain one value")
    return int(values[0])


def _only_query_fields(query: Mapping[str, list[str]], allowed: set[str]) -> None:
    if set(query) - allowed:
        raise ValueError("Unknown local authoring query field")


def _require_empty_query(query: Mapping[str, list[str]]) -> None:
    _only_query_fields(query, set())


def _required_header(
    headers: tuple[tuple[bytes, bytes], ...],
    name: bytes,
) -> str:
    values = [value for key, value in headers if key.lower() == name]
    if len(values) != 1:
        raise ValueError("Required request header must occur exactly once")
    try:
        decoded = values[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Required request header is not valid UTF-8") from exc
    if not decoded:
        raise ValueError("Required request header must not be empty")
    return decoded


def _correlation_identity(
    headers: tuple[tuple[bytes, bytes], ...],
    idempotency_key: str,
) -> str:
    values = [value for key, value in headers if key.lower() == CORRELATION_HEADER]
    if not values:
        return idempotency_key
    if len(values) != 1:
        raise ValueError("Correlation header must not be repeated")
    try:
        decoded = values[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Correlation header is not valid UTF-8") from exc
    if not decoded:
        raise ValueError("Correlation header must not be empty")
    return decoded
