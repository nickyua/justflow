"""Authenticated bounded HTTP reads for operational runtime state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus

from pydantic import BaseModel, ValidationError

from justflow.config.settings import ControlSettings, ScheduledStartSettings
from justflow.runtime.auth import AuthorizationAction
from justflow.runtime.blocking_io import ControlPlaneBusyError, run_blocking
from justflow.runtime.operations import (
    ControlErrorCode,
    ControlOperationError,
    WorkflowListQuery,
)
from justflow.runtime.operations_query import (
    OperationsQueryError,
    OperationsQueryErrorCode,
    OperationsQueryService,
)
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartState,
)
from justflow.scope import TrustedScopeBinding

MAX_OPERATIONS_ROUTE_SEGMENTS = 5


class OperationsRouteKind(str, Enum):
    OVERVIEW = "operations_overview"
    WORKFLOWS = "operations_workflows"
    WORKFLOW_DETAIL = "operations_workflow_detail"
    DEFINITIONS = "operations_definitions"
    RUNS = "operations_runs"
    RUN_DETAIL = "operations_run_detail"
    TRIGGERS = "operations_triggers"
    CONFIGURATION = "operations_configuration"
    ACTIVATIONS = "operations_activations"
    CONFIGURATION_SCHEMA = "operations_configuration_schema"
    AUTHORING_REFERENCE = "operations_authoring_reference"
    WORKFLOW_DEFINITION = "operations_workflow_definition"
    SCHEDULED_STARTS = "operations_scheduled_starts"
    SCHEDULED_START_DETAIL = "operations_scheduled_start_detail"


@dataclass(frozen=True, kw_only=True)
class OperationsRoute:
    kind: OperationsRouteKind
    action: AuthorizationAction
    resource: str | None = None


@dataclass(frozen=True, kw_only=True)
class OperationsApiResponse:
    status: HTTPStatus
    payload: object


class OperationsApiRequestError(Exception):
    def __init__(self, status: HTTPStatus, code: str, message: str) -> None:
        self.status = status
        self.code = code
        super().__init__(message)


class OperationsApi:
    def __init__(
        self,
        *,
        settings: ControlSettings,
        query_service: OperationsQueryService,
        scheduled_start_settings: ScheduledStartSettings | None = None,
        scheduled_starts: ScheduledStartService | None = None,
    ) -> None:
        self._settings = settings
        self._queries = query_service
        self._scheduled_start_settings = scheduled_start_settings or ScheduledStartSettings()
        self._scheduled_starts = scheduled_starts

    def resolve_route(self, method: str, segments: tuple[str, ...]) -> OperationsRoute:
        if len(segments) > MAX_OPERATIONS_ROUTE_SEGMENTS:
            raise OperationsApiRequestError(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                "path_too_large",
                "Operations route exceeds its segment bound",
            )
        if method != "GET":
            raise OperationsApiRequestError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Operations route not found",
            )
        suffix = segments[2:]
        routes = {
            (): OperationsRoute(
                kind=OperationsRouteKind.OVERVIEW,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("workflows",): OperationsRoute(
                kind=OperationsRouteKind.WORKFLOWS,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("definitions",): OperationsRoute(
                kind=OperationsRouteKind.DEFINITIONS,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("runs",): OperationsRoute(
                kind=OperationsRouteKind.RUNS,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("triggers",): OperationsRoute(
                kind=OperationsRouteKind.TRIGGERS,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("configuration",): OperationsRoute(
                kind=OperationsRouteKind.CONFIGURATION,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("activations",): OperationsRoute(
                kind=OperationsRouteKind.ACTIVATIONS,
                action=AuthorizationAction.OPERATIONS_VIEW,
            ),
            ("configuration-schema",): OperationsRoute(
                kind=OperationsRouteKind.CONFIGURATION_SCHEMA,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            ),
            ("authoring-reference",): OperationsRoute(
                kind=OperationsRouteKind.AUTHORING_REFERENCE,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            ),
            ("scheduled-starts",): OperationsRoute(
                kind=OperationsRouteKind.SCHEDULED_STARTS,
                action=AuthorizationAction.SCHEDULED_START_VIEW,
            ),
        }
        route = routes.get(suffix)
        if route is not None:
            return route
        if len(suffix) == 2 and suffix[0] == "runs":
            return OperationsRoute(
                kind=OperationsRouteKind.RUN_DETAIL,
                action=AuthorizationAction.OPERATIONS_VIEW,
                resource=suffix[1],
            )
        if len(suffix) == 2 and suffix[0] == "workflows":
            return OperationsRoute(
                kind=OperationsRouteKind.WORKFLOW_DETAIL,
                action=AuthorizationAction.OPERATIONS_VIEW,
                resource=suffix[1],
            )
        if len(suffix) == 2 and suffix[0] == "scheduled-starts":
            return OperationsRoute(
                kind=OperationsRouteKind.SCHEDULED_START_DETAIL,
                action=AuthorizationAction.SCHEDULED_START_VIEW,
                resource=suffix[1],
            )
        if len(suffix) == 3 and suffix[0] == "workflows" and suffix[2] == "definition":
            return OperationsRoute(
                kind=OperationsRouteKind.WORKFLOW_DEFINITION,
                action=AuthorizationAction.CONFIGURATION_VIEW,
                resource=suffix[1],
            )
        raise OperationsApiRequestError(
            HTTPStatus.NOT_FOUND,
            "not_found",
            "Operations route not found",
        )

    async def dispatch(
        self,
        route: OperationsRoute,
        *,
        query: Mapping[str, list[str]],
        scope_binding: TrustedScopeBinding,
    ) -> OperationsApiResponse:
        scope = scope_binding.scope
        result: BaseModel
        try:
            if route.kind is OperationsRouteKind.OVERVIEW:
                _require_empty_query(query)
                result = await run_blocking(self._queries.overview)
            elif route.kind is OperationsRouteKind.WORKFLOWS:
                _only_query_fields(query, {"limit", "cursor"})
                result = await run_blocking(
                    self._queries.list_workflows,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
            elif route.kind is OperationsRouteKind.DEFINITIONS:
                _only_query_fields(query, {"limit", "cursor"})
                result = await run_blocking(
                    self._queries.list_definitions,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
            elif route.kind is OperationsRouteKind.WORKFLOW_DETAIL:
                _require_empty_query(query)
                result = await run_blocking(
                    self._queries.workflow_detail, scope, _required_resource(route)
                )
            elif route.kind is OperationsRouteKind.WORKFLOW_DEFINITION:
                _require_empty_query(query)
                result = await run_blocking(
                    self._queries.workflow_definition, scope, _required_resource(route)
                )
            elif route.kind is OperationsRouteKind.AUTHORING_REFERENCE:
                _require_empty_query(query)
                result = await run_blocking(self._queries.authoring_reference, scope)
            elif route.kind is OperationsRouteKind.RUNS:
                _only_query_fields(
                    query,
                    {
                        "limit",
                        "cursor",
                        "workflow",
                        "state",
                        "started_after",
                        "started_before",
                        "definition_digest",
                        "trigger_source",
                        "worker_artifact",
                        "scope",
                    },
                )
                result = await self._queries.list_runs(
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                    query=WorkflowListQuery.model_validate(
                        {
                            field: _single_query_value(query, field)
                            for field in (
                                "workflow",
                                "state",
                                "started_after",
                                "started_before",
                                "definition_digest",
                                "trigger_source",
                                "worker_artifact",
                                "scope",
                            )
                            if field in query
                        }
                    ),
                )
            elif route.kind is OperationsRouteKind.RUN_DETAIL:
                _only_query_fields(query, {"run_id"})
                result = await self._queries.describe_run(
                    scope,
                    _required_resource(route),
                    run_id=_single_query_value(query, "run_id"),
                )
            elif route.kind is OperationsRouteKind.TRIGGERS:
                _require_empty_query(query)
                result = await self._queries.list_triggers(scope)
            elif route.kind is OperationsRouteKind.CONFIGURATION:
                _only_query_fields(query, {"limit", "cursor"})
                result = await run_blocking(
                    self._queries.configuration,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
            elif route.kind is OperationsRouteKind.ACTIVATIONS:
                _only_query_fields(query, {"limit", "cursor"})
                result = await run_blocking(
                    self._queries.activations,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
            elif route.kind is OperationsRouteKind.SCHEDULED_STARTS:
                _only_query_fields(query, {"limit", "cursor", "state"})
                scheduled_starts = self._require_scheduled_starts()
                result = await scheduled_starts.list(
                    scope=scope,
                    limit=_scheduled_start_list_limit(
                        query,
                        self._scheduled_start_settings,
                    ),
                    cursor=_single_query_value(query, "cursor"),
                    states=_scheduled_start_states(query),
                )
            elif route.kind is OperationsRouteKind.SCHEDULED_START_DETAIL:
                _require_empty_query(query)
                result = await self._require_scheduled_starts().describe(
                    _required_resource(route),
                    scope=scope,
                )
            else:
                _require_empty_query(query)
                result = await run_blocking(self._queries.configuration_schema)
        except ControlPlaneBusyError as exc:
            raise OperationsApiRequestError(
                HTTPStatus.SERVICE_UNAVAILABLE, "control_plane_busy", str(exc)
            ) from exc
        except (ValueError, ValidationError) as exc:
            raise OperationsApiRequestError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                "Operations query is invalid",
            ) from exc
        except ControlOperationError as exc:
            raise _control_error(exc) from exc
        except OperationsQueryError as exc:
            raise _query_error(exc) from exc
        except ScheduledStartError as exc:
            raise _scheduled_start_error(exc) from exc
        return OperationsApiResponse(
            status=HTTPStatus.OK,
            payload=result.model_dump(mode="json"),
        )

    def _require_scheduled_starts(self) -> ScheduledStartService:
        if self._scheduled_starts is None:
            raise OperationsApiRequestError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Scheduled-start operations are not available",
            )
        return self._scheduled_starts


def _control_error(exc: ControlOperationError) -> OperationsApiRequestError:
    status = {
        ControlErrorCode.INVALID_REQUEST: HTTPStatus.BAD_REQUEST,
        ControlErrorCode.FILTER_UNAVAILABLE: HTTPStatus.CONFLICT,
        ControlErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
        ControlErrorCode.TEMPORAL_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
    }[exc.code]
    return OperationsApiRequestError(status, exc.code.value, str(exc))


def _query_error(exc: OperationsQueryError) -> OperationsApiRequestError:
    status = {
        OperationsQueryErrorCode.INVALID_QUERY: HTTPStatus.BAD_REQUEST,
        OperationsQueryErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
        OperationsQueryErrorCode.STALE_CURSOR: HTTPStatus.CONFLICT,
        OperationsQueryErrorCode.UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
    }[exc.code]
    return OperationsApiRequestError(status, exc.code.value, str(exc))


def _scheduled_start_error(exc: ScheduledStartError) -> OperationsApiRequestError:
    status = {
        ScheduledStartErrorCode.INVALID_REQUEST: HTTPStatus.BAD_REQUEST,
        ScheduledStartErrorCode.TRIGGER_UNAVAILABLE: HTTPStatus.CONFLICT,
        ScheduledStartErrorCode.TRIGGER_PAUSED: HTTPStatus.CONFLICT,
        ScheduledStartErrorCode.INPUT_REJECTED: HTTPStatus.UNPROCESSABLE_ENTITY,
        ScheduledStartErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
        ScheduledStartErrorCode.CONFLICT: HTTPStatus.CONFLICT,
        ScheduledStartErrorCode.QUOTA_EXCEEDED: HTTPStatus.TOO_MANY_REQUESTS,
        ScheduledStartErrorCode.QUOTA_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
        ScheduledStartErrorCode.PRIORITY_UNSUPPORTED: HTTPStatus.SERVICE_UNAVAILABLE,
        ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
    }[exc.code]
    return OperationsApiRequestError(status, exc.code.value, str(exc))


def _required_resource(route: OperationsRoute) -> str:
    if route.resource is None:
        raise RuntimeError("Operations route requires a resource identity")
    return route.resource


def _single_query_value(query: Mapping[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError("Query field requires one non-empty value")
    return values[0]


def _only_query_fields(query: Mapping[str, list[str]], allowed: set[str]) -> None:
    if set(query) - allowed:
        raise ValueError("Unknown operations query field")


def _require_empty_query(query: Mapping[str, list[str]]) -> None:
    _only_query_fields(query, set())


def _list_limit(query: Mapping[str, list[str]], settings: ControlSettings) -> int:
    value = _single_query_value(query, "limit")
    return settings.default_list_limit if value is None else int(value)


def _scheduled_start_list_limit(
    query: Mapping[str, list[str]],
    settings: ScheduledStartSettings,
) -> int:
    value = _single_query_value(query, "limit")
    return settings.default_list_limit if value is None else int(value)


def _scheduled_start_states(
    query: Mapping[str, list[str]],
) -> frozenset[ScheduledStartState] | None:
    values = query.get("state")
    if values is None:
        return None
    if not values or len(values) > len(ScheduledStartState):
        raise ValueError("Scheduled-start state filter is invalid")
    return frozenset(ScheduledStartState(value) for value in values)
