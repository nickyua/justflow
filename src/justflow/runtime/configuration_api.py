"""Authenticated bounded HTTP operations for configuration publication and activation."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from justflow.config.settings import MAX_CONTROL_LIST_LIMIT, ControlSettings
from justflow.configuration.activation import ProvenanceDigest, WorkerReadinessRegistration
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
    ActivationUnavailableError,
)
from justflow.configuration.errors import (
    ConfigurationConflictError,
    ConfigurationError,
    ConfigurationIntegrityError,
    ConfigurationLimitError,
    ConfigurationNotFoundError,
    ConfigurationUnavailableError,
)
from justflow.configuration.models import RevisionIdentity, TenantConfiguration
from justflow.configuration.publication import (
    ConfigurationPublicationService,
    PublicationOperationError,
)
from justflow.runtime.api_models import ActivationReadinessApiResponse
from justflow.runtime.auth import AuthorizationAction
from justflow.runtime.blocking_io import ControlPlaneBusyError, run_blocking
from justflow.runtime.configuration_activation import (
    ActivationControllerError,
    ConfigurationActivationController,
)
from justflow.scope import TrustedScopeBinding

YAML_CONTENT_TYPE = b"application/yaml; charset=utf-8"
MAX_CONFIGURATION_ROUTE_SEGMENTS = 6
IDEMPOTENCY_HEADER = b"x-idempotency-key"
CORRELATION_HEADER = b"x-correlation-id"


class StrictConfigurationApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class CreateDraftRequest(StrictConfigurationApiModel):
    configuration: TenantConfiguration


class UpdateDraftRequest(StrictConfigurationApiModel):
    expected_version: int = Field(ge=1)
    configuration: TenantConfiguration


class PublishConfigurationRequest(StrictConfigurationApiModel):
    expected_draft_version: int = Field(ge=1)


class ApplyConfigurationRequest(StrictConfigurationApiModel):
    expected_draft_version: int = Field(ge=1)


class DiscardConfigurationRequest(ApplyConfigurationRequest):
    expected_active_identity: RevisionIdentity


class PlanActivationRequest(StrictConfigurationApiModel):
    target_revision_id: RevisionIdentity


class ActivateConfigurationRequest(PlanActivationRequest):
    plan_digest: ProvenanceDigest


class RollbackConfigurationRequest(StrictConfigurationApiModel):
    plan_digest: ProvenanceDigest


class ConfigurationRouteKind(str, Enum):
    CREATE_DRAFT = "create_draft"
    READ_DRAFT = "read_draft"
    UPDATE_DRAFT = "update_draft"
    IMPORT_DRAFT = "import_draft"
    EXPORT_DRAFT = "export_draft"
    VALIDATE_DRAFT = "validate_draft"
    READ_RELATIONSHIPS = "read_relationships"
    APPLY = "apply"
    DISCARD = "discard"
    READ_DISCARD = "read_discard"
    READ_WORKFLOW_FRAGMENT = "read_workflow_fragment"
    UPDATE_WORKFLOW_FRAGMENT = "update_workflow_fragment"
    DELETE_WORKFLOW_FRAGMENT = "delete_workflow_fragment"
    PREVIEW_WORKFLOW_FRAGMENT = "preview_workflow_fragment"
    READ_TRIGGERS_FRAGMENT = "read_triggers_fragment"
    UPDATE_TRIGGERS_FRAGMENT = "update_triggers_fragment"
    LIST_REVISIONS = "list_revisions"
    READ_REVISION = "read_revision"
    COMPARE_REVISIONS = "compare_revisions"
    PUBLISH = "publish"
    READ_PUBLICATION = "read_publication"
    PLAN_ACTIVATION = "plan_activation"
    ACTIVATE = "activate"
    LIST_ACTIVATIONS = "list_activations"
    READ_ACTIVATION = "read_activation"
    READ_READINESS = "read_readiness"
    REGISTER_READINESS = "register_readiness"
    ROLLBACK = "rollback"


class ConfigurationAuthoringMode(str, Enum):
    MANAGED = "managed"
    LOCAL_SOURCE = "local_source"


@dataclass(frozen=True, kw_only=True)
class ConfigurationRoute:
    kind: ConfigurationRouteKind
    action: AuthorizationAction
    resource: str | None = None


@dataclass(frozen=True, kw_only=True)
class ConfigurationApiResponse:
    status: HTTPStatus
    payload: object | None = None
    content_type: bytes | None = None
    raw_body: bytes | None = None


class ConfigurationApiRequestError(Exception):
    def __init__(self, status: HTTPStatus, code: str, message: str) -> None:
        self.status = status
        self.code = code
        super().__init__(message)


class ConfigurationApiBinding(Protocol):
    @property
    def authoring_mode(self) -> ConfigurationAuthoringMode: ...

    @property
    def supported_actions(self) -> frozenset[AuthorizationAction]: ...

    def resolve_route(
        self,
        method: str,
        segments: tuple[str, ...],
    ) -> ConfigurationRoute: ...

    async def dispatch(
        self,
        route: ConfigurationRoute,
        *,
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        read_body: Callable[[], Awaitable[bytes]],
        scope_binding: TrustedScopeBinding,
    ) -> ConfigurationApiResponse: ...


class ConfigurationApi:
    def __init__(
        self,
        *,
        settings: ControlSettings,
        publication: ConfigurationPublicationService,
        activation: ConfigurationActivationController,
    ) -> None:
        self._settings = settings
        self._publication = publication
        self._activation = activation

    @property
    def authoring_mode(self) -> ConfigurationAuthoringMode:
        return ConfigurationAuthoringMode.MANAGED

    @property
    def supported_actions(self) -> frozenset[AuthorizationAction]:
        return frozenset(
            {
                AuthorizationAction.CONFIGURATION_VIEW,
                AuthorizationAction.CONFIGURATION_EDIT,
                AuthorizationAction.CONFIGURATION_VALIDATE,
                AuthorizationAction.CONFIGURATION_DISCARD,
                AuthorizationAction.CONFIGURATION_PUBLISH,
                AuthorizationAction.CONFIGURATION_ACTIVATE,
                AuthorizationAction.CONFIGURATION_ROLLBACK,
            }
        )

    def resolve_route(
        self,
        method: str,
        segments: tuple[str, ...],
    ) -> ConfigurationRoute:
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
            if method in routes:
                return routes[method]
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
        if suffix == ("revisions",) and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.LIST_REVISIONS,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            )
        if suffix == ("revisions", "compare") and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.COMPARE_REVISIONS,
                action=AuthorizationAction.CONFIGURATION_VIEW,
            )
        if len(suffix) == 2 and suffix[0] == "revisions" and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.READ_REVISION,
                action=AuthorizationAction.CONFIGURATION_VIEW,
                resource=suffix[1],
            )
        if suffix == ("publications",) and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.PUBLISH,
                action=AuthorizationAction.CONFIGURATION_PUBLISH,
            )
        if len(suffix) == 2 and suffix[0] == "publications" and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.READ_PUBLICATION,
                action=AuthorizationAction.CONFIGURATION_VIEW,
                resource=suffix[1],
            )
        if suffix == ("activations", "plan") and method == "POST":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.PLAN_ACTIVATION,
                action=AuthorizationAction.CONFIGURATION_ACTIVATE,
            )
        if suffix == ("activations",):
            routes = {
                "POST": ConfigurationRoute(
                    kind=ConfigurationRouteKind.ACTIVATE,
                    action=AuthorizationAction.CONFIGURATION_ACTIVATE,
                ),
                "GET": ConfigurationRoute(
                    kind=ConfigurationRouteKind.LIST_ACTIVATIONS,
                    action=AuthorizationAction.CONFIGURATION_VIEW,
                ),
            }
            if method in routes:
                return routes[method]
        if len(suffix) == 2 and suffix[0] == "activations" and method == "GET":
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.READ_ACTIVATION,
                action=AuthorizationAction.CONFIGURATION_VIEW,
                resource=suffix[1],
            )
        if len(suffix) == 3 and suffix[0] == "activations" and suffix[2] == "readiness":
            routes = {
                "GET": ConfigurationRoute(
                    kind=ConfigurationRouteKind.READ_READINESS,
                    action=AuthorizationAction.CONFIGURATION_VIEW,
                    resource=suffix[1],
                ),
                "POST": ConfigurationRoute(
                    kind=ConfigurationRouteKind.REGISTER_READINESS,
                    action=AuthorizationAction.CONFIGURATION_ACTIVATE,
                    resource=suffix[1],
                ),
            }
            if method in routes:
                return routes[method]
        if (
            len(suffix) == 3
            and suffix[0] == "activations"
            and suffix[2] == "rollback"
            and method == "POST"
        ):
            return ConfigurationRoute(
                kind=ConfigurationRouteKind.ROLLBACK,
                action=AuthorizationAction.CONFIGURATION_ROLLBACK,
                resource=suffix[1],
            )
        raise ConfigurationApiRequestError(
            HTTPStatus.NOT_FOUND,
            "not_found",
            "Configuration route not found",
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
        scope = scope_binding.scope
        try:
            if route.kind is ConfigurationRouteKind.CREATE_DRAFT:
                _require_empty_query(query)
                create_request = CreateDraftRequest.model_validate(_parse_json(await read_body()))
                draft = await run_blocking(
                    self._publication.create_draft, scope, create_request.configuration
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.CREATED,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_DRAFT:
                _require_empty_query(query)
                draft = await run_blocking(self._publication.read_draft, scope)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.UPDATE_DRAFT:
                _require_empty_query(query)
                update_request = UpdateDraftRequest.model_validate(_parse_json(await read_body()))
                draft = await run_blocking(
                    self._publication.update_draft,
                    scope,
                    update_request.configuration,
                    expected_version=update_request.expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.IMPORT_DRAFT:
                _only_query_fields(query, {"expected_version"})
                expected_version_text = _single_query_value(query, "expected_version")
                expected_version = (
                    int(expected_version_text) if expected_version_text is not None else None
                )
                draft = await run_blocking(
                    self._publication.import_draft_yaml,
                    scope,
                    await read_body(),
                    expected_version=expected_version,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=draft.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.EXPORT_DRAFT:
                _only_query_fields(query, {"expected_version"})
                expected_version_text = _single_query_value(query, "expected_version")
                expected_version = (
                    int(expected_version_text) if expected_version_text is not None else None
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    content_type=YAML_CONTENT_TYPE,
                    raw_body=await run_blocking(
                        self._publication.export_draft_yaml,
                        scope,
                        expected_version=expected_version,
                    ),
                )
            if route.kind is ConfigurationRouteKind.VALIDATE_DRAFT:
                _require_empty_query(query)
                _require_empty_body(await read_body())
                report = await run_blocking(self._publication.validate_draft, scope)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=report.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_RELATIONSHIPS:
                _require_empty_query(query)
                relationships = await run_blocking(self._publication.relationships, scope)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=relationships.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.DISCARD:
                _require_empty_query(query)
                discard_request = DiscardConfigurationRequest.model_validate(
                    _parse_json(await read_body())
                )
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                result = await run_blocking(
                    self._publication.discard,
                    scope,
                    expected_draft_version=discard_request.expected_draft_version,
                    expected_active_identity=discard_request.expected_active_identity,
                    idempotency_key=idempotency_key,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=result.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_DISCARD:
                _require_empty_query(query)
                record = await run_blocking(
                    self._publication.read_discard, scope, _required_resource(route)
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=record.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.LIST_REVISIONS:
                _only_query_fields(query, {"limit", "cursor"})
                revision_page = await run_blocking(
                    self._publication.list_history,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=revision_page.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_REVISION:
                _require_empty_query(query)
                revision = await run_blocking(
                    self._publication.read_revision,
                    scope,
                    RevisionIdentity(_required_resource(route)),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=revision.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.COMPARE_REVISIONS:
                _only_query_fields(query, {"source_revision_id", "target_revision_id"})
                difference = await run_blocking(
                    self._publication.compare_revisions,
                    scope,
                    RevisionIdentity(_required_query_value(query, "source_revision_id")),
                    RevisionIdentity(_required_query_value(query, "target_revision_id")),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=difference.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.PUBLISH:
                _require_empty_query(query)
                publish_request = PublishConfigurationRequest.model_validate(
                    _parse_json(await read_body())
                )
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                publication = await run_blocking(
                    self._publication.publish,
                    scope,
                    expected_draft_version=publish_request.expected_draft_version,
                    idempotency_key=idempotency_key,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.ACCEPTED,
                    payload=publication.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_PUBLICATION:
                _require_empty_query(query)
                publication = await run_blocking(
                    self._publication.read_publication,
                    scope,
                    _required_resource(route),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=publication.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.PLAN_ACTIVATION:
                _require_empty_query(query)
                plan_request = PlanActivationRequest.model_validate(_parse_json(await read_body()))
                plan = await self._activation.plan(scope, plan_request.target_revision_id)
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=plan.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.ACTIVATE:
                _require_empty_query(query)
                activation_request = ActivateConfigurationRequest.model_validate(
                    _parse_json(await read_body())
                )
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                activation = await self._activation.activate(
                    scope,
                    activation_request.target_revision_id,
                    plan_digest=activation_request.plan_digest,
                    idempotency_key=idempotency_key,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.ACCEPTED,
                    payload=activation.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.LIST_ACTIVATIONS:
                _only_query_fields(query, {"limit", "cursor"})
                activation_page = await run_blocking(
                    self._activation.list,
                    scope,
                    limit=_list_limit(query, self._settings),
                    cursor=_single_query_value(query, "cursor"),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=activation_page.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_ACTIVATION:
                _require_empty_query(query)
                activation = await run_blocking(
                    self._activation.read, scope, _required_resource(route)
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=activation.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.READ_READINESS:
                _require_empty_query(query)
                activation = await run_blocking(
                    self._activation.read, scope, _required_resource(route)
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.OK,
                    payload=ActivationReadinessApiResponse(
                        activation_id=activation.activation_id,
                        ready=activation.state.value == "applied",
                        state=activation.state,
                        target_revision_id=str(activation.plan.target_revision_id),
                        worker_readiness_registered=activation.worker_readiness is not None,
                        completed_checkpoints=tuple(
                            checkpoint.kind for checkpoint in activation.checkpoints
                        ),
                    ).model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.REGISTER_READINESS:
                _require_empty_query(query)
                registration = WorkerReadinessRegistration.model_validate(
                    _parse_json(await read_body())
                )
                activation = await self._activation.register_readiness(
                    scope,
                    _required_resource(route),
                    registration,
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.ACCEPTED,
                    payload=activation.model_dump(mode="json"),
                )
            if route.kind is ConfigurationRouteKind.ROLLBACK:
                _require_empty_query(query)
                rollback_request = RollbackConfigurationRequest.model_validate(
                    _parse_json(await read_body())
                )
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                activation = await self._activation.rollback(
                    scope,
                    _required_resource(route),
                    plan_digest=rollback_request.plan_digest,
                    idempotency_key=idempotency_key,
                    actor_identity=scope_binding.binding_id,
                    correlation_identity=_correlation_identity(headers, idempotency_key),
                )
                return ConfigurationApiResponse(
                    status=HTTPStatus.ACCEPTED,
                    payload=activation.model_dump(mode="json"),
                )
        except ControlPlaneBusyError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.SERVICE_UNAVAILABLE, "control_plane_busy", str(exc)
            ) from exc
        except (TypeError, ValueError, ValidationError) as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Configuration request is invalid",
            ) from exc
        except PublicationOperationError as exc:
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if exc.retryable
                else (
                    HTTPStatus.CONFLICT
                    if exc.code.value == "conflict"
                    else HTTPStatus.UNPROCESSABLE_ENTITY
                )
            )
            raise ConfigurationApiRequestError(status, exc.code.value, str(exc)) from exc
        except ConfigurationNotFoundError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.NOT_FOUND,
                "configuration_not_found",
                "Configuration state was not found",
            ) from exc
        except ConfigurationConflictError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.CONFLICT,
                "configuration_conflict",
                "Configuration state changed concurrently",
            ) from exc
        except ConfigurationLimitError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.BAD_REQUEST,
                "configuration_limit",
                "Configuration operation exceeds its bound",
            ) from exc
        except ConfigurationUnavailableError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "configuration_unavailable",
                "Configuration storage is unavailable",
            ) from exc
        except ConfigurationIntegrityError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "configuration_integrity",
                "Stored configuration is inconsistent",
            ) from exc
        except ActivationControllerError as exc:
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if exc.retryable
                else (
                    HTTPStatus.CONFLICT
                    if exc.code.value in {"stale_plan", "configuration_conflict"}
                    else HTTPStatus.UNPROCESSABLE_ENTITY
                )
            )
            raise ConfigurationApiRequestError(status, exc.code.value, str(exc)) from exc
        except ActivationNotFoundError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.NOT_FOUND,
                "activation_not_found",
                "Configuration activation was not found",
            ) from exc
        except ActivationConflictError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.CONFLICT,
                "activation_conflict",
                "Configuration activation changed concurrently",
            ) from exc
        except ActivationLimitError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.BAD_REQUEST,
                "activation_limit",
                "Configuration activation operation exceeds its bound",
            ) from exc
        except ActivationUnavailableError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "activation_unavailable",
                "Configuration activation storage is unavailable",
            ) from exc
        except ActivationIntegrityError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "activation_integrity",
                "Stored configuration activation is inconsistent",
            ) from exc
        except ConfigurationError as exc:
            raise ConfigurationApiRequestError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_configuration",
                "Configuration document is invalid",
            ) from exc
        raise RuntimeError("Configuration route is not implemented")


def _parse_json(payload: bytes) -> object:
    try:
        return json.loads(
            payload,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ConfigurationApiRequestError(
            HTTPStatus.BAD_REQUEST,
            "invalid_json",
            "Request body must be strict JSON",
        ) from exc


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> Any:
    raise ValueError(f"Non-finite JSON number '{value}'")


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


def _optional_header(
    headers: tuple[tuple[bytes, bytes], ...],
    name: bytes,
) -> str | None:
    values = [value for key, value in headers if key.lower() == name]
    if not values:
        return None
    if len(values) != 1:
        raise ValueError("Request header must not be repeated")
    try:
        decoded = values[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Request header is not valid UTF-8") from exc
    if not decoded:
        raise ValueError("Request header must not be empty")
    return decoded


def _correlation_identity(
    headers: tuple[tuple[bytes, bytes], ...],
    idempotency_key: str,
) -> str:
    return _optional_header(headers, CORRELATION_HEADER) or idempotency_key


def _single_query_value(query: Mapping[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError("Query field must have exactly one non-empty value")
    return values[0]


def _required_query_value(query: Mapping[str, list[str]], name: str) -> str:
    value = _single_query_value(query, name)
    if value is None:
        raise ValueError("Required query field is missing")
    return value


def _only_query_fields(query: Mapping[str, list[str]], allowed: set[str]) -> None:
    if set(query) - allowed:
        raise ValueError("Unknown query field")


def _require_empty_query(query: Mapping[str, list[str]]) -> None:
    if query:
        raise ValueError("Route does not accept query fields")


def _require_empty_body(payload: bytes) -> None:
    if payload:
        raise ValueError("Route does not accept a request body")


def _list_limit(query: Mapping[str, list[str]], settings: ControlSettings) -> int:
    value = _single_query_value(query, "limit")
    limit = settings.default_list_limit if value is None else int(value)
    if not 1 <= limit <= MAX_CONTROL_LIST_LIMIT:
        raise ValueError("List limit is invalid")
    return limit


def _required_resource(route: ConfigurationRoute) -> str:
    if route.resource is None:
        raise RuntimeError("Configuration route has no resource identity")
    return route.resource
