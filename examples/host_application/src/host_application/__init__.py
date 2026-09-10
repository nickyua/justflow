"""Host-owned Justflow runtime composition."""

from __future__ import annotations

from pathlib import Path

from host_application.configuration import initialize_local_configuration_database
from host_application.resources import create_resource_registry
from justflow.brokers.sqs import builtin_broker_registry
from justflow.config.settings import (
    ControlSettings,
    DeploymentSettings,
    FileConfigurationSettings,
    LocalTemporalConnectionSettings,
    PathSettings,
    RuntimeSettings,
    Settings,
    SqliteConfigurationSettings,
    TemporalSettings,
)
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, RuntimeProfile
from justflow.runtime import RuntimeApplication
from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthenticationError,
    AuthenticationRequest,
    AuthorizationRequest,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope
from justflow.transports.builtins import builtin_transport_registry

LOCAL_SUBJECT_HEADER = b"x-host-subject"
LOCAL_SUBJECT = b"local-operator"
EXAMPLE_ROOT = Path(__file__).resolve().parent


class LocalHostAuthentication:
    async def authenticate(
        self,
        request: AuthenticationRequest,
    ) -> AuthenticatedPrincipal:
        subjects = [value for name, value in request.headers if name == LOCAL_SUBJECT_HEADER]
        if subjects != [LOCAL_SUBJECT]:
            raise AuthenticationError("Local host subject is required")
        return AuthenticatedPrincipal.for_local_development("local-operator")

    async def authorize(
        self,
        principal: AuthenticatedPrincipal,
        _request: AuthorizationRequest,
    ) -> bool:
        return principal.principal_id == "local-operator"


def compose_application(settings: Settings) -> RuntimeApplication:
    if settings.runtime.profile is not RuntimeProfile.LOCAL:
        raise ValueError(
            "Local host authentication requires the local profile; use the production host"
        )
    return RuntimeApplication(
        settings,
        transport_registry=builtin_transport_registry(),
        broker_registry=builtin_broker_registry(),
        resource_registry=create_resource_registry(),
        authentication=LocalHostAuthentication(),
    )


def create_application(
    *,
    configuration_database: str | Path | None = None,
    scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
) -> RuntimeApplication:
    configuration = (
        FileConfigurationSettings()
        if configuration_database is None
        else SqliteConfigurationSettings(path=str(configuration_database))
    )
    settings = Settings(
        configuration=configuration,
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL, scope=scope),
        temporal=TemporalSettings(
            address="127.0.0.1:7233",
            connection=LocalTemporalConnectionSettings(host="127.0.0.1"),
        ),
        control=ControlSettings(host="127.0.0.1", port=8080),
        paths=PathSettings(config_dir=str(EXAMPLE_ROOT / "configs")),
        deployment=DeploymentSettings(
            name="justflow-local",
            build_id="development",
            artifact_digest=LOCAL_ARTIFACT_DIGEST,
            package_version="development",
        ),
    )
    if configuration_database is not None:
        initialize_local_configuration_database(
            configuration_database,
            EXAMPLE_ROOT / "configs",
            scope=scope,
            limits=settings.limits.snapshot(),
        )
    return compose_application(settings)


__all__ = [
    "LocalHostAuthentication",
    "compose_application",
    "create_application",
    "initialize_local_configuration_database",
]
