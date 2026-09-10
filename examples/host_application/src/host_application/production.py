"""Explicit production factories; managed activation is supplied by the consuming host."""

from justflow_admin import BetaAdminPanel

from host_application.authentication import (
    BrowserAuthenticationBoundary,
    HostAuthentication,
    HostSettings,
)
from host_application.resources import create_resource_registry
from justflow.config.settings import Settings, load_settings
from justflow.provenance import RuntimeProfile
from justflow.runtime import RuntimeApplication, WorkerAsgiApplication


def require_production(settings: Settings) -> None:
    if settings.runtime.profile is not RuntimeProfile.PRODUCTION:
        raise ValueError("The production host requires the production runtime profile")


def create_gateway_application() -> BrowserAuthenticationBoundary:
    settings = load_settings()
    require_production(settings)
    host_settings = HostSettings().for_scope(settings.runtime.scope)
    authentication = HostAuthentication(host_settings)
    application = RuntimeApplication(
        settings,
        resource_registry=create_resource_registry(),
        authentication=authentication,
        admin_panel=BetaAdminPanel() if settings.operations.admin_panel_enabled else None,
    )
    return BrowserAuthenticationBoundary(
        application.create_gateway_app(), authentication, host_settings
    )


def create_worker_application() -> WorkerAsgiApplication:
    settings = load_settings()
    require_production(settings)
    return RuntimeApplication(
        settings, resource_registry=create_resource_registry()
    ).create_worker_app()
