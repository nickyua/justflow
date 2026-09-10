"""ASGI factory for the independently deployed gateway process."""

from host_application import compose_application
from justflow.config.settings import load_settings
from justflow.runtime import RuntimeAsgiApplication


def create_gateway_application() -> RuntimeAsgiApplication:
    return compose_application(load_settings()).create_gateway_app()
