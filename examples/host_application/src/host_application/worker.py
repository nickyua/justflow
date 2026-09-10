"""ASGI factory for the independently deployed worker process."""

from host_application import compose_application
from justflow.config.settings import load_settings
from justflow.runtime import WorkerAsgiApplication


def create_worker_application() -> WorkerAsgiApplication:
    return compose_application(load_settings()).create_worker_app()
