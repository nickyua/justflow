"""ASGI entry point for the combined local runtime deployment."""

from host_application import create_application

app = create_application().create_combined_app()
