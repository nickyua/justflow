"""ASGI entry point for the local object-ingestion example."""

from object_ingestion.application import create_application

app = create_application().create_combined_app()
