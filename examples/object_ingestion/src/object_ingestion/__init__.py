"""S3 object-created ingestion example package."""

from object_ingestion.actions import LocalObjectService
from object_ingestion.application import create_application, create_event_registry
from object_ingestion.contracts import ObjectReference, ObjectResult

__all__ = [
    "LocalObjectService",
    "ObjectReference",
    "ObjectResult",
    "create_application",
    "create_event_registry",
]
