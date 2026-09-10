"""Safe Temporal Visibility indexes owned by the runtime host."""

from __future__ import annotations

from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

SCOPE_DIGEST_SEARCH_ATTRIBUTE = "JustflowScopeDigest"
LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE = "JustflowLogicalWorkflow"
DEFINITION_DIGEST_SEARCH_ATTRIBUTE = "JustflowDefinitionDigest"
TRIGGER_SOURCE_SEARCH_ATTRIBUTE = "JustflowTriggerSource"
WORKER_ARTIFACT_SEARCH_ATTRIBUTE = "JustflowWorkerArtifact"


def execution_search_attributes(
    *,
    scope_digest: str,
    logical_workflow: str,
    definition_digest: str,
    trigger_source: str,
    worker_artifact_digest: str,
) -> TypedSearchAttributes:
    return TypedSearchAttributes(
        [
            SearchAttributePair(
                SearchAttributeKey.for_keyword(SCOPE_DIGEST_SEARCH_ATTRIBUTE),
                scope_digest,
            ),
            SearchAttributePair(
                SearchAttributeKey.for_keyword(LOGICAL_WORKFLOW_SEARCH_ATTRIBUTE),
                logical_workflow,
            ),
            SearchAttributePair(
                SearchAttributeKey.for_keyword(DEFINITION_DIGEST_SEARCH_ATTRIBUTE),
                definition_digest,
            ),
            SearchAttributePair(
                SearchAttributeKey.for_keyword(TRIGGER_SOURCE_SEARCH_ATTRIBUTE),
                trigger_source,
            ),
            SearchAttributePair(
                SearchAttributeKey.for_keyword(WORKER_ARTIFACT_SEARCH_ATTRIBUTE),
                worker_artifact_digest,
            ),
        ]
    )
