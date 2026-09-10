"""Immutable runtime limits captured when workflows are compiled."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_TRIGGER_PAYLOAD_BYTES = 256 * 1024
DEFAULT_WORKFLOW_OUTPUT_BYTES = 1024 * 1024
DEFAULT_ACTIVITY_INPUT_BYTES = 512 * 1024
DEFAULT_ACTIVITY_OUTPUT_BYTES = 512 * 1024
DEFAULT_FAILURE_RECORD_BYTES = 32 * 1024
DEFAULT_AUDIT_RECORD_BYTES = 1024 * 1024
DEFAULT_CACHE_ENTRY_BYTES = 512 * 1024
DEFAULT_FANOUT_ITEMS = 1000
DEFAULT_FANOUT_CHUNK_ITEMS = 20
DEFAULT_PARALLELISM = 32
DEFAULT_LOOP_ATTEMPTS = 1000
DEFAULT_TOTAL_INVOCATIONS = 10_000
DEFAULT_QUEUED_MESSAGES = 1000
DEFAULT_SIGNAL_PAYLOAD_BYTES = 256 * 1024
DEFAULT_QUEUED_MESSAGE_BYTES = 256 * 1024
DEFAULT_COLLECTION_ITEMS = 10_000
DEFAULT_WORKFLOW_STATE_BYTES = 768 * 1024
DEFAULT_CONTINUATION_INPUT_BYTES = 1536 * 1024
DEFAULT_HISTORY_EVENTS = 10_000
DEFAULT_HISTORY_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True, kw_only=True)
class RuntimeLimits:
    trigger_payload_bytes: int = DEFAULT_TRIGGER_PAYLOAD_BYTES
    workflow_output_bytes: int = DEFAULT_WORKFLOW_OUTPUT_BYTES
    activity_input_bytes: int = DEFAULT_ACTIVITY_INPUT_BYTES
    activity_output_bytes: int = DEFAULT_ACTIVITY_OUTPUT_BYTES
    failure_record_bytes: int = DEFAULT_FAILURE_RECORD_BYTES
    audit_record_bytes: int = DEFAULT_AUDIT_RECORD_BYTES
    cache_entry_bytes: int = DEFAULT_CACHE_ENTRY_BYTES
    fanout_items: int = DEFAULT_FANOUT_ITEMS
    fanout_chunk_items: int = DEFAULT_FANOUT_CHUNK_ITEMS
    parallelism: int = DEFAULT_PARALLELISM
    loop_attempts: int = DEFAULT_LOOP_ATTEMPTS
    total_invocations: int = DEFAULT_TOTAL_INVOCATIONS
    queued_messages: int = DEFAULT_QUEUED_MESSAGES
    signal_payload_bytes: int = DEFAULT_SIGNAL_PAYLOAD_BYTES
    queued_message_bytes: int = DEFAULT_QUEUED_MESSAGE_BYTES
    collection_items: int = DEFAULT_COLLECTION_ITEMS
    workflow_state_bytes: int = DEFAULT_WORKFLOW_STATE_BYTES
    continuation_input_bytes: int = DEFAULT_CONTINUATION_INPUT_BYTES
    history_events: int = DEFAULT_HISTORY_EVENTS
    history_bytes: int = DEFAULT_HISTORY_BYTES

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if value < 1:
                raise ValueError(f"Runtime limit '{name}' must be positive")
        if self.fanout_chunk_items > self.fanout_items:
            raise ValueError("fanout_chunk_items cannot exceed fanout_items")


DEFAULT_RUNTIME_LIMITS = RuntimeLimits()
