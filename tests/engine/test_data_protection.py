"""Tests for audit capture, strict serialization, and payload encryption."""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure

from justflow.config.models import (
    ApprovedFullAuditCapture,
    AuditCaptureConfig,
    AuditCaptureMode,
    MetadataOnlyAuditCapture,
    RedactedAuditCapture,
)
from justflow.config.settings import CodecPayloadProtection, PlaintextPayloadProtection
from justflow.engine.audit_capture import (
    REDACTED_VALUE,
    AuditCaptureLimitError,
    capture_audit_record,
)
from justflow.engine.payload_protection import (
    EncryptedPayloadCodec,
    PayloadProtectionBinding,
    PayloadProtectionError,
    configured_data_converter,
    decode_archive_record,
    encode_archive_record,
)
from justflow.engine.serialization import StrictJsonLayout, dumps_strict_json

SENSITIVE_SENTINEL = "synthetic-secret-value"
OLD_KEY_ID = "key-2026-01"
NEW_KEY_ID = "key-2026-08"
PAYLOAD_LIMIT_BYTES = 4_096
TINY_PAYLOAD_LIMIT_BYTES = 1

AUDIT_RECORD = {
    "audit_version": 4,
    "serialization_version": 1,
    "request_id": "opaque-request-1",
    "workflow": "synthetic_record_flow",
    "status": "completed",
    "reason": None,
    "params": {"token": SENSITIVE_SENTINEL, "region": "eu"},
    "steps": {
        "load": {
            "seq": 0,
            "status": "succeeded",
            "input": {"token": SENSITIVE_SENTINEL},
            "output": {"record": SENSITIVE_SENTINEL},
            "duration_ms": 10,
        }
    },
    "transitions": [],
    "started_at": "2026-08-02T00:00:00+00:00",
    "completed_at": "2026-08-02T00:00:01+00:00",
    "result": {"record": SENSITIVE_SENTINEL},
}


@dataclass(frozen=True, kw_only=True)
class AuditCaptureCase:
    id: str
    capture: AuditCaptureConfig
    expected_mode: AuditCaptureMode
    sentinel_present: bool
    result_present: bool
    expected_input_token: str | None


AUDIT_CAPTURE_CASES = [
    AuditCaptureCase(
        id="metadata-only",
        capture=MetadataOnlyAuditCapture(),
        expected_mode=AuditCaptureMode.METADATA_ONLY,
        sentinel_present=False,
        result_present=False,
        expected_input_token=None,
    ),
    AuditCaptureCase(
        id="redacted",
        capture=RedactedAuditCapture(
            paths=[
                "/params/token",
                "/steps/*/input/token",
                "/steps/*/output/record",
                "/result/record",
            ],
            max_payload_bytes=PAYLOAD_LIMIT_BYTES,
        ),
        expected_mode=AuditCaptureMode.REDACTED,
        sentinel_present=False,
        result_present=True,
        expected_input_token=REDACTED_VALUE,
    ),
    AuditCaptureCase(
        id="approved-full",
        capture=ApprovedFullAuditCapture(max_payload_bytes=PAYLOAD_LIMIT_BYTES),
        expected_mode=AuditCaptureMode.APPROVED_FULL,
        sentinel_present=True,
        result_present=True,
        expected_input_token=SENSITIVE_SENTINEL,
    ),
]


@pytest.mark.parametrize("case", AUDIT_CAPTURE_CASES, ids=lambda case: case.id)
def test_audit_capture_policy(case: AuditCaptureCase) -> None:
    captured = capture_audit_record(AUDIT_RECORD, case.capture)
    serialized = dumps_strict_json(captured, layout=StrictJsonLayout.CANONICAL)

    assert captured["capture_mode"] == case.expected_mode
    assert (SENSITIVE_SENTINEL in serialized) is case.sentinel_present
    assert ("result" in captured) is case.result_present
    if case.expected_input_token is None:
        assert "input" not in captured["steps"]["load"]
    else:
        assert captured["steps"]["load"]["input"]["token"] == case.expected_input_token


def test_captured_payload_limit_is_applied_after_redaction() -> None:
    capture = RedactedAuditCapture(
        paths=["/params/token"],
        max_payload_bytes=TINY_PAYLOAD_LIMIT_BYTES,
    )

    with pytest.raises(AuditCaptureLimitError, match="configured maximum"):
        capture_audit_record(AUDIT_RECORD, capture)


class DeterministicTestCipher:
    def __init__(self, keys: dict[str, bytes]) -> None:
        self._keys = keys

    async def encrypt_authenticated(
        self,
        plaintext: bytes,
        *,
        key_id: str,
        associated_data: bytes,
    ) -> bytes:
        key = self._keys[key_id]
        ciphertext = self._transform(plaintext, key)
        return hmac.digest(key, associated_data + ciphertext, "sha256") + ciphertext

    async def decrypt_authenticated(
        self,
        ciphertext: bytes,
        *,
        key_id: str,
        associated_data: bytes,
    ) -> bytes:
        key = self._keys[key_id]
        tag = ciphertext[:32]
        encrypted = ciphertext[32:]
        expected = hmac.digest(key, associated_data + encrypted, "sha256")
        if not hmac.compare_digest(tag, expected):
            raise ValueError("authentication failed")
        return self._transform(encrypted, key)

    @staticmethod
    def _transform(value: bytes, key: bytes) -> bytes:
        return bytes(byte ^ key[index % len(key)] for index, byte in enumerate(value))


def binding(
    cipher: DeterministicTestCipher,
    *,
    active_key_id: str,
    readable_key_ids: frozenset[str],
) -> PayloadProtectionBinding:
    return PayloadProtectionBinding(
        cipher=cipher,
        active_key_id=active_key_id,
        readable_key_ids=readable_key_ids,
    )


async def test_archive_and_temporal_payloads_survive_key_rotation() -> None:
    cipher = DeterministicTestCipher(
        {
            OLD_KEY_ID: b"old-test-key",
            NEW_KEY_ID: b"new-test-key",
        }
    )
    old_binding = binding(
        cipher,
        active_key_id=OLD_KEY_ID,
        readable_key_ids=frozenset({OLD_KEY_ID}),
    )
    rotated_binding = binding(
        cipher,
        active_key_id=NEW_KEY_ID,
        readable_key_ids=frozenset({OLD_KEY_ID, NEW_KEY_ID}),
    )

    archive = await encode_archive_record(AUDIT_RECORD, old_binding)
    payload = Payload(data=SENSITIVE_SENTINEL.encode("utf-8"))
    encoded_payload = (await EncryptedPayloadCodec(old_binding).encode([payload]))[0]
    encoded_failure = Failure()
    await old_binding.data_converter().encode_failure(
        RuntimeError(SENSITIVE_SENTINEL),
        encoded_failure,
    )

    assert SENSITIVE_SENTINEL not in archive
    assert SENSITIVE_SENTINEL.encode("utf-8") not in encoded_payload.data
    assert SENSITIVE_SENTINEL not in str(encoded_failure)
    assert await decode_archive_record(archive, rotated_binding) == AUDIT_RECORD
    assert (await EncryptedPayloadCodec(rotated_binding).decode([encoded_payload]))[0] == payload
    decoded_failure = await rotated_binding.data_converter().decode_failure(encoded_failure)
    assert SENSITIVE_SENTINEL in str(decoded_failure)


async def test_missing_rotation_key_fails_without_exposing_plaintext() -> None:
    cipher = DeterministicTestCipher({OLD_KEY_ID: b"old-test-key", NEW_KEY_ID: b"new-test-key"})
    old_binding = binding(
        cipher,
        active_key_id=OLD_KEY_ID,
        readable_key_ids=frozenset({OLD_KEY_ID}),
    )
    new_only_binding = binding(
        cipher,
        active_key_id=NEW_KEY_ID,
        readable_key_ids=frozenset({NEW_KEY_ID}),
    )
    archive = await encode_archive_record(AUDIT_RECORD, old_binding)

    with pytest.raises(PayloadProtectionError) as exc_info:
        await decode_archive_record(archive, new_only_binding)

    assert SENSITIVE_SENTINEL not in str(exc_info.value)


async def test_temporal_codec_rejects_plaintext_payload() -> None:
    cipher = DeterministicTestCipher({NEW_KEY_ID: b"new-test-key"})
    codec = EncryptedPayloadCodec(
        binding(
            cipher,
            active_key_id=NEW_KEY_ID,
            readable_key_ids=frozenset({NEW_KEY_ID}),
        )
    )

    with pytest.raises(PayloadProtectionError, match="not encrypted"):
        await codec.decode([Payload(data=b"plaintext")])


async def test_temporal_codec_rejects_tampered_ciphertext() -> None:
    cipher = DeterministicTestCipher({NEW_KEY_ID: b"new-test-key"})
    codec = EncryptedPayloadCodec(
        binding(
            cipher,
            active_key_id=NEW_KEY_ID,
            readable_key_ids=frozenset({NEW_KEY_ID}),
        )
    )
    encoded = (await codec.encode([Payload(data=b"protected")]))[0]
    encoded.data = bytes([encoded.data[0] ^ 1]) + encoded.data[1:]

    with pytest.raises(PayloadProtectionError, match="decryption failed"):
        await codec.decode([encoded])


async def test_archive_codec_rejects_tampered_associated_metadata() -> None:
    cipher = DeterministicTestCipher({OLD_KEY_ID: b"old-test-key", NEW_KEY_ID: b"new-test-key"})
    active_binding = binding(
        cipher,
        active_key_id=OLD_KEY_ID,
        readable_key_ids=frozenset({OLD_KEY_ID, NEW_KEY_ID}),
    )
    archive = json.loads(await encode_archive_record(AUDIT_RECORD, active_binding))
    archive["key_id"] = NEW_KEY_ID

    with pytest.raises(PayloadProtectionError, match="decryption failed"):
        await decode_archive_record(json.dumps(archive), active_binding)


async def test_archive_recovery_enforces_envelope_byte_limit() -> None:
    cipher = DeterministicTestCipher({NEW_KEY_ID: b"new-test-key"})
    active_binding = binding(
        cipher,
        active_key_id=NEW_KEY_ID,
        readable_key_ids=frozenset({NEW_KEY_ID}),
    )
    archive = await encode_archive_record(AUDIT_RECORD, active_binding)

    with pytest.raises(PayloadProtectionError, match="exceeds its byte limit"):
        await decode_archive_record(
            archive,
            active_binding,
            max_envelope_bytes=TINY_PAYLOAD_LIMIT_BYTES,
        )


@pytest.mark.parametrize(
    ("settings", "match"),
    [
        pytest.param(
            PlaintextPayloadProtection(),
            "requires Temporal payload_protection.mode='codec'",
            id="binding-with-plaintext-mode",
        ),
        pytest.param(
            CodecPayloadProtection(
                active_key_id=NEW_KEY_ID,
                readable_key_ids=frozenset({OLD_KEY_ID, NEW_KEY_ID}),
            ),
            "active payload key ids differ",
            id="active-key-mismatch",
        ),
    ],
)
def test_payload_binding_must_match_settings(settings, match: str) -> None:
    cipher = DeterministicTestCipher({OLD_KEY_ID: b"old-test-key"})
    old_binding = binding(
        cipher,
        active_key_id=OLD_KEY_ID,
        readable_key_ids=frozenset({OLD_KEY_ID}),
    )

    with pytest.raises(PayloadProtectionError, match=match):
        configured_data_converter(settings, old_binding)
