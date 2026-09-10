"""Shared encryption integration for Temporal payloads and audit archives."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    DataConverter,
    DefaultFailureConverterWithEncodedAttributes,
    PayloadCodec,
)

from justflow.config.runtime_limits import DEFAULT_AUDIT_RECORD_BYTES
from justflow.config.settings import (
    CodecPayloadProtection,
    PayloadProtectionSettings,
    PlaintextPayloadProtection,
)
from justflow.engine.serialization import (
    STRICT_JSON_VERSION,
    StrictJsonError,
    StrictJsonLayout,
    dumps_strict_json,
    loads_strict_json,
    strict_json_bytes,
)

CODEC_FORMAT_VERSION = 2
CODEC_ENCODING = b"binary/justflow-encrypted"
TEMPORAL_PAYLOAD_FORMAT = "justflow-temporal-payload"
ENCODING_METADATA_KEY = "encoding"
KEY_ID_METADATA_KEY = "justflow-key-id"
CODEC_VERSION_METADATA_KEY = "justflow-codec-version"
ARCHIVE_FORMAT = "justflow-encrypted-json"
MAX_KEY_ID_LENGTH = 128
DEFAULT_ARCHIVE_ENVELOPE_BYTES = DEFAULT_AUDIT_RECORD_BYTES * 2
ARCHIVE_ENVELOPE_FIELDS = frozenset(
    {"format", "format_version", "serialization_version", "key_id", "ciphertext"}
)


class PayloadProtectionError(RuntimeError):
    pass


class AuthenticatedPayloadCipher(Protocol):
    """Host AEAD boundary; implementations must reject ciphertext or metadata tampering."""

    async def encrypt_authenticated(
        self,
        plaintext: bytes,
        *,
        key_id: str,
        associated_data: bytes,
    ) -> bytes: ...

    async def decrypt_authenticated(
        self,
        ciphertext: bytes,
        *,
        key_id: str,
        associated_data: bytes,
    ) -> bytes: ...


@dataclass(frozen=True, kw_only=True)
class PayloadProtectionBinding:
    cipher: AuthenticatedPayloadCipher
    active_key_id: str
    readable_key_ids: frozenset[str]

    def __post_init__(self) -> None:
        key_ids = self.readable_key_ids | {self.active_key_id}
        if any(not key_id or len(key_id) > MAX_KEY_ID_LENGTH for key_id in key_ids):
            raise ValueError(
                f"Payload key identifiers must contain 1-{MAX_KEY_ID_LENGTH} characters"
            )
        if self.active_key_id not in self.readable_key_ids:
            raise ValueError("The active payload key must also be readable")

    def data_converter(self) -> DataConverter:
        return DataConverter(
            payload_codec=EncryptedPayloadCodec(self),
            failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
        )


class EncryptedPayloadCodec(PayloadCodec):
    def __init__(self, binding: PayloadProtectionBinding) -> None:
        self._binding = binding

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(await asyncio.gather(*(self._encode_payload(payload) for payload in payloads)))

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(await asyncio.gather(*(self._decode_payload(payload) for payload in payloads)))

    async def _encode_payload(self, payload: Payload) -> Payload:
        try:
            encrypted = await self._binding.cipher.encrypt_authenticated(
                payload.SerializeToString(),
                key_id=self._binding.active_key_id,
                associated_data=_temporal_associated_data(self._binding.active_key_id),
            )
            if not isinstance(encrypted, bytes) or not encrypted:
                raise ValueError("cipher returned no bytes")
        except Exception:  # noqa: BLE001 - host cipher boundary
            raise PayloadProtectionError("Temporal payload encryption failed") from None
        return Payload(
            metadata={
                ENCODING_METADATA_KEY: CODEC_ENCODING,
                KEY_ID_METADATA_KEY: self._binding.active_key_id.encode("utf-8"),
                CODEC_VERSION_METADATA_KEY: str(CODEC_FORMAT_VERSION).encode("ascii"),
            },
            data=encrypted,
        )

    async def _decode_payload(self, payload: Payload) -> Payload:
        if payload.metadata.get(ENCODING_METADATA_KEY) != CODEC_ENCODING:
            raise PayloadProtectionError(
                "Temporal payload is not encrypted by the configured codec"
            )
        if payload.metadata.get(CODEC_VERSION_METADATA_KEY) != str(CODEC_FORMAT_VERSION).encode(
            "ascii"
        ):
            raise PayloadProtectionError("Unsupported Temporal payload codec version")
        key_id = _metadata_key_id(payload)
        _require_readable_key(key_id, self._binding)
        try:
            plaintext = await self._binding.cipher.decrypt_authenticated(
                payload.data,
                key_id=key_id,
                associated_data=_temporal_associated_data(key_id),
            )
            if not isinstance(plaintext, bytes) or not plaintext:
                raise ValueError("cipher returned no plaintext")
            return Payload.FromString(plaintext)
        except Exception:  # noqa: BLE001 - host cipher boundary
            raise PayloadProtectionError("Temporal payload decryption failed") from None


def validate_payload_protection_binding(
    settings: PayloadProtectionSettings,
    binding: PayloadProtectionBinding | None,
) -> None:
    if isinstance(settings, PlaintextPayloadProtection):
        if binding is not None:
            raise PayloadProtectionError(
                "A payload protection binding requires Temporal payload_protection.mode='codec'"
            )
        return
    if binding is None:
        raise PayloadProtectionError(
            "Temporal payload protection mode 'codec' requires a host-provided binding"
        )
    if settings.active_key_id != binding.active_key_id:
        raise PayloadProtectionError("Configured and bound active payload key ids differ")
    if settings.readable_key_ids != binding.readable_key_ids:
        raise PayloadProtectionError("Configured and bound readable payload key ids differ")


def configured_data_converter(
    settings: PayloadProtectionSettings,
    binding: PayloadProtectionBinding | None,
) -> DataConverter | None:
    validate_payload_protection_binding(settings, binding)
    if isinstance(settings, CodecPayloadProtection):
        if binding is None:
            raise PayloadProtectionError("Payload protection binding is unavailable")
        return binding.data_converter()
    return None


async def encode_archive_record(
    record: dict[str, Any],
    binding: PayloadProtectionBinding,
) -> str:
    plaintext = strict_json_bytes(record, layout=StrictJsonLayout.CANONICAL)
    try:
        ciphertext = await binding.cipher.encrypt_authenticated(
            plaintext,
            key_id=binding.active_key_id,
            associated_data=_archive_associated_data(binding.active_key_id),
        )
        if not isinstance(ciphertext, bytes) or not ciphertext:
            raise ValueError("cipher returned no bytes")
    except Exception:  # noqa: BLE001 - host cipher boundary
        raise PayloadProtectionError("Archive encryption failed") from None
    envelope = {
        "format": ARCHIVE_FORMAT,
        "format_version": CODEC_FORMAT_VERSION,
        "serialization_version": STRICT_JSON_VERSION,
        "key_id": binding.active_key_id,
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
    }
    return dumps_strict_json(envelope, layout=StrictJsonLayout.PRETTY)


async def decode_archive_record(
    serialized: str | bytes | bytearray,
    binding: PayloadProtectionBinding,
    *,
    max_envelope_bytes: int = DEFAULT_ARCHIVE_ENVELOPE_BYTES,
) -> dict[str, Any]:
    if max_envelope_bytes < 1:
        raise ValueError("Archive envelope byte limit must be positive")
    try:
        serialized_bytes = (
            serialized.encode("utf-8") if isinstance(serialized, str) else bytes(serialized)
        )
    except UnicodeEncodeError:
        raise PayloadProtectionError("Encrypted archive envelope is not valid UTF-8") from None
    if len(serialized_bytes) > max_envelope_bytes:
        raise PayloadProtectionError("Encrypted archive envelope exceeds its byte limit")
    try:
        envelope = loads_strict_json(serialized_bytes)
    except StrictJsonError:
        raise PayloadProtectionError("Encrypted archive envelope is invalid JSON") from None
    if not isinstance(envelope, dict) or set(envelope) != ARCHIVE_ENVELOPE_FIELDS:
        raise PayloadProtectionError("Encrypted archive envelope has an invalid shape")
    if (
        envelope.get("format") != ARCHIVE_FORMAT
        or envelope.get("format_version") != CODEC_FORMAT_VERSION
        or envelope.get("serialization_version") != STRICT_JSON_VERSION
    ):
        raise PayloadProtectionError("Encrypted archive envelope has an unsupported version")
    key_id = envelope.get("key_id")
    ciphertext_text = envelope.get("ciphertext")
    if not isinstance(key_id, str) or not isinstance(ciphertext_text, str):
        raise PayloadProtectionError("Encrypted archive envelope has invalid fields")
    _require_readable_key(key_id, binding)
    try:
        ciphertext = base64.b64decode(ciphertext_text, validate=True)
        if not ciphertext:
            raise ValueError("archive ciphertext is empty")
        plaintext = await binding.cipher.decrypt_authenticated(
            ciphertext,
            key_id=key_id,
            associated_data=_archive_associated_data(key_id),
        )
        if not isinstance(plaintext, bytes) or not plaintext:
            raise ValueError("cipher returned no plaintext")
        record = loads_strict_json(plaintext)
    except Exception:  # noqa: BLE001 - archive cipher boundary
        raise PayloadProtectionError("Archive decryption failed") from None
    if not isinstance(record, dict):
        raise PayloadProtectionError("Decrypted archive record must be a JSON object")
    return record


def _metadata_key_id(payload: Payload) -> str:
    key_id_bytes = payload.metadata.get(KEY_ID_METADATA_KEY)
    if key_id_bytes is None:
        raise PayloadProtectionError("Encrypted Temporal payload has no key id")
    try:
        key_id = key_id_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raise PayloadProtectionError("Encrypted Temporal payload has an invalid key id") from None
    _validate_key_id(key_id)
    return key_id


def _temporal_associated_data(key_id: str) -> bytes:
    return strict_json_bytes(
        {
            "format": TEMPORAL_PAYLOAD_FORMAT,
            "format_version": CODEC_FORMAT_VERSION,
            "key_id": key_id,
        },
        layout=StrictJsonLayout.CANONICAL,
    )


def _archive_associated_data(key_id: str) -> bytes:
    return strict_json_bytes(
        {
            "format": ARCHIVE_FORMAT,
            "format_version": CODEC_FORMAT_VERSION,
            "serialization_version": STRICT_JSON_VERSION,
            "key_id": key_id,
        },
        layout=StrictJsonLayout.CANONICAL,
    )


def _require_readable_key(key_id: str, binding: PayloadProtectionBinding) -> None:
    _validate_key_id(key_id)
    if key_id not in binding.readable_key_ids:
        raise PayloadProtectionError("Payload decryption key is not available")


def _validate_key_id(key_id: str) -> None:
    if not key_id or len(key_id) > MAX_KEY_ID_LENGTH:
        raise PayloadProtectionError("Encrypted payload has an invalid key id")
