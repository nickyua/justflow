"""Validation shared by typed S3 catalog settings and storage construction."""

from __future__ import annotations

import ipaddress
import re

S3_BUCKET_MIN_LENGTH = 3
S3_BUCKET_MAX_LENGTH = 63
S3_BUCKET_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
S3_RESERVED_PREFIXES = ("xn--", "sthree-", "amzn_s3_demo_")
S3_RESERVED_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")


def normalize_s3_catalog_prefix(prefix: str) -> str:
    if not prefix or prefix.startswith("/") or "\\" in prefix or "\0" in prefix:
        raise ValueError("S3 catalog prefix must be a non-empty relative object prefix")
    normalized = prefix if prefix.endswith("/") else f"{prefix}/"
    if any(segment in {"", ".", ".."} for segment in normalized[:-1].split("/")):
        raise ValueError("S3 catalog prefix contains an invalid path segment")
    return normalized


def validate_s3_bucket(bucket: str) -> None:
    if not S3_BUCKET_MIN_LENGTH <= len(bucket) <= S3_BUCKET_MAX_LENGTH:
        raise ValueError(
            f"S3 bucket length must be {S3_BUCKET_MIN_LENGTH}-{S3_BUCKET_MAX_LENGTH} characters"
        )
    if S3_BUCKET_PATTERN.fullmatch(bucket) is None:
        raise ValueError("S3 bucket contains unsupported characters or delimiters")
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise ValueError("S3 bucket contains adjacent invalid delimiters")
    if bucket.startswith(S3_RESERVED_PREFIXES) or bucket.endswith(S3_RESERVED_SUFFIXES):
        raise ValueError("S3 bucket uses an AWS-reserved prefix or suffix")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise ValueError("S3 bucket must not be formatted as an IP address")
