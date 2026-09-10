"""Tests for the container reference application's host boundary."""

from __future__ import annotations

import pytest
from host_application import (
    LOCAL_SUBJECT,
    LOCAL_SUBJECT_HEADER,
    LocalHostAuthentication,
)

from justflow.runtime.auth import AuthenticationError, AuthenticationRequest


async def test_local_container_authentication_accepts_the_registered_subject() -> None:
    authentication = LocalHostAuthentication()

    principal = await authentication.authenticate(
        AuthenticationRequest(
            method="GET",
            path="/v1/operations",
            headers=((LOCAL_SUBJECT_HEADER, LOCAL_SUBJECT),),
        )
    )

    assert principal.principal_id == "local-operator"


async def test_local_container_authentication_rejects_missing_subject() -> None:
    authentication = LocalHostAuthentication()

    with pytest.raises(AuthenticationError, match="subject is required"):
        await authentication.authenticate(
            AuthenticationRequest(
                method="GET",
                path="/v1/operations",
                headers=(),
            )
        )
