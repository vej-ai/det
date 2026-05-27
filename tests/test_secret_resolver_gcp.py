"""Tests for :class:`dtex.secrets._gcp.GcpSecretManagerResolver` — stage 9b.

Two paths:

* **Unit tests** (always run): substitute a fake
  ``google.cloud.secretmanager`` module via
  ``monkeypatch.setattr(dtex.secrets._gcp, "_lazy_import_gcp_secrets", ...)``
  and a fake ``google.api_core.exceptions`` module via the same lever on
  ``_lazy_import_google_exceptions``. The fakes record calls and serve
  canned responses; no network, no live GCP.
* **Integration test** (gated): exercises the resolver against a real
  GCP Secret Manager secret, enabled when ``DET_GCP_SECRETS_TEST_PROJECT``
  + ``DET_GCP_SECRETS_TEST_SECRET`` are set. Marked
  ``@pytest.mark.integration`` so the default ``pytest`` run skips it.

To set up the integration test (one-time, on a project the runner's ADC
can access)::

    gcloud secrets create dtex-it-secret --replication-policy=automatic \\
        --project=$DET_GCP_SECRETS_TEST_PROJECT
    echo -n "hello-from-dtex-test" | gcloud secrets versions add dtex-it-secret \\
        --data-file=- --project=$DET_GCP_SECRETS_TEST_PROJECT
    # then in the shell that runs pytest:
    export DET_GCP_SECRETS_TEST_PROJECT=<your-gcp-project>
    export DET_GCP_SECRETS_TEST_SECRET=dtex-it-secret
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from typing import Any

import pytest

from dtex.secrets import (
    SecretResolutionError,
    _reset_resolvers_for_testing,
    resolve_secret_url,
)
from dtex.secrets import _gcp as gcp_resolver_mod
from dtex.secrets._gcp import GcpSecretManagerResolver


@pytest.fixture(autouse=True)
def _clean_resolvers() -> Iterator[None]:
    """Wipe the module-level registry before AND after every test.

    The stage 9a registry is process-global; without this reset, a fake
    resolver leaked from a prior test or this test's own entry-point
    discovery would carry over to the next test. Mirrors the autouse
    fixture in :mod:`tests.test_secret_resolvers`.
    """
    _reset_resolvers_for_testing()
    yield
    _reset_resolvers_for_testing()


# ---------------------------------------------------------------------------
# Fake SDK — substituted via monkeypatch.setattr on the lazy-import accessors
# ---------------------------------------------------------------------------


class _FakePayload:
    """Stand-in for ``response.payload`` — carries one ``data`` bytes attr."""

    def __init__(self, data: bytes) -> None:
        self.data = data


class _FakeResponse:
    """Stand-in for the SDK's ``SecretVersion`` response."""

    def __init__(self, data: bytes) -> None:
        self.payload = _FakePayload(data)


class _FakeClient:
    """Stand-in for ``secretmanager.SecretManagerServiceClient``.

    Records every ``access_secret_version`` call so tests can assert
    name-passing. Behavior is configurable per-instance: a canned
    payload by default, or an exception to raise.
    """

    instances_built = 0

    def __init__(self) -> None:
        type(self).instances_built += 1
        self.calls: list[str] = []
        # Default behavior: serve a canned payload.
        self.payload_bytes: bytes = b"FAKE-CREDENTIAL-VALUE"
        self.raise_on_call: BaseException | None = None

    def access_secret_version(self, name: str) -> _FakeResponse:
        self.calls.append(name)
        if self.raise_on_call is not None:
            exc = self.raise_on_call
            # Reset so subsequent calls succeed (mostly defensive — most
            # tests use one call per resolver).
            self.raise_on_call = None
            raise exc
        return _FakeResponse(self.payload_bytes)


class _FakeSecretManagerModule:
    """Stand-in for the ``google.cloud.secretmanager`` namespace."""

    def __init__(self) -> None:
        # Reset the per-class counter so test isolation is clean — the
        # counter is on the class because tests assert "build called
        # exactly once" against an instance counter that survives one
        # resolver lifetime.
        _FakeClient.instances_built = 0
        self.last_client: _FakeClient | None = None

    def SecretManagerServiceClient(self) -> _FakeClient:  # noqa: N802 — SDK name
        client = _FakeClient()
        self.last_client = client
        return client


# Fake google.api_core.exceptions — only the three classes the resolver
# branches on need to exist. They inherit from a fake ``GoogleAPIError``
# base so the catch-all branch in :meth:`resolve` matches.


class _FakeGoogleAPIError(Exception):
    """Stand-in base for ``google.api_core.exceptions.GoogleAPIError``."""


class _FakePermissionDenied(_FakeGoogleAPIError):
    """Stand-in for :class:`google.api_core.exceptions.PermissionDenied`."""


class _FakeNotFound(_FakeGoogleAPIError):
    """Stand-in for :class:`google.api_core.exceptions.NotFound`."""


class _FakeDeadlineExceeded(_FakeGoogleAPIError):
    """Stand-in for :class:`google.api_core.exceptions.DeadlineExceeded`."""


class _FakeExceptionsModule:
    """Stand-in for ``google.api_core.exceptions``."""

    GoogleAPIError = _FakeGoogleAPIError
    PermissionDenied = _FakePermissionDenied
    NotFound = _FakeNotFound
    DeadlineExceeded = _FakeDeadlineExceeded


def _install_fake_sdk(monkeypatch: pytest.MonkeyPatch) -> _FakeSecretManagerModule:
    """Wire fake modules into the resolver's lazy-import seams.

    Returns the fake ``secretmanager`` module so tests can poke at
    ``last_client.payload_bytes`` / ``raise_on_call`` to script per-test
    behavior.
    """
    fake_sm = _FakeSecretManagerModule()
    fake_exc = _FakeExceptionsModule()
    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_gcp_secrets", lambda: fake_sm
    )
    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_google_exceptions", lambda: fake_exc
    )
    return fake_sm


# ---------------------------------------------------------------------------
# Shape: scheme, class attribute
# ---------------------------------------------------------------------------


def test_scheme_is_locked_to_canonical_spelling() -> None:
    """The scheme MUST be ``gcp-secret-manager`` — docs/08 §3."""
    assert GcpSecretManagerResolver.scheme == "gcp-secret-manager"


# ---------------------------------------------------------------------------
# Resolution — happy path
# ---------------------------------------------------------------------------


def test_resolves_valid_path_to_decoded_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    value = resolver.resolve(
        "projects/p/secrets/s/versions/latest", None
    )
    assert value == "FAKE-CREDENTIAL-VALUE"
    # Verify the SDK was called with the verbatim resource name.
    assert fake_sm.last_client is not None
    assert fake_sm.last_client.calls == [
        "projects/p/secrets/s/versions/latest"
    ]


def test_resolves_via_full_secret_url_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end through :func:`resolve_secret_url` after manual registration.

    We register the resolver by hand here instead of relying on the
    entry-point so this test stays isolated from the install state.
    """
    from dtex.secrets import register_secret_resolver

    _install_fake_sdk(monkeypatch)
    register_secret_resolver("gcp-secret-manager", GcpSecretManagerResolver)
    value = resolve_secret_url(
        "secret://gcp-secret-manager/projects/p/secrets/s/versions/1"
    )
    assert value == "FAKE-CREDENTIAL-VALUE"


def test_payload_decoded_as_utf8(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-ASCII payload (a UTF-8 byte sequence) decodes correctly."""
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    # First build the client by calling resolve once; then poke the payload.
    # Cleaner: build the client manually via the lazy-import shim.
    resolver._build_client()
    assert fake_sm.last_client is not None
    fake_sm.last_client.payload_bytes = "пароль-с-юникодом".encode()
    value = resolver.resolve("projects/p/secrets/s/versions/latest", None)
    assert value == "пароль-с-юникодом"


def test_payload_invalid_utf8_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    resolver._build_client()
    assert fake_sm.last_client is not None
    # 0xff is invalid as a UTF-8 start byte.
    fake_sm.last_client.payload_bytes = b"\xff\xfe\xfd"
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("projects/p/secrets/s/versions/latest", None)
    assert "UTF-8" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, UnicodeDecodeError)


# ---------------------------------------------------------------------------
# Path validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_path",
    [
        "",
        "projects/p/secrets/",
        "projects/p/secrets/s",
        "projects//secrets/s/versions/latest",
        "projects/p/secrets//versions/latest",
        "projects/p/secrets/s/versions/",
        "projects/p/secrets/s/something/latest",
        "not-a-resource-name",
        "secrets/s/versions/latest",  # missing projects/
    ],
)
def test_malformed_path_raises_with_clear_message(
    bad_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Install fakes so a real SDK isn't required even though the path
    # check fires before SDK import.
    _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve(bad_path, None)
    msg = str(exc_info.value)
    assert "malformed" in msg
    # Honor docs/08 §3: the error names the expected shape.
    assert "projects/" in msg
    assert "versions/" in msg


def test_path_check_fires_before_sdk_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad path MUST NOT trigger any SDK import — fast-fail on typos."""
    # Install a tripwire that fails the test if the lazy import runs.
    def _explode() -> Any:
        raise AssertionError("SDK must not be imported for a malformed path")

    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_gcp_secrets", _explode
    )
    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_google_exceptions", _explode
    )
    resolver = GcpSecretManagerResolver()
    with pytest.raises(SecretResolutionError):
        resolver.resolve("not-a-resource-name", None)


# ---------------------------------------------------------------------------
# SDK error mapping
# ---------------------------------------------------------------------------


def test_permission_denied_wrapped_with_clear_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    resolver._build_client()
    assert fake_sm.last_client is not None
    fake_sm.last_client.raise_on_call = _FakePermissionDenied("403 from server")
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("projects/p/secrets/s/versions/latest", None)
    msg = str(exc_info.value)
    assert "permission denied" in msg.lower()
    assert "projects/p/secrets/s/versions/latest" in msg
    # The SDK message text MUST NOT inline — only the resource name does.
    assert "403 from server" not in msg
    # Chained for tracebacks.
    assert isinstance(exc_info.value.__cause__, _FakePermissionDenied)


def test_not_found_wrapped_with_clear_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    resolver._build_client()
    assert fake_sm.last_client is not None
    fake_sm.last_client.raise_on_call = _FakeNotFound("404 from server")
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("projects/p/secrets/s/versions/latest", None)
    msg = str(exc_info.value)
    assert "not found" in msg.lower()
    assert "projects/p/secrets/s/versions/latest" in msg
    assert isinstance(exc_info.value.__cause__, _FakeNotFound)


def test_generic_google_api_error_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-Permission/NotFound GoogleAPIError surfaces with class name."""
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    resolver._build_client()
    assert fake_sm.last_client is not None
    fake_sm.last_client.raise_on_call = _FakeDeadlineExceeded("504 timeout")
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("projects/p/secrets/s/versions/latest", None)
    msg = str(exc_info.value)
    assert "_FakeDeadlineExceeded" in msg
    # SDK message NOT inlined.
    assert "504 timeout" not in msg
    assert isinstance(exc_info.value.__cause__, _FakeDeadlineExceeded)


# ---------------------------------------------------------------------------
# Lazy instantiation
# ---------------------------------------------------------------------------


def test_init_does_not_import_or_build_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``__init__`` MUST NOT import the SDK or construct a client.

    Lazy-instantiation contract: the resolver is built by stage 9a's
    ``_get_resolver`` on first scheme reference, but the SDK is built on
    first ``.resolve()`` call inside the resolver.
    """
    # Tripwire: any lazy-import call during __init__ fails the test.
    def _explode() -> Any:
        raise AssertionError("SDK must not be imported during __init__")

    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_gcp_secrets", _explode
    )
    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_google_exceptions", _explode
    )
    resolver = GcpSecretManagerResolver()
    # No exception means the contract holds.
    assert resolver._client is None


def test_client_built_once_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SDK client is constructed on first ``.resolve()`` only — once
    cached, subsequent calls reuse it.
    """
    fake_sm = _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()
    # No build yet.
    assert _FakeClient.instances_built == 0
    resolver.resolve("projects/p/secrets/s/versions/latest", None)
    assert _FakeClient.instances_built == 1
    resolver.resolve("projects/p/secrets/s/versions/1", None)
    resolver.resolve("projects/q/secrets/t/versions/latest", None)
    # Still one — the client is reused across resolves.
    assert _FakeClient.instances_built == 1
    # Sanity: the same fake module's last_client is the one used throughout.
    assert fake_sm.last_client is not None
    assert len(fake_sm.last_client.calls) == 3


# ---------------------------------------------------------------------------
# #field ignored with a one-time warning
# ---------------------------------------------------------------------------


def test_field_is_ignored_with_one_warning_per_pair(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """``#field`` is dropped (with a single warning per unique pair), and
    resolution still returns the full blob."""
    _install_fake_sdk(monkeypatch)
    resolver = GcpSecretManagerResolver()

    path = "projects/p/secrets/s/versions/latest"
    with caplog.at_level(logging.WARNING, logger="dtex.secrets"):
        v1 = resolver.resolve(path, "token")
        v2 = resolver.resolve(path, "token")  # same pair — no second warning
        v3 = resolver.resolve(path, "other")  # different field — new warning
    # All three calls succeed and return the full blob.
    assert v1 == v2 == "FAKE-CREDENTIAL-VALUE"
    assert v3 == "FAKE-CREDENTIAL-VALUE"
    # Two distinct (path, field) pairs → exactly two warning records.
    warnings = [
        r for r in caplog.records
        if r.name == "dtex.secrets" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 2
    # The warning names the field that was ignored.
    msgs = " ".join(w.getMessage() for w in warnings)
    assert "token" in msgs
    assert "other" in msgs


# ---------------------------------------------------------------------------
# Missing SDK → clear ImportError
# ---------------------------------------------------------------------------


def test_missing_sdk_raises_import_error_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lazy-import shim raises a clean :class:`ImportError` naming the
    extra when the SDK is absent."""
    # Simulate the SDK being missing — re-monkeypatch the canonical
    # lazy import to call the original logic with a tripwire that
    # forces an ImportError. Cleaner: just call the real
    # ``_lazy_import_gcp_secrets`` after monkeypatching the
    # ``google.cloud`` import path. Since we can't easily unimport,
    # we substitute the shim with one that mirrors the failure mode.
    def _missing() -> Any:
        raise ImportError(
            "the GCP Secret Manager resolver needs `google-cloud-secret-manager`; "
            "install with `pip install dtex[gcp-secrets]`"
        )

    monkeypatch.setattr(
        gcp_resolver_mod, "_lazy_import_gcp_secrets", _missing
    )
    resolver = GcpSecretManagerResolver()
    with pytest.raises(ImportError) as exc_info:
        resolver.resolve("projects/p/secrets/s/versions/latest", None)
    assert "pip install dtex[gcp-secrets]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Entry-point registration — picked up via importlib.metadata
# ---------------------------------------------------------------------------


def test_entry_point_registration_populates_registry() -> None:
    """When ``dtex`` is installed (editable or otherwise) with stage 9b's
    ``pyproject.toml`` block, ``importlib.metadata.entry_points`` exposes
    ``gcp-secret-manager`` under the ``dtex.secret_resolvers`` group, and
    stage 9a's :func:`_load_entry_points` walks it into the registry.
    """
    from dtex.secrets.resolvers import _RESOLVERS, _load_entry_points

    # The autouse fixture has already reset everything; nothing is loaded yet.
    assert "gcp-secret-manager" not in _RESOLVERS
    _load_entry_points()
    # The class itself is what the entry-point points at, so the
    # factory IS the class. The registry stores the callable.
    assert "gcp-secret-manager" in _RESOLVERS
    factory = _RESOLVERS["gcp-secret-manager"]
    assert factory is GcpSecretManagerResolver


# ---------------------------------------------------------------------------
# Integration test (live GCP) — gated by env vars + the integration marker
# ---------------------------------------------------------------------------


_INTEGRATION_ENV_VARS = (
    "DET_GCP_SECRETS_TEST_PROJECT",
    "DET_GCP_SECRETS_TEST_SECRET",
)


def _have_live_creds() -> bool:
    return all(os.getenv(v) for v in _INTEGRATION_ENV_VARS)


@pytest.mark.integration
@pytest.mark.skipif(
    not _have_live_creds(),
    reason=(
        "needs live GCP Secret Manager "
        "(set DET_GCP_SECRETS_TEST_PROJECT + DET_GCP_SECRETS_TEST_SECRET)"
    ),
)
def test_integration_resolves_against_live_secret_manager() -> None:
    """End-to-end: resolve a real GCP secret via ADC.

    Only runs when ``DET_GCP_SECRETS_TEST_PROJECT`` +
    ``DET_GCP_SECRETS_TEST_SECRET`` env vars are set AND the runner's
    ADC has ``roles/secretmanager.secretAccessor`` on the secret.

    NEVER logs or asserts against the actual secret value — that would
    defeat the purpose of having a secret. Asserts only that resolution
    succeeded and returned a non-empty string.
    """
    project = os.environ["DET_GCP_SECRETS_TEST_PROJECT"]
    secret = os.environ["DET_GCP_SECRETS_TEST_SECRET"]
    path = f"projects/{project}/secrets/{secret}/versions/latest"

    resolver = GcpSecretManagerResolver()
    value = resolver.resolve(path, None)
    assert isinstance(value, str)
    assert value  # non-empty — we don't assert the value itself
