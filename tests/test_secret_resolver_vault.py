"""Tests for :class:`dtex.secrets._vault.VaultResolver` — stage 9c.

Two paths:

* **Unit tests** (always run): substitute a fake ``hvac`` module via
  ``monkeypatch.setattr(dtex.secrets._vault, "_lazy_import_hvac", ...)``
  and a fake ``hvac.exceptions`` module via the same lever on
  ``_lazy_import_hvac_exceptions``. The fakes record calls and serve
  canned responses; no network, no live Vault.
* **Integration test** (gated): exercises the resolver against a real
  Vault deployment, enabled when ``VAULT_ADDR`` + ``VAULT_TOKEN`` +
  ``DET_VAULT_TEST_PATH`` + ``DET_VAULT_TEST_FIELD`` are set. Marked
  ``@pytest.mark.integration`` so the default ``pytest`` run skips it.

To set up the integration test (one-time, on a Vault dev server)::

    vault kv put secret/dtex-it-secret det_test_field=hello-from-dtex-test
    # then in the shell that runs pytest:
    export VAULT_ADDR=http://127.0.0.1:8200
    export VAULT_TOKEN=<your-token>
    export DET_VAULT_TEST_PATH=secret/data/dtex-it-secret    # KV v2 path
    export DET_VAULT_TEST_FIELD=det_test_field
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

from dtex.secrets import (
    SecretResolutionError,
    _reset_resolvers_for_testing,
    resolve_secret_url,
)
from dtex.secrets import _vault as vault_resolver_mod
from dtex.secrets._vault import VaultResolver


@pytest.fixture(autouse=True)
def _clean_resolvers() -> Iterator[None]:
    """Wipe the module-level registry before AND after every test.

    Mirrors the autouse fixture in :mod:`tests.test_secret_resolver_gcp`.
    """
    _reset_resolvers_for_testing()
    yield
    _reset_resolvers_for_testing()


# ---------------------------------------------------------------------------
# Fake SDK — substituted via monkeypatch.setattr on the lazy-import accessors
# ---------------------------------------------------------------------------


class _FakeHvacClient:
    """Stand-in for ``hvac.Client(url=..., token=...)``.

    Records every ``read`` call so tests can assert path-passing.
    Behavior is configurable per-instance: a canned response by
    default, or an exception to raise.
    """

    def __init__(self, url: str, token: str) -> None:
        self.url = url
        self.token = token
        self.calls: list[str] = []
        # Default response: a KV v2 shape with one ``password`` field.
        self.response: Any = {
            "data": {
                "data": {"password": "FAKE-CREDENTIAL-VALUE"},
                "metadata": {"version": 1},
            }
        }
        self.raise_on_call: BaseException | None = None

    def read(self, path: str) -> Any:
        self.calls.append(path)
        if self.raise_on_call is not None:
            exc = self.raise_on_call
            self.raise_on_call = None
            raise exc
        return self.response


_UNSET = object()


class _FakeHvacModule:
    """Stand-in for the ``hvac`` namespace."""

    def __init__(self) -> None:
        self.construct_count = 0
        self.last_client: _FakeHvacClient | None = None
        # Tests can pre-populate these to control the next-constructed
        # client's behavior. ``_UNSET`` is the "no override" sentinel —
        # using ``None`` directly is meaningful (it's the hvac return
        # for missing-path / forbidden), so we can't use ``None`` as
        # the "no override" marker.
        self.next_response: Any = _UNSET
        self.next_raise: BaseException | None = None

    def Client(  # noqa: N802 — matches hvac.Client class name
        self, url: str, token: str
    ) -> _FakeHvacClient:
        self.construct_count += 1
        client = _FakeHvacClient(url=url, token=token)
        if self.next_response is not _UNSET:
            client.response = self.next_response
        if self.next_raise is not None:
            client.raise_on_call = self.next_raise
        self.last_client = client
        return client


# Fake hvac.exceptions — only ``VaultError`` and a subclass need exist.


class _FakeVaultError(Exception):
    """Stand-in for :class:`hvac.exceptions.VaultError`."""


class _FakeForbidden(_FakeVaultError):
    """Stand-in for :class:`hvac.exceptions.Forbidden`."""


class _FakeHvacExceptionsModule:
    """Stand-in for ``hvac.exceptions``."""

    VaultError = _FakeVaultError
    Forbidden = _FakeForbidden


def _install_fake_sdk(monkeypatch: pytest.MonkeyPatch) -> _FakeHvacModule:
    """Wire fake modules into the resolver's lazy-import seams."""
    fake_hvac = _FakeHvacModule()
    fake_exc = _FakeHvacExceptionsModule()
    monkeypatch.setattr(
        vault_resolver_mod, "_lazy_import_hvac", lambda: fake_hvac
    )
    monkeypatch.setattr(
        vault_resolver_mod, "_lazy_import_hvac_exceptions", lambda: fake_exc
    )
    return fake_hvac


def _set_env(
    monkeypatch: pytest.MonkeyPatch,
    addr: str = "http://127.0.0.1:8200",
    token: str = "fake-token",
) -> None:
    monkeypatch.setenv("VAULT_ADDR", addr)
    monkeypatch.setenv("VAULT_TOKEN", token)


# ---------------------------------------------------------------------------
# Shape: scheme, class attribute
# ---------------------------------------------------------------------------


def test_scheme_is_locked_to_canonical_spelling() -> None:
    """The scheme MUST be ``vault`` — docs/08 §3."""
    assert VaultResolver.scheme == "vault"


# ---------------------------------------------------------------------------
# KV v1 / KV v2 response-shape parsing
# ---------------------------------------------------------------------------


def test_resolves_kv_v2_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {
        "data": {
            "data": {"password": "v2-secret"},
            "metadata": {"version": 3},
        }
    }
    resolver = VaultResolver()
    assert resolver.resolve("secret/data/warehouse", "password") == "v2-secret"
    assert fake.last_client is not None
    assert fake.last_client.calls == ["secret/data/warehouse"]


def test_resolves_kv_v1_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {
        "data": {"password": "v1-secret"},
    }
    resolver = VaultResolver()
    assert resolver.resolve("secret/legacy/warehouse", "password") == "v1-secret"


def test_resolves_via_full_secret_url_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end through :func:`resolve_secret_url` after manual registration."""
    from dtex.secrets import register_secret_resolver

    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {
        "data": {"data": {"token": "via-url"}}
    }
    register_secret_resolver("vault", VaultResolver)
    value = resolve_secret_url("secret://vault/secret/data/app#token")
    assert value == "via-url"


# ---------------------------------------------------------------------------
# Field requirement
# ---------------------------------------------------------------------------


def test_missing_field_raises_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``#field`` is REQUIRED — the check fires BEFORE SDK import."""

    def _explode() -> Any:
        raise AssertionError("SDK must not be imported when #field missing")

    monkeypatch.setattr(vault_resolver_mod, "_lazy_import_hvac", _explode)
    monkeypatch.setattr(
        vault_resolver_mod, "_lazy_import_hvac_exceptions", _explode
    )
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/warehouse", None)
    msg = str(exc_info.value)
    assert "#field" in msg
    assert "secret/data/warehouse" in msg


def test_empty_path_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty path is a clear error — fires BEFORE SDK import."""

    def _explode() -> Any:
        raise AssertionError("SDK must not be imported on empty path")

    monkeypatch.setattr(vault_resolver_mod, "_lazy_import_hvac", _explode)
    monkeypatch.setattr(
        vault_resolver_mod, "_lazy_import_hvac_exceptions", _explode
    )
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("", "field")
    assert "path is empty" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Env var auth
# ---------------------------------------------------------------------------


def test_missing_vault_addr_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    monkeypatch.setenv("VAULT_TOKEN", "fake")
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/warehouse", "password")
    msg = str(exc_info.value)
    assert "VAULT_ADDR" in msg


def test_missing_vault_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    monkeypatch.setenv("VAULT_ADDR", "http://127.0.0.1:8200")
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/warehouse", "password")
    msg = str(exc_info.value)
    assert "VAULT_TOKEN" in msg


def test_client_built_with_env_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``VAULT_ADDR`` / ``VAULT_TOKEN`` flow into the constructed client."""
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch, addr="https://vault.example:8200", token="t-abc")
    resolver = VaultResolver()
    resolver.resolve("secret/data/x", "password")
    assert fake.last_client is not None
    assert fake.last_client.url == "https://vault.example:8200"
    assert fake.last_client.token == "t-abc"


# ---------------------------------------------------------------------------
# Missing data / missing field
# ---------------------------------------------------------------------------


def test_read_returns_none_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """``client.read`` returns None for missing path or insufficient perms."""
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = None
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/missing", "password")
    msg = str(exc_info.value)
    assert "no secret at path" in msg
    assert "secret/data/missing" in msg
    assert "token lacks read" in msg


def test_field_not_in_payload_kv_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {
        "data": {
            "data": {"username": "x"},
            "metadata": {"version": 1},
        }
    }
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/warehouse", "password")
    msg = str(exc_info.value)
    assert "password" in msg
    assert "not in secret" in msg


def test_field_not_in_payload_kv_v1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {"data": {"username": "x"}}
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/legacy/warehouse", "password")
    msg = str(exc_info.value)
    assert "password" in msg
    assert "not in secret" in msg


def test_non_string_field_value_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {
        "data": {"data": {"nested": {"k": "v"}}}
    }
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/x", "nested")
    msg = str(exc_info.value)
    assert "not a string" in msg
    assert "nested" in msg


def test_malformed_response_no_data_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_response = {"unexpected": "shape"}
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/x", "password")
    assert "unexpected response shape" in str(exc_info.value)


# ---------------------------------------------------------------------------
# SDK error mapping
# ---------------------------------------------------------------------------


def test_vault_error_surfaces_class_name_and_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vault SDK error class name AND message both surface.

    The hvac message body is intentionally INCLUDED — Vault errors echo
    the path (already known to the caller — it's in the `secret://` URL)
    and policy template names, but the secret VALUE is never in the
    message (the read failed before any value left Vault). The engine's
    per-run Redactor is the safety net for any value that did slip
    through. See ``_vault.py::resolve`` and the rationale comment on
    the catch-all hvac-exception branch.
    """
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    fake.next_raise = _FakeForbidden(
        "denied by policy 'secret-warehouse-policy'"
    )
    resolver = VaultResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("secret/data/warehouse", "password")
    msg = str(exc_info.value)
    assert "_FakeForbidden" in msg
    # Policy message IS inlined — operator-diagnostic, no value leak.
    assert "denied by policy" in msg
    assert isinstance(exc_info.value.__cause__, _FakeForbidden)


# ---------------------------------------------------------------------------
# Lazy instantiation
# ---------------------------------------------------------------------------


def test_init_does_not_import_or_build_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _explode() -> Any:
        raise AssertionError("SDK must not be imported during __init__")

    monkeypatch.setattr(vault_resolver_mod, "_lazy_import_hvac", _explode)
    monkeypatch.setattr(
        vault_resolver_mod, "_lazy_import_hvac_exceptions", _explode
    )
    resolver = VaultResolver()
    assert resolver._client is None


def test_client_built_once_and_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    _set_env(monkeypatch)
    resolver = VaultResolver()
    assert fake.construct_count == 0
    resolver.resolve("secret/data/a", "password")
    assert fake.construct_count == 1
    resolver.resolve("secret/data/b", "password")
    resolver.resolve("secret/data/c", "password")
    # Still one — the hvac client is reused.
    assert fake.construct_count == 1
    assert fake.last_client is not None
    assert len(fake.last_client.calls) == 3


# ---------------------------------------------------------------------------
# Missing SDK → clear ImportError
# ---------------------------------------------------------------------------


def test_missing_sdk_raises_import_error_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch)

    def _missing() -> Any:
        raise ImportError(
            "the Vault resolver needs `hvac`; "
            "install with `pip install dtex[vault]`"
        )

    monkeypatch.setattr(vault_resolver_mod, "_lazy_import_hvac", _missing)
    resolver = VaultResolver()
    with pytest.raises(ImportError) as exc_info:
        resolver.resolve("secret/data/warehouse", "password")
    assert "pip install dtex[vault]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Entry-point registration
# ---------------------------------------------------------------------------


def test_entry_point_registration_populates_registry() -> None:
    """``dtex`` installed with stage 9c's ``pyproject.toml`` block exposes
    ``vault`` under the ``dtex.secret_resolvers`` group."""
    from dtex.secrets.resolvers import _RESOLVERS, _load_entry_points

    assert "vault" not in _RESOLVERS
    _load_entry_points()
    assert "vault" in _RESOLVERS
    factory = _RESOLVERS["vault"]
    assert factory is VaultResolver


# ---------------------------------------------------------------------------
# Integration test (live Vault) — gated by env vars + the integration marker
# ---------------------------------------------------------------------------


_INTEGRATION_ENV_VARS = (
    "VAULT_ADDR",
    "VAULT_TOKEN",
    "DET_VAULT_TEST_PATH",
    "DET_VAULT_TEST_FIELD",
)


def _have_live_creds() -> bool:
    return all(os.getenv(v) for v in _INTEGRATION_ENV_VARS)


@pytest.mark.integration
@pytest.mark.skipif(
    not _have_live_creds(),
    reason=(
        "needs live Vault "
        "(set VAULT_ADDR + VAULT_TOKEN + DET_VAULT_TEST_PATH + "
        "DET_VAULT_TEST_FIELD)"
    ),
)
def test_integration_resolves_against_live_vault() -> None:
    """End-to-end: resolve a real Vault secret via env-var auth.

    Only runs when all four env vars are set AND the token has read
    capability on the secret path.

    NEVER logs or asserts against the actual secret value — asserts
    only that resolution succeeded and returned a non-empty string.
    """
    path = os.environ["DET_VAULT_TEST_PATH"]
    field = os.environ["DET_VAULT_TEST_FIELD"]

    resolver = VaultResolver()
    value = resolver.resolve(path, field)
    assert isinstance(value, str)
    assert value
