"""Tests for :class:`detx.secrets._aws.AwsSecretsManagerResolver` — stage 9c.

Two paths:

* **Unit tests** (always run): substitute a fake ``boto3`` module via
  ``monkeypatch.setattr(detx.secrets._aws, "_lazy_import_boto3", ...)``
  and a fake ``botocore.exceptions`` module via the same lever on
  ``_lazy_import_botocore_exceptions``. The fakes record calls and
  serve canned responses; no network, no live AWS.
* **Integration test** (gated): exercises the resolver against a real
  AWS Secrets Manager secret, enabled when
  ``DET_AWS_SECRETS_TEST_REGION`` + ``DET_AWS_SECRETS_TEST_SECRET_ID``
  are set. Marked ``@pytest.mark.integration`` so the default
  ``pytest`` run skips it.

To set up the integration test (one-time, on an AWS account the
runner's credential chain can access)::

    aws secretsmanager create-secret \\
        --name detx-it-secret \\
        --secret-string 'hello-from-detx-test' \\
        --region us-east-1
    # then in the shell that runs pytest:
    export DET_AWS_SECRETS_TEST_REGION=us-east-1
    export DET_AWS_SECRETS_TEST_SECRET_ID=detx-it-secret
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any

import pytest

from detx.secrets import (
    SecretResolutionError,
    _reset_resolvers_for_testing,
    resolve_secret_url,
)
from detx.secrets import _aws as aws_resolver_mod
from detx.secrets._aws import AwsSecretsManagerResolver


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


class _FakeClient:
    """Stand-in for ``boto3.client("secretsmanager", region_name=...)``.

    Records every ``get_secret_value`` call so tests can assert
    SecretId / VersionStage passing. Behavior is configurable
    per-instance: a canned ``SecretString`` payload by default, or an
    exception to raise.
    """

    def __init__(self, region_name: str) -> None:
        self.region_name = region_name
        self.calls: list[dict[str, str]] = []
        # Default behavior: serve a canned SecretString.
        self.response_payload: dict[str, Any] = {
            "SecretString": "FAKE-CREDENTIAL-VALUE",
        }
        self.raise_on_call: BaseException | None = None

    def get_secret_value(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(dict(kwargs))
        if self.raise_on_call is not None:
            exc = self.raise_on_call
            self.raise_on_call = None
            raise exc
        return self.response_payload


class _FakeBoto3Module:
    """Stand-in for the ``boto3`` namespace."""

    def __init__(self) -> None:
        # Per-region client cache the fake exposes for assertions.
        self.clients_by_region: dict[str, _FakeClient] = {}
        # Construction counter — increments on every ``boto3.client(...)`` call.
        self.construct_count = 0
        # Override that lets a test inject a pre-built client for a
        # given region (used by tests that need to set up
        # ``response_payload`` / ``raise_on_call`` BEFORE the resolver
        # builds the client itself).
        self.pre_built: dict[str, _FakeClient] = {}

    def client(self, service_name: str, region_name: str) -> _FakeClient:
        assert service_name == "secretsmanager"
        self.construct_count += 1
        if region_name in self.pre_built:
            client = self.pre_built[region_name]
        else:
            client = _FakeClient(region_name)
        self.clients_by_region[region_name] = client
        return client


# Fake botocore.exceptions — only ``ClientError`` need exist. AWS's real
# ClientError carries a ``response`` dict; we mirror that shape.


class _FakeClientError(Exception):
    """Stand-in for :class:`botocore.exceptions.ClientError`."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message)
        self.response: dict[str, Any] = {"Error": {"Code": code, "Message": message}}


class _FakeBotocoreExceptionsModule:
    """Stand-in for ``botocore.exceptions``."""

    ClientError = _FakeClientError


def _install_fake_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> _FakeBoto3Module:
    """Wire fake modules into the resolver's lazy-import seams.

    Returns the fake ``boto3`` module so tests can poke at
    ``clients_by_region[...]``'s ``response_payload`` / ``raise_on_call``
    to script per-test behavior.
    """
    fake_boto3 = _FakeBoto3Module()
    fake_exc = _FakeBotocoreExceptionsModule()
    monkeypatch.setattr(
        aws_resolver_mod, "_lazy_import_boto3", lambda: fake_boto3
    )
    monkeypatch.setattr(
        aws_resolver_mod,
        "_lazy_import_botocore_exceptions",
        lambda: fake_exc,
    )
    return fake_boto3


# ---------------------------------------------------------------------------
# Shape: scheme, class attribute
# ---------------------------------------------------------------------------


def test_scheme_is_locked_to_canonical_spelling() -> None:
    """The scheme MUST be ``aws-secrets-manager`` — docs/08 §3."""
    assert AwsSecretsManagerResolver.scheme == "aws-secrets-manager"


# ---------------------------------------------------------------------------
# Path parsing
# ---------------------------------------------------------------------------


def test_parses_region_and_secret_id_with_default_version_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    value = resolver.resolve("us-east-1/mysecret", None)
    assert value == "FAKE-CREDENTIAL-VALUE"
    client = fake.clients_by_region["us-east-1"]
    assert client.calls == [
        {"SecretId": "mysecret", "VersionStage": "AWSCURRENT"}
    ]


def test_parses_explicit_version_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    resolver.resolve("eu-west-2/mysecret:AWSPENDING", None)
    client = fake.clients_by_region["eu-west-2"]
    assert client.calls == [
        {"SecretId": "mysecret", "VersionStage": "AWSPENDING"}
    ]


def test_secret_id_can_contain_slashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AWS console uses ``/`` for hierarchical secret names — only the
    FIRST ``/`` is the region/secret separator."""
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    resolver.resolve("us-east-1/team/db-creds", None)
    client = fake.clients_by_region["us-east-1"]
    assert client.calls == [
        {"SecretId": "team/db-creds", "VersionStage": "AWSCURRENT"}
    ]


def test_secret_id_can_be_an_arn(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ARN contains ``:`` — must NOT be split as version-stage. The
    parser discriminates on the literal ``arn:`` prefix so the whole
    ARN passes to AWS verbatim regardless of its trailing tokens."""
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf"
    resolver.resolve(f"us-east-1/{arn}", None)
    client = fake.clients_by_region["us-east-1"]
    assert client.calls == [
        {"SecretId": arn, "VersionStage": "AWSCURRENT"}
    ]


def test_custom_staging_label_recognized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AWS permits operator-defined staging labels in any case —
    ``us-east-1/mysecret:rollout-canary`` should split correctly.
    """
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    resolver.resolve("us-east-1/mysecret:rollout-canary", None)
    client = fake.clients_by_region["us-east-1"]
    assert client.calls == [
        {"SecretId": "mysecret", "VersionStage": "rollout-canary"}
    ]


@pytest.mark.parametrize(
    "bad_path",
    [
        "",
        "no-slash-here",
        "/mysecret",  # empty region
        "us-east-1/",  # empty secret-id
        "US-EAST-1/mysecret",  # invalid region (uppercase)
        "us_east_1/mysecret",  # invalid region (underscore)
    ],
)
def test_malformed_path_raises_with_clear_message(
    bad_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve(bad_path, None)
    msg = str(exc_info.value)
    assert "<region>/<secret-id>" in msg or "region" in msg.lower()


def test_path_check_fires_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bad path MUST NOT trigger any SDK import — fast-fail on typos."""

    def _explode() -> Any:
        raise AssertionError("SDK must not be imported for a malformed path")

    monkeypatch.setattr(aws_resolver_mod, "_lazy_import_boto3", _explode)
    monkeypatch.setattr(
        aws_resolver_mod, "_lazy_import_botocore_exceptions", _explode
    )
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError):
        resolver.resolve("no-slash", None)


# ---------------------------------------------------------------------------
# Happy-path resolution + end-to-end via resolve_secret_url
# ---------------------------------------------------------------------------


def test_resolves_via_full_secret_url_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end through :func:`resolve_secret_url` after manual registration."""
    from detx.secrets import register_secret_resolver

    _install_fake_sdk(monkeypatch)
    register_secret_resolver(
        "aws-secrets-manager", AwsSecretsManagerResolver
    )
    value = resolve_secret_url(
        "secret://aws-secrets-manager/us-east-1/mysecret"
    )
    assert value == "FAKE-CREDENTIAL-VALUE"


# ---------------------------------------------------------------------------
# JSON field extraction
# ---------------------------------------------------------------------------


def test_json_field_extraction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {
        "SecretString": json.dumps(
            {"username": "x", "password": "y"}
        )
    }
    resolver = AwsSecretsManagerResolver()
    assert (
        resolver.resolve("us-east-1/db", "password") == "y"
    )
    assert (
        resolver.resolve("us-east-1/db", "username") == "x"
    )


def test_json_field_missing_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {
        "SecretString": json.dumps({"username": "x"})
    }
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/db", "password")
    msg = str(exc_info.value)
    assert "password" in msg
    assert "not in JSON" in msg


def test_non_json_secret_with_field_requested_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asking for ``#field`` on a non-JSON SecretString is a clear error."""
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {"SecretString": "not-json"}
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/db", "password")
    msg = str(exc_info.value)
    assert "not JSON" in msg
    assert "password" in msg
    # The chained cause is the JSONDecodeError.
    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


def test_json_non_object_with_field_requested_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JSON arrays / scalars at the top level cannot be field-indexed."""
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {"SecretString": "[1, 2, 3]"}
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/db", "password")
    assert "not a JSON object" in str(exc_info.value)


def test_json_field_non_string_value_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nested object under a field is not a usable credential."""
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {
        "SecretString": json.dumps({"nested": {"k": "v"}})
    }
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/db", "nested")
    assert "is not a string" in str(exc_info.value)


# ---------------------------------------------------------------------------
# SDK error mapping
# ---------------------------------------------------------------------------


def test_resource_not_found_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.raise_on_call = _FakeClientError(
        "ResourceNotFoundException", "secret not found server-side"
    )
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/mysecret", None)
    msg = str(exc_info.value)
    assert "not found" in msg.lower()
    assert "mysecret" in msg
    assert "us-east-1" in msg
    # SDK message body MUST NOT inline.
    assert "secret not found server-side" not in msg
    assert isinstance(exc_info.value.__cause__, _FakeClientError)


def test_access_denied_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.raise_on_call = _FakeClientError(
        "AccessDeniedException", "403 from server"
    )
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/mysecret", None)
    msg = str(exc_info.value)
    assert "permission denied" in msg.lower()
    assert "mysecret" in msg
    assert "403 from server" not in msg


def test_generic_client_error_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-NotFound/non-AccessDenied ClientError surfaces with class name."""
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.raise_on_call = _FakeClientError(
        "ThrottlingException", "rate-limited server-side"
    )
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/mysecret", None)
    msg = str(exc_info.value)
    assert "_FakeClientError" in msg
    # SDK message body MUST NOT inline.
    assert "rate-limited server-side" not in msg
    assert isinstance(exc_info.value.__cause__, _FakeClientError)


def test_binary_secret_not_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    fake.pre_built["us-east-1"] = client = _FakeClient("us-east-1")
    client.response_payload = {"SecretBinary": b"\x00\x01\x02"}
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(SecretResolutionError) as exc_info:
        resolver.resolve("us-east-1/mysecret", None)
    assert "binary secrets not supported" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Per-region client cache
# ---------------------------------------------------------------------------


def test_client_built_once_per_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install_fake_sdk(monkeypatch)
    resolver = AwsSecretsManagerResolver()
    assert fake.construct_count == 0
    resolver.resolve("us-east-1/a", None)
    assert fake.construct_count == 1
    # Same region — reuses.
    resolver.resolve("us-east-1/b", None)
    assert fake.construct_count == 1
    # Different region — new client.
    resolver.resolve("eu-west-2/c", None)
    assert fake.construct_count == 2
    # Both regions accumulated calls on their respective clients.
    assert len(fake.clients_by_region["us-east-1"].calls) == 2
    assert len(fake.clients_by_region["eu-west-2"].calls) == 1


# ---------------------------------------------------------------------------
# Lazy instantiation
# ---------------------------------------------------------------------------


def test_init_does_not_import_or_build_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _explode() -> Any:
        raise AssertionError("SDK must not be imported during __init__")

    monkeypatch.setattr(aws_resolver_mod, "_lazy_import_boto3", _explode)
    monkeypatch.setattr(
        aws_resolver_mod, "_lazy_import_botocore_exceptions", _explode
    )
    resolver = AwsSecretsManagerResolver()
    assert resolver._clients == {}


# ---------------------------------------------------------------------------
# Missing SDK → clear ImportError
# ---------------------------------------------------------------------------


def test_missing_sdk_raises_import_error_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lazy-import shim raises a clean :class:`ImportError` naming
    the extra when the SDK is absent."""

    def _missing() -> Any:
        raise ImportError(
            "the AWS Secrets Manager resolver needs `boto3`; "
            "install with `pip install detx[aws-secrets]`"
        )

    monkeypatch.setattr(aws_resolver_mod, "_lazy_import_boto3", _missing)
    resolver = AwsSecretsManagerResolver()
    with pytest.raises(ImportError) as exc_info:
        resolver.resolve("us-east-1/mysecret", None)
    assert "pip install detx[aws-secrets]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Entry-point registration — picked up via importlib.metadata
# ---------------------------------------------------------------------------


def test_entry_point_registration_populates_registry() -> None:
    """``detx`` installed with stage 9c's ``pyproject.toml`` block exposes
    ``aws-secrets-manager`` under the ``detx.secret_resolvers`` group.
    """
    from detx.secrets.resolvers import _RESOLVERS, _load_entry_points

    assert "aws-secrets-manager" not in _RESOLVERS
    _load_entry_points()
    assert "aws-secrets-manager" in _RESOLVERS
    factory = _RESOLVERS["aws-secrets-manager"]
    assert factory is AwsSecretsManagerResolver


# ---------------------------------------------------------------------------
# Integration test (live AWS) — gated by env vars + the integration marker
# ---------------------------------------------------------------------------


_INTEGRATION_ENV_VARS = (
    "DET_AWS_SECRETS_TEST_REGION",
    "DET_AWS_SECRETS_TEST_SECRET_ID",
)


def _have_live_creds() -> bool:
    return all(os.getenv(v) for v in _INTEGRATION_ENV_VARS)


@pytest.mark.integration
@pytest.mark.skipif(
    not _have_live_creds(),
    reason=(
        "needs live AWS Secrets Manager "
        "(set DET_AWS_SECRETS_TEST_REGION + DET_AWS_SECRETS_TEST_SECRET_ID)"
    ),
)
def test_integration_resolves_against_live_secrets_manager() -> None:
    """End-to-end: resolve a real AWS secret via boto3's standard creds chain.

    Only runs when ``DET_AWS_SECRETS_TEST_REGION`` +
    ``DET_AWS_SECRETS_TEST_SECRET_ID`` env vars are set AND the
    runner's IAM principal has ``secretsmanager:GetSecretValue`` on
    the secret.

    NEVER logs or asserts against the actual secret value — that
    would defeat the purpose of having a secret. Asserts only that
    resolution succeeded and returned a non-empty string.
    """
    region = os.environ["DET_AWS_SECRETS_TEST_REGION"]
    secret_id = os.environ["DET_AWS_SECRETS_TEST_SECRET_ID"]
    path = f"{region}/{secret_id}"

    resolver = AwsSecretsManagerResolver()
    value = resolver.resolve(path, None)
    assert isinstance(value, str)
    assert value  # non-empty — we don't assert the value itself
