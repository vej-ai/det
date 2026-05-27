"""Tests for the ``dtex.secrets`` package — stage 9a.

Covers the :class:`SecretResolver` Protocol, the URL parser, the
project-local + entry-points discovery mechanisms, and the Redactor
integration that masks resolved values in log lines. No live external
services — everything is fake / monkeypatched.

The autouse ``_clean_resolvers`` fixture resets the module-level
registry before AND after each test so a leaked registration in one test
cannot affect another.
"""

from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import Iterator
from io import StringIO
from pathlib import Path
from typing import ClassVar

import pytest

import dtex
from dtex.engine.logger import Redactor, build_logger
from dtex.secrets import (
    SecretResolutionError,
    SecretResolver,
    _reset_resolvers_for_testing,
    is_secret_url,
    load_project_plugins,
    register_secret_resolver,
    resolve_secret_url,
)


@pytest.fixture(autouse=True)
def _clean_resolvers() -> Iterator[None]:
    """Wipe the module-level registry before and after every test."""
    _reset_resolvers_for_testing()
    yield
    _reset_resolvers_for_testing()


# ---------------------------------------------------------------------------
# Fake resolvers used across multiple tests
# ---------------------------------------------------------------------------


class FakeResolver:
    """Returns ``RESOLVED:<path>`` (no field) or ``RESOLVED:<path>#<field>``.

    Long enough to clear the Redactor's :data:`_MIN_REDACT_LEN` (4) floor
    so a redaction-integration test actually observes masking.
    """

    scheme: ClassVar[str] = "fake"

    def resolve(self, path: str, field: str | None) -> str:
        if field is None:
            return f"RESOLVED-VALUE-{path}"
        return f"RESOLVED-VALUE-{path}#{field}"


# ---------------------------------------------------------------------------
# is_secret_url
# ---------------------------------------------------------------------------


def test_is_secret_url_accepts_valid_prefix() -> None:
    assert is_secret_url("secret://gcp/foo")
    assert is_secret_url("secret://vault/x/y#token")


def test_is_secret_url_rejects_non_strings_and_empty() -> None:
    assert not is_secret_url(None)
    assert not is_secret_url(42)
    assert not is_secret_url("")
    assert not is_secret_url("${env.X}")
    assert not is_secret_url("plain-string")


def test_is_secret_url_case_sensitive_on_prefix() -> None:
    """The prefix is fixed lowercase per docs/08 §3."""
    assert not is_secret_url("Secret://gcp/foo")
    assert not is_secret_url("SECRET://gcp/foo")


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


def test_parse_simple_url_no_field() -> None:
    register_secret_resolver("gcp", FakeResolver)
    value = resolve_secret_url("secret://gcp/projects/x/secrets/y")
    # FakeResolver echoes the path; assert it preserved nested slashes.
    assert value == "RESOLVED-VALUE-projects/x/secrets/y"


def test_parse_url_with_field() -> None:
    register_secret_resolver("vault", FakeResolver)
    value = resolve_secret_url("secret://vault/x/y#token")
    assert value == "RESOLVED-VALUE-x/y#token"


def test_parse_url_field_preserves_dots() -> None:
    """A ``#deep.nested.field`` round-trips verbatim — urlsplit puts the
    whole fragment in ``.fragment``, dots and all."""
    register_secret_resolver("my-scheme", FakeResolver)
    value = resolve_secret_url("secret://my-scheme/a/b/c#deep.nested.field")
    assert value == "RESOLVED-VALUE-a/b/c#deep.nested.field"


def test_parse_url_lowercases_scheme() -> None:
    """``secret://GCP/...`` routes to the resolver registered as ``gcp``."""
    register_secret_resolver("gcp", FakeResolver)
    value = resolve_secret_url("secret://GCP/anything")
    assert value == "RESOLVED-VALUE-anything"


def test_parse_url_empty_scheme_rejected() -> None:
    """``secret:///foo`` — no scheme between ``//`` and ``/foo``."""
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret:///foo")
    assert "scheme" in str(exc_info.value).lower()


def test_parse_url_empty_path_rejected() -> None:
    """``secret://gcp/`` — empty path after the scheme."""
    register_secret_resolver("gcp", FakeResolver)
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://gcp/")
    assert "path" in str(exc_info.value).lower()


def test_parse_url_no_path_slash_rejected() -> None:
    """``secret://gcp`` — no slash at all, no path."""
    with pytest.raises(SecretResolutionError):
        resolve_secret_url("secret://gcp")


def test_parse_url_wrong_outer_scheme_rejected() -> None:
    """A URL whose outer scheme isn't ``secret`` is a parse error."""
    with pytest.raises(SecretResolutionError):
        resolve_secret_url("http://gcp/foo")


def test_parse_url_scheme_with_underscore_rejected() -> None:
    """Underscores aren't in the locked scheme grammar."""
    register_secret_resolver("gcp", FakeResolver)  # canonical
    with pytest.raises(SecretResolutionError):
        resolve_secret_url("secret://my_scheme/foo")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_register_then_resolve() -> None:
    register_secret_resolver("fake", FakeResolver)
    assert resolve_secret_url("secret://fake/anything") == "RESOLVED-VALUE-anything"


def test_register_rejects_bad_scheme() -> None:
    with pytest.raises(ValueError):
        register_secret_resolver("Bad_Scheme", FakeResolver)
    with pytest.raises(ValueError):
        register_secret_resolver("", FakeResolver)


def test_register_rejects_non_callable() -> None:
    with pytest.raises(ValueError):
        register_secret_resolver("fake", "not callable")  # type: ignore[arg-type]


def test_register_duplicate_scheme_rejected() -> None:
    """First registration wins; second is a hard error."""
    register_secret_resolver("fake", FakeResolver)
    with pytest.raises(SecretResolutionError) as exc_info:
        register_secret_resolver("fake", FakeResolver)
    assert "already registered" in str(exc_info.value)


def test_unknown_scheme_message_lists_known() -> None:
    register_secret_resolver("vault", FakeResolver)
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://nope/anything")
    msg = str(exc_info.value)
    assert "no resolver registered for scheme 'nope'" in msg
    assert "vault" in msg


# ---------------------------------------------------------------------------
# Lazy / cached instantiation
# ---------------------------------------------------------------------------


def test_factory_called_once_per_scheme_per_run() -> None:
    """The factory runs lazily, on first reference, and the instance is
    cached for the rest of the process."""
    call_count = {"n": 0}

    class CountingResolver:
        scheme: ClassVar[str] = "counter"

        def __init__(self) -> None:
            call_count["n"] += 1

        def resolve(self, path: str, field: str | None) -> str:
            return f"r:{path}"

    register_secret_resolver("counter", CountingResolver)
    # Registration does NOT instantiate.
    assert call_count["n"] == 0
    resolve_secret_url("secret://counter/a")
    assert call_count["n"] == 1
    resolve_secret_url("secret://counter/b")
    resolve_secret_url("secret://counter/c#field")
    # Still 1 — instance cached for the run.
    assert call_count["n"] == 1


def test_factory_raise_wrapped_with_cause() -> None:
    """A factory that raises surfaces as :class:`SecretResolutionError` with
    the original exception chained as ``__cause__``."""

    def bad_factory() -> SecretResolver:
        raise RuntimeError("init crashed")

    register_secret_resolver("bad", bad_factory)
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://bad/whatever")
    assert "failed to instantiate" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_resolver_raise_wrapped_with_cause() -> None:
    """An exception from ``resolver.resolve`` surfaces with the original
    chained, and the outer message does NOT inline the resolver's text."""

    class FailingResolver:
        scheme: ClassVar[str] = "failing"

        def resolve(self, path: str, field: str | None) -> str:
            raise ValueError("upstream-error-text")

    register_secret_resolver("failing", FailingResolver)
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://failing/x")
    # The outer message names the resolver + exception class — not the
    # original message text (which could embed a resolved value in a real
    # resolver).
    assert "failing" in str(exc_info.value)
    assert "ValueError" in str(exc_info.value)
    assert "upstream-error-text" not in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_resolver_returning_non_string_rejected() -> None:
    class IntResolver:
        scheme: ClassVar[str] = "intish"

        def resolve(self, path: str, field: str | None) -> str:
            return 42  # type: ignore[return-value]

    register_secret_resolver("intish", IntResolver)
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://intish/x")
    assert "non-string" in str(exc_info.value)


def test_factory_returning_object_without_resolve_rejected() -> None:
    class NotAResolver:
        pass

    register_secret_resolver("notaresolver", lambda: NotAResolver())  # type: ignore[arg-type,return-value]
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://notaresolver/x")
    assert ".resolve()" in str(exc_info.value) or "resolve()" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Entry-points discovery (monkeypatched importlib.metadata)
# ---------------------------------------------------------------------------


class _FakeEntryPoint:
    """A stand-in for :class:`importlib.metadata.EntryPoint` minus the
    surrounding machinery — just enough for ``_load_entry_points`` to walk."""

    def __init__(self, name: str, factory: object, *, raise_on_load: Exception | None = None):
        self.name = name
        self._factory = factory
        self._raise = raise_on_load

    def load(self) -> object:
        if self._raise is not None:
            raise self._raise
        return self._factory


def test_entry_point_discovered_and_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fake entry-point is picked up on the first ``resolve_secret_url`` call."""

    class EntryPointResolver:
        scheme: ClassVar[str] = "from-ep"

        def resolve(self, path: str, field: str | None) -> str:
            return f"ep:{path}"

    def fake_entry_points(*, group: str) -> list[_FakeEntryPoint]:
        if group == "dtex.secret_resolvers":
            return [_FakeEntryPoint("from-ep", EntryPointResolver)]
        return []

    monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)
    value = resolve_secret_url("secret://from-ep/abc")
    assert value == "ep:abc"


def test_project_local_wins_over_entry_point(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolution precedence: project-local registration shadows an
    entry-point of the same name."""

    class EntryPointResolver:
        scheme: ClassVar[str] = "shared"

        def resolve(self, path: str, field: str | None) -> str:
            return f"ep:{path}"

    class LocalResolver:
        scheme: ClassVar[str] = "shared"

        def resolve(self, path: str, field: str | None) -> str:
            return f"local:{path}"

    def fake_entry_points(*, group: str) -> list[_FakeEntryPoint]:
        if group == "dtex.secret_resolvers":
            return [_FakeEntryPoint("shared", EntryPointResolver)]
        return []

    monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)
    # Project-local registers FIRST (mirrors load_project_plugins running
    # before the first resolve call).
    register_secret_resolver("shared", LocalResolver)
    value = resolve_secret_url("secret://shared/abc")
    assert value == "local:abc"


def test_broken_entry_point_does_not_block_others(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry-point that fails to load is recorded but does not block the
    rest of discovery, and the failure surfaces only when its scheme is
    referenced."""

    class OkResolver:
        scheme: ClassVar[str] = "ok"

        def resolve(self, path: str, field: str | None) -> str:
            return f"ok:{path}"

    def fake_entry_points(*, group: str) -> list[_FakeEntryPoint]:
        if group == "dtex.secret_resolvers":
            return [
                _FakeEntryPoint(
                    "broken",
                    object(),
                    raise_on_load=ImportError("missing-dep"),
                ),
                _FakeEntryPoint("ok", OkResolver),
            ]
        return []

    monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)
    # The OK one still works.
    assert resolve_secret_url("secret://ok/foo") == "ok:foo"
    # The broken one surfaces its underlying load error as a hint.
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret_url("secret://broken/anything")
    assert "broken" in str(exc_info.value)
    assert "ImportError" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Project-local dtex_plugins.py
# ---------------------------------------------------------------------------


def test_project_plugins_file_runs_registrations(tmp_path: Path) -> None:
    """A ``dtex_plugins.py`` next to ``dtex_project.yml`` is imported, and its
    ``register_secret_resolver`` calls take effect."""
    (tmp_path / "dtex_project.yml").write_text("name: t\n")
    (tmp_path / "dtex_plugins.py").write_text(
        "from typing import ClassVar\n"
        "import dtex\n"
        "\n"
        "class P:\n"
        "    scheme: ClassVar[str] = 'plugin'\n"
        "    def resolve(self, path, field):\n"
        "        return f'plugin:{path}'\n"
        "\n"
        "dtex.register_secret_resolver('plugin', P)\n"
    )
    load_project_plugins(tmp_path)
    assert resolve_secret_url("secret://plugin/abc") == "plugin:abc"


def test_project_plugins_idempotent(tmp_path: Path) -> None:
    """Calling :func:`load_project_plugins` twice for the same root does not
    re-import the file (which would trigger the duplicate-registration
    error)."""
    (tmp_path / "dtex_project.yml").write_text("name: t\n")
    (tmp_path / "dtex_plugins.py").write_text(
        "from typing import ClassVar\n"
        "import dtex\n"
        "\n"
        "class P:\n"
        "    scheme: ClassVar[str] = 'p1'\n"
        "    def resolve(self, path, field):\n"
        "        return 'v'\n"
        "\n"
        "dtex.register_secret_resolver('p1', P)\n"
    )
    load_project_plugins(tmp_path)
    load_project_plugins(tmp_path)  # Must not raise.
    assert resolve_secret_url("secret://p1/x") == "v"


def test_project_plugins_missing_is_noop(tmp_path: Path) -> None:
    """A project without ``dtex_plugins.py`` is fine — no error."""
    (tmp_path / "dtex_project.yml").write_text("name: t\n")
    load_project_plugins(tmp_path)  # No raise.


def test_project_plugins_import_error_wrapped(tmp_path: Path) -> None:
    """A ``dtex_plugins.py`` that raises at import surfaces as
    :class:`SecretResolutionError` with the underlying exception chained."""
    (tmp_path / "dtex_project.yml").write_text("name: t\n")
    (tmp_path / "dtex_plugins.py").write_text("raise RuntimeError('boom')\n")
    with pytest.raises(SecretResolutionError) as exc_info:
        load_project_plugins(tmp_path)
    assert "dtex_plugins.py" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, RuntimeError)


# ---------------------------------------------------------------------------
# Redactor integration — a resolved value must not surface in logs
# ---------------------------------------------------------------------------


def test_resolved_value_redacted_in_logger(tmp_path: Path) -> None:
    """When a resolved value is added to the Redactor, any log line that
    later contains it gets masked to ``***``."""
    import uuid

    register_secret_resolver("fake", FakeResolver)
    resolved = resolve_secret_url("secret://fake/very-long-credential-id")
    # Must be long enough to clear the redactor's _MIN_REDACT_LEN=4 floor.
    assert len(resolved) >= 4

    redactor = Redactor()
    redactor.add([resolved])

    # Unique run_id per invocation so build_logger always builds a fresh
    # handler-less logger — otherwise a prior test's logger of the same
    # name leaks its handlers through to this run.
    run_id = f"redact-test-{uuid.uuid4().hex[:8]}"
    buf = StringIO()
    log = build_logger(run_id, redactor, stream=buf)
    log.info("connecting with %s", resolved)
    # Detach handlers to keep the test logger from leaking into others.
    for h in list(log.handlers):
        log.removeHandler(h)

    output = buf.getvalue()
    assert resolved not in output
    assert "***" in output


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


def test_public_api_exports() -> None:
    """The three new public names are re-exported from the top-level package."""
    assert dtex.SecretResolver is SecretResolver
    assert dtex.SecretResolutionError is SecretResolutionError
    assert dtex.register_secret_resolver is register_secret_resolver
    for name in ("SecretResolver", "SecretResolutionError", "register_secret_resolver"):
        assert name in dtex.__all__


# ---------------------------------------------------------------------------
# Integration: SecretRef accepts secret:// refs (stage 9a extension)
# ---------------------------------------------------------------------------


def test_secret_ref_accepts_secret_url() -> None:
    from dtex.types import SecretRef

    ref = SecretRef.from_dict({"name": "token", "ref": "secret://gcp/projects/x/secrets/y"})
    assert ref.ref == "secret://gcp/projects/x/secrets/y"
    assert SecretRef.is_valid_ref("secret://vault/x/y#token") is True
    assert SecretRef.is_valid_ref("secret://gcp/foo") is True
    # The original two forms still validate.
    assert SecretRef.is_valid_ref("${env.X}") is True
    assert SecretRef.is_valid_ref("${profile.X.Y}") is True
    # Garbage still rejected.
    assert SecretRef.is_valid_ref("vault:/foo") is False


def test_resolve_secret_ref_dispatches_secret_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """The engine's :func:`resolve_secret_ref` dispatches ``secret://`` URLs
    to the plugin registry."""
    from dtex.engine.config import Profiles, resolve_secret_ref
    from dtex.types import SecretRef

    register_secret_resolver("fake", FakeResolver)
    ref = SecretRef(name="token", ref="secret://fake/abc")
    profiles = Profiles(destinations={}, secret_profiles={}, threads=1)
    value = resolve_secret_ref(ref, "dev", profiles)
    assert value == "RESOLVED-VALUE-abc"


def test_resolve_secret_ref_secret_url_failure_wrapped() -> None:
    """An unresolvable ``secret://`` ref surfaces as the engine's
    :class:`ConfigError` (not :class:`SecretResolutionError`) so the
    runner's existing error path stays uniform."""
    from dtex.engine.config import ConfigError, Profiles, resolve_secret_ref
    from dtex.types import SecretRef

    ref = SecretRef(name="token", ref="secret://unknown/abc")
    profiles = Profiles(destinations={}, secret_profiles={}, threads=1)
    with pytest.raises(ConfigError) as exc_info:
        resolve_secret_ref(ref, "dev", profiles)
    assert "token" in str(exc_info.value)
    # The underlying SecretResolutionError is chained.
    assert isinstance(exc_info.value.__cause__, SecretResolutionError)


def teardown_module(module: object) -> None:
    """Final sweep — no module-level state should leak past this file."""
    _reset_resolvers_for_testing()
    # Detach any test loggers we created so they don't accumulate in other suites.
    for name in list(logging.Logger.manager.loggerDict):
        if name.startswith("dtex.run.test-"):
            logger = logging.getLogger(name)
            for h in list(logger.handlers):
                logger.removeHandler(h)
