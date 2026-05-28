"""Shared test fixtures + a small connector-folder import harness.

:func:`load_connector` is the test-only loader the registry-layer tests use to
exercise one connector folder in isolation. It opens a
:func:`~dtex.registry.registration_scope` and execs every ``.py`` file in the
folder as a standalone synthetic module, so the connector's decorators
populate one :class:`~dtex.registry.ConnectorRegistry`.

This harness pre-dates the engine and is *not* the engine's production load
path. As of stage 11 the engine loads each connector folder as a synthetic
*Python package* under a process-unique name — see
``dtex/engine/discovery.py:_load_connector_folder``. That mechanism is what
lets a project-local connector use ``from .client import X`` between its own
sibling files. The harness here is still fine for the single-file fixture
connectors (``echo`` source, baked DuckDB destination) it drives, because
neither uses a relative import; tests that need to assert on the engine's
real load behaviour go through ``dtex.run`` / ``disc.resolve_*`` and exercise
the package-load path directly.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from dtex.registry import ConnectorRegistry, registration_scope
from dtex.types import ConnectorManifest

# --------------------------------------------------------------------------
# Connector-folder locations
# --------------------------------------------------------------------------

# The pre-baked DuckDB destination folder, inside the installed package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_CONNECTOR_DIR = _REPO_ROOT / "dtex" / "destinations" / "duckdb"
# The echo fixture source folder, under tests/. Stage 8.B split connectors/
# into sources/ + destinations/ — the echo source lives at tests/fixtures/
# sources/echo/ (the test project's own source_paths: [sources]).
ECHO_CONNECTOR_DIR = _REPO_ROOT / "tests" / "fixtures" / "sources" / "echo"


class LoadedConnector:
    """A connector folder loaded for a test — its manifest + populated registry.

    This is the in-memory pair the engine (stage 5) carries per connector: the
    parsed ``register.yaml`` (a :class:`ConnectorManifest`) and the
    :class:`ConnectorRegistry` the connector's decorators populated.
    """

    def __init__(self, manifest: ConnectorManifest, registry: ConnectorRegistry) -> None:
        self.manifest = manifest
        self.registry = registry


def load_connector(folder: Path) -> LoadedConnector:
    """Test-only loader for one connector folder.

    Parses ``register.yaml`` into a :class:`ConnectorManifest`, then imports
    every ``.py`` file in the folder *inside a single*
    :func:`registration_scope` so every decorator from every file lands in one
    registry (docs/03 §1, registry.py module docstring).

    Each module is loaded under a process-unique synthetic name, so importing
    the same connector twice in one test session genuinely re-executes its
    decorators (no ``sys.modules`` cache hit that would leave the second
    registry empty).

    This is *not* the engine's production load path — the engine loads each
    connector folder as a synthetic Python package
    (``dtex/engine/discovery.py:_load_connector_folder``). For the single-file
    fixture connectors this harness drives, the two paths are observationally
    equivalent; tests that need the engine's exact behaviour go through
    ``dtex.run`` or ``disc.resolve_*``.
    """
    manifest_path = folder / "register.yaml"
    raw = yaml.safe_load(manifest_path.read_text())
    manifest = ConnectorManifest.from_dict(raw)

    py_files = sorted(
        p for p in folder.glob("*.py") if p.name != "__init__.py"
    )

    with registration_scope(manifest.name) as registry:
        for py_file in py_files:
            _import_module_from_path(py_file)
    return LoadedConnector(manifest=manifest, registry=registry)


def _import_module_from_path(path: Path) -> None:
    """Execute one connector ``.py`` file under a process-unique module name.

    A fresh name per call (``<stem>_<uuid>``) means a re-import re-runs the
    module body — and therefore the decorators — instead of getting a cached
    no-op. The module is registered in ``sys.modules`` before execution so any
    intra-module ``from . import`` style reference resolves.
    """
    unique_name = f"_dtex_test_connector_{path.stem}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(unique_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover — defensive.
        raise ImportError(f"cannot load connector module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(unique_name, None)


# --------------------------------------------------------------------------
# pytest fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def duckdb_path(tmp_path: Path) -> str:
    """A path to a fresh ``.duckdb`` file inside the test's temp dir.

    File-backed (not ``:memory:``) so a test can close the connection, reopen
    it, and assert state persisted across the reopen — the "resume from a fresh
    process" property the state design promises (docs/05 §5.1).
    """
    return str(tmp_path / "warehouse.duckdb")


@pytest.fixture
def query_duckdb() -> Callable[[str, str], list[tuple[Any, ...]]]:
    """Return a helper that runs a read-only query against a ``.duckdb`` file.

    Opens a *separate* connection, so it observes only what was durably
    committed — the right lens for asserting on a destination's output.
    """

    def _query(db_path: str, sql: str) -> list[tuple[Any, ...]]:
        conn = duckdb.connect(db_path)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    return _query


@pytest.fixture
def duckdb_destination() -> Iterator[LoadedConnector]:
    """The pre-baked DuckDB destination connector, loaded via the harness."""
    yield load_connector(DUCKDB_CONNECTOR_DIR)


@pytest.fixture
def echo_source() -> Iterator[LoadedConnector]:
    """The echo fixture source connector, loaded via the harness."""
    yield load_connector(ECHO_CONNECTOR_DIR)


# The committed fixture project (`tests/fixtures/`) is a real dtex project, so
# tests that call ``dtex.run(project_dir=str(FIXTURES_DIR), ...)`` cause the
# engine to write its per-run JSONL log into ``tests/fixtures/.dtex/logs/``
# (stage 8a, docs/09 §3.2 — project-rooted by design). ``.dtex/`` is
# gitignored, but accumulated cruft over many runs is ugly. This autouse
# fixture wipes it before *and* after each test, keeping the committed tree
# spotless. Tests that copy the fixture into ``tmp_path`` are unaffected.
_FIXTURE_DTEX_DIR = (
    Path(__file__).resolve().parent / "fixtures" / ".dtex"
)


@pytest.fixture(autouse=True)
def _clean_fixture_dtex_dir() -> Iterator[None]:
    """Remove ``tests/fixtures/.dtex/`` around each test (engine writes leak there)."""
    import shutil as _sh

    if _FIXTURE_DTEX_DIR.exists():
        _sh.rmtree(_FIXTURE_DTEX_DIR, ignore_errors=True)
    try:
        yield
    finally:
        if _FIXTURE_DTEX_DIR.exists():
            _sh.rmtree(_FIXTURE_DTEX_DIR, ignore_errors=True)
