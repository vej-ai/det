"""Shared test fixtures + the connector-folder import harness.

The engine (stage 5) does not exist yet, so these tests have to do for
themselves the one thing the engine will do: import a connector folder's
``.py`` files *inside* a :func:`~det.registry.registration_scope` so the
``@stream`` / ``@destination`` decorators register into one
:class:`~det.registry.ConnectorRegistry`.

:func:`load_connector` is that harness. The DuckDB destination and the ``echo``
fixture source are both loaded through it — exactly the path the engine's
discovery step will take.
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

from det.registry import ConnectorRegistry, registration_scope
from det.types import ConnectorManifest

# --------------------------------------------------------------------------
# Connector-folder locations
# --------------------------------------------------------------------------

# The pre-baked DuckDB destination folder, inside the installed package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_CONNECTOR_DIR = _REPO_ROOT / "det" / "destinations" / "duckdb"
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
    """Discover + import one connector folder — the engine's discovery step.

    Parses ``register.yaml`` into a :class:`ConnectorManifest`, then imports
    every ``.py`` file in the folder *inside a single*
    :func:`registration_scope` so every decorator from every file lands in one
    registry (docs/03 §1, registry.py module docstring).

    Each module is loaded under a process-unique synthetic name, so importing
    the same connector twice in one test session genuinely re-executes its
    decorators (no ``sys.modules`` cache hit that would leave the second
    registry empty).
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
    unique_name = f"_det_test_connector_{path.stem}_{uuid.uuid4().hex}"
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
