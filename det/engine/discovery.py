"""Discovery — stage 1 of the run lifecycle (docs/02 §Run lifecycle).

Discovery answers two questions before any data moves:

* **Where is the project?** :func:`find_project_root` walks up from a starting
  directory for ``det_project.yml`` (docs/02, docs/06).
* **What does a connector NAME resolve to?** :func:`resolve_connector` turns a
  name into a :class:`LoadedConnector` — the parsed ``register.yaml`` plus the
  :class:`~det.registry.ConnectorRegistry` the connector's decorators
  populated. Resolution order is fixed (docs/03 §5): project-local
  ``connectors/<name>/`` wins over the baked ``det/connectors/`` (sources)
  / ``det/destinations/`` (destinations).

The connector-folder *import* mechanism — open a
:func:`~det.registry.registration_scope`, exec every ``.py`` file under a
process-unique module name so a re-import genuinely re-runs the decorators — is
the same one ``tests/conftest.py`` uses. It is reused here verbatim, per the
task's "reuse the harness, do not reinvent it" rule; that harness *is* the
engine's discovery-import step (its own docstring says so).

This module also owns discovery-time validation (docs/03 §7): manifest schema
(enforced by the ``types.py`` ``from_dict`` parsers), ``kind`` consistency,
stream integrity, decorator coverage (every ``streams[].name`` has a ``@stream``
and vice versa), and ``@stream`` signature injectability.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

import yaml

from det.registry import (
    ConnectorRegistry,
    registration_scope,
)
from det.types import (
    ConnectorKind,
    ConnectorManifest,
)

# The project root marker file (docs/02, docs/06).
PROJECT_FILE = "det_project.yml"

# The manifest file every connector folder must carry (docs/03 §1).
MANIFEST_FILE = "register.yaml"


class DiscoveryError(Exception):
    """A connector could not be discovered or failed discovery-time validation.

    Raised for: a missing project root, an unresolvable connector name, a
    malformed ``register.yaml``, or any docs/03 §7 validation failure. The
    runner converts it into a ``FAILED`` :class:`~det.types.RunResult`
    rather than letting it escape as an unhandled traceback.
    """


@dataclass(frozen=True)
class LoadedConnector:
    """A discovered connector — its parsed manifest plus its populated registry.

    The in-memory pair the engine carries per connector (the same shape
    ``tests/conftest.py`` calls ``LoadedConnector``): the parsed
    ``register.yaml`` (:class:`~det.types.ConnectorManifest`) and the
    :class:`~det.registry.ConnectorRegistry` the connector's decorators
    populated while its folder was imported.
    """

    manifest: ConnectorManifest
    registry: ConnectorRegistry
    folder: Path


# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------


def find_project_root(start: Path | str | None = None) -> Path:
    """Walk up from ``start`` to the directory holding ``det_project.yml``.

    docs/02: "find the project root (walk up for ``det_project.yml``)".
    ``start`` defaults to the current working directory. The walk stops at the
    filesystem root; if no marker is found a :class:`DiscoveryError` is raised
    with the search origin, so the failure names exactly what was looked for.
    """
    here = Path(start).resolve() if start is not None else Path.cwd().resolve()
    if here.is_file():
        here = here.parent
    for candidate in (here, *here.parents):
        if (candidate / PROJECT_FILE).is_file():
            return candidate
    raise DiscoveryError(
        f"no {PROJECT_FILE} found in {here} or any parent directory; "
        f"a det project must contain a {PROJECT_FILE} (docs/06)"
    )


# ---------------------------------------------------------------------------
# Connector folder location — baked vs project-local (docs/03 §5)
# ---------------------------------------------------------------------------


def _baked_dirs() -> tuple[Path, Path]:
    """Return the two baked connector roots shipped inside the ``det`` package.

    docs/03 §5 / docs/06: baked *sources* live under ``det/connectors/``
    and baked *destinations* under ``det/destinations/``. Both are searched;
    a connector is the same kind of object either way (docs/02).
    """
    pkg_root = Path(__file__).resolve().parent.parent
    return pkg_root / "connectors", pkg_root / "destinations"


def find_connector_folder(
    name: str,
    project_root: Path,
    connector_paths: list[str] | None = None,
) -> Path:
    """Resolve a connector NAME to its folder — project-local beats baked (docs/03 §5).

    Search order (locked decision):

    1. Each ``connector_paths`` directory under ``project_root`` — the
       project-local connectors a user authored or forked.
    2. The baked roots (``det/connectors/``, ``det/destinations/``).

    The first directory ``<dir>/<name>/`` that contains a ``register.yaml`` is
    the match, so a project-local folder shadows a same-named baked one. A name
    that resolves nowhere raises :class:`DiscoveryError` listing every place
    searched — the message *is* the debugging aid.
    """
    searched: list[Path] = []
    for rel in connector_paths or ["connectors"]:
        candidate = (project_root / rel / name).resolve()
        searched.append(candidate)
        if (candidate / MANIFEST_FILE).is_file():
            return candidate
    for baked in _baked_dirs():
        candidate = (baked / name).resolve()
        searched.append(candidate)
        if (candidate / MANIFEST_FILE).is_file():
            return candidate
    locations = "\n  ".join(str(p) for p in searched)
    raise DiscoveryError(
        f"connector {name!r} not found; looked for a {MANIFEST_FILE} in:\n  {locations}"
    )


# ---------------------------------------------------------------------------
# Connector-folder import — the conftest harness, reused (registry.py docstring)
# ---------------------------------------------------------------------------


def _import_module_from_path(path: Path) -> None:
    """Execute one connector ``.py`` file under a process-unique module name.

    A fresh synthetic name per call (``<stem>_<uuid>``) means a re-import of the
    same connector genuinely re-runs the module body — and therefore its
    decorators — instead of hitting a stale ``sys.modules`` cache that would
    leave the second :class:`ConnectorRegistry` empty. This is the exact
    mechanism ``tests/conftest.py`` documents and the registry module's
    docstring assumes; it is reused, not reinvented.
    """
    unique_name = f"_det_connector_{path.stem}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(unique_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover — defensive.
        raise DiscoveryError(f"cannot load connector module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(unique_name, None)


def _load_connector_folder(folder: Path) -> LoadedConnector:
    """Parse + import one connector folder — the engine's discovery-import step.

    Parses ``register.yaml`` into a :class:`ConnectorManifest`, then imports
    every ``.py`` file in the folder inside a *single*
    :func:`~det.registry.registration_scope`, so every ``@stream`` /
    ``@destination`` decorator across all of the connector's files lands in one
    :class:`ConnectorRegistry` (docs/03 §1, registry.py module docstring).

    A YAML parse error or a manifest-schema violation (an unknown
    ``register.yaml`` key, a missing required key — caught by the ``types.py``
    ``from_dict`` parsers, docs/03 §7 step 2) is re-raised as a
    :class:`DiscoveryError` naming the offending folder.
    """
    manifest_path = folder / MANIFEST_FILE
    try:
        raw = yaml.safe_load(manifest_path.read_text())
    except yaml.YAMLError as exc:
        raise DiscoveryError(f"{manifest_path} is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise DiscoveryError(
            f"{manifest_path} must parse to a mapping of register.yaml keys"
        )
    try:
        manifest = ConnectorManifest.from_dict(raw)
    except (ValueError, TypeError) as exc:
        raise DiscoveryError(f"invalid {manifest_path}: {exc}") from exc

    py_files = sorted(p for p in folder.glob("*.py") if p.name != "__init__.py")
    try:
        with registration_scope(manifest.name) as registry:
            for py_file in py_files:
                _import_module_from_path(py_file)
    except (ValueError, TypeError) as exc:
        # A decorator raised at import time — a duplicate stream name, a bad
        # @stream signature (docs/03 §7 rules 7-8). Surface it as discovery.
        raise DiscoveryError(
            f"connector {manifest.name!r} failed to import cleanly: {exc}"
        ) from exc
    return LoadedConnector(manifest=manifest, registry=registry, folder=folder)


# ---------------------------------------------------------------------------
# Discovery-time validation — docs/03 §7
# ---------------------------------------------------------------------------


def validate_connector(loaded: LoadedConnector) -> None:
    """Run the code-vs-manifest discovery checks — docs/03 §7 steps 3, 4, 7, 8.

    The manifest *schema* checks (steps 1, 2) and the stream-integrity rules
    (step 4: unique names, ``merge`` ⇒ ``primary_key``, cursor field in schema)
    are enforced inside the ``types.py`` ``from_dict`` parsers and ``__post_init__``
    — so a parsed :class:`ConnectorManifest` is already known-valid for those.
    This function adds the checks that need the *imported code*:

    * **kind ↔ registry consistency** — a ``kind: source`` manifest must have a
      source registry (``@stream`` registrations), a ``kind: destination``
      manifest a destination registry (``@destination`` hooks).
    * **Decorator coverage (step 7)** — every ``streams[].name`` has exactly one
      ``@stream`` / ``@stream_method``; no ``@stream`` is an orphan with no
      manifest entry.
    * **Signature injectability (step 8)** — the ``@stream`` decorators already
      reject a non-injectable parameter at import time; this re-confirms each
      registered stream carries the recorded injectable list.

    docs/03 §7: validation is fail-fast but reports *every* problem found, so
    the raised :class:`DiscoveryError` aggregates all coverage gaps at once.
    """
    manifest = loaded.manifest
    registry = loaded.registry
    problems: list[str] = []

    if manifest.kind is ConnectorKind.SOURCE:
        if registry.kind is not ConnectorKind.SOURCE:
            problems.append(
                "manifest declares kind 'source' but no @stream functions were "
                "registered when the connector was imported"
            )
        else:
            declared = {s.name for s in manifest.streams}
            registered = set(registry.stream_names)
            for missing in sorted(declared - registered):
                problems.append(
                    f"stream {missing!r} is declared in register.yaml but has no "
                    f"matching @stream/@stream_method (docs/03 §7 rule 7)"
                )
            for orphan in sorted(registered - declared):
                problems.append(
                    f"@stream {orphan!r} has no matching streams[] entry in "
                    f"register.yaml (docs/03 §7 rule 7)"
                )
            for name in sorted(declared & registered):
                reg = registry.stream(name)
                if reg is not None and reg.inject is None:  # pragma: no cover
                    problems.append(f"stream {name!r} has no recorded injectable list")
    else:  # ConnectorKind.DESTINATION
        if registry.kind is not ConnectorKind.DESTINATION:
            problems.append(
                "manifest declares kind 'destination' but no @destination hooks "
                "were registered when the connector was imported"
            )
        else:
            missing_hooks = registry.missing_mandatory_hooks()
            if missing_hooks:
                problems.append(
                    f"destination is missing mandatory @destination hook(s): "
                    f"{', '.join(missing_hooks)} (docs/03 §3.4)"
                )

    if problems:
        bullets = "\n  - ".join(problems)
        raise DiscoveryError(
            f"connector {manifest.name!r} failed discovery-time validation "
            f"(docs/03 §7):\n  - {bullets}"
        )


# ---------------------------------------------------------------------------
# The public discovery entry point
# ---------------------------------------------------------------------------


def resolve_connector(
    name: str,
    project_root: Path,
    connector_paths: list[str] | None = None,
    *,
    validate: bool = True,
) -> LoadedConnector:
    """Resolve a connector NAME to a validated :class:`LoadedConnector` — docs/02 §1.

    The full discovery step for one connector: locate its folder
    (:func:`find_connector_folder` — project-local beats baked), parse + import
    it (:func:`_load_connector_folder`), then run discovery-time validation
    (:func:`validate_connector`) unless ``validate=False``.

    Every failure path raises :class:`DiscoveryError`; the runner catches it and
    records a ``FAILED`` run rather than letting it crash.
    """
    folder = find_connector_folder(name, project_root, connector_paths)
    loaded = _load_connector_folder(folder)
    if validate:
        validate_connector(loaded)
    return loaded


def connectors_with_tag(
    tag: str,
    project_root: Path,
    connector_paths: list[str] | None = None,
) -> list[str]:
    """Return the names of every connector whose manifest declares ``tag`` — docs/02.

    docs/02 §Tag-based selection: ``--tag`` is "a filter over discovered
    connectors". Every connector folder reachable on ``connector_paths`` (and
    every baked one) is scanned; a folder whose ``register.yaml`` lists ``tag``
    in its ``tags`` is included. The result is sorted for deterministic run
    order. A folder whose ``register.yaml`` will not parse is skipped silently
    here — a broken connector should not block tag selection of the good ones;
    it fails loudly later if a run actually selects it.
    """
    roots: list[Path] = [
        project_root / rel for rel in (connector_paths or ["connectors"])
    ]
    roots.extend(_baked_dirs())

    matched: set[str] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for child in sorted(root.iterdir()):
            manifest_path = child / MANIFEST_FILE
            if not manifest_path.is_file():
                continue
            try:
                raw = yaml.safe_load(manifest_path.read_text())
                manifest = ConnectorManifest.from_dict(raw)
            except (yaml.YAMLError, ValueError, TypeError, OSError):
                continue
            if tag in manifest.tags:
                matched.add(manifest.name)
    return sorted(matched)
