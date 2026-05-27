# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Discovery — stage 1 of the run lifecycle (docs/02 §Run lifecycle).

Discovery answers three questions before any data moves:

* **Where is the project?** :func:`find_project_root` walks up from a starting
  directory for ``dtex_project.yml`` (docs/02, docs/06).
* **What does a SOURCE name resolve to?** :func:`resolve_source` turns a name
  into a :class:`LoadedConnector` — the parsed ``register.yaml`` plus the
  :class:`~dtex.registry.ConnectorRegistry` the connector's decorators
  populated. Resolution order is fixed (docs/03 §5): project-local
  ``sources/<name>/`` wins over the baked ``dtex/sources/<name>/``.
* **What does a DESTINATION name resolve to?** :func:`resolve_destination`
  does the same for destination connectors, walking project-local
  ``destinations/<name>/`` then baked ``dtex/destinations/<name>/``.

Stage 8.B split the old generic ``resolve_connector`` into the two functions
above, mirroring the project-layout split (``sources/`` + ``destinations/``).
The lookup algorithm is unchanged; the only thing that changed is which roots
are searched.

The connector-folder *import* mechanism — open a
:func:`~dtex.registry.registration_scope`, exec every ``.py`` file under a
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

from dtex.registry import (
    ConnectorRegistry,
    registration_scope,
)
from dtex.types import (
    ConnectorKind,
    ConnectorManifest,
)

# The project root marker file (docs/02, docs/06).
PROJECT_FILE = "dtex_project.yml"

# The manifest file every connector folder must carry (docs/03 §1).
MANIFEST_FILE = "register.yaml"


class DiscoveryError(Exception):
    """A connector could not be discovered or failed discovery-time validation.

    Raised for: a missing project root, an unresolvable connector name, a
    malformed ``register.yaml``, or any docs/03 §7 validation failure. The
    runner converts it into a ``FAILED`` :class:`~dtex.types.RunResult`
    rather than letting it escape as an unhandled traceback.
    """


@dataclass(frozen=True)
class LoadedConnector:
    """A discovered connector — its parsed manifest plus its populated registry.

    The in-memory pair the engine carries per connector (the same shape
    ``tests/conftest.py`` calls ``LoadedConnector``): the parsed
    ``register.yaml`` (:class:`~dtex.types.ConnectorManifest`) and the
    :class:`~dtex.registry.ConnectorRegistry` the connector's decorators
    populated while its folder was imported.
    """

    manifest: ConnectorManifest
    registry: ConnectorRegistry
    folder: Path


# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------


def find_project_root(start: Path | str | None = None) -> Path:
    """Walk up from ``start`` to the directory holding ``dtex_project.yml``.

    docs/02: "find the project root (walk up for ``dtex_project.yml``)".
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
        f"a dtex project must contain a {PROJECT_FILE} (docs/06)"
    )


# ---------------------------------------------------------------------------
# Baked roots — sources and destinations (docs/03 §5, docs/06 post-8.B)
# ---------------------------------------------------------------------------


def _baked_source_dir() -> Path:
    """Return the baked-sources root shipped inside the ``dtex`` package.

    docs/03 §5 / docs/06 post-8.B: baked *sources* live under ``dtex/sources/``
    (renamed from ``dtex/connectors/`` in stage 8.B). A custom source's
    project-local folder under ``<project>/sources/<name>/`` shadows the baked
    same-named one.
    """
    return Path(__file__).resolve().parent.parent / "sources"


def _baked_destination_dir() -> Path:
    """Return the baked-destinations root shipped inside the ``dtex`` package.

    docs/03 §5 / docs/06: baked *destinations* live under ``dtex/destinations/``;
    a project-local ``<project>/destinations/<name>/`` shadows the baked
    same-named one.
    """
    return Path(__file__).resolve().parent.parent / "destinations"


# ---------------------------------------------------------------------------
# Folder location — split per kind (docs/06 post-8.B)
# ---------------------------------------------------------------------------


def find_source_folder(
    name: str,
    project_root: Path,
    source_paths: list[str] | None = None,
) -> Path:
    """Resolve a SOURCE NAME to its folder — project-local beats baked (docs/03 §5).

    Search order (locked decision, mirrors the original resolution rule):

    1. Each ``source_paths`` directory under ``project_root`` — the
       project-local sources a user authored or forked.
    2. The baked source root (``dtex/sources/``).

    The first directory ``<dir>/<name>/`` that contains a ``register.yaml`` is
    the match, so a project-local folder shadows a same-named baked one. A name
    that resolves nowhere raises :class:`DiscoveryError` listing every place
    searched — the message *is* the debugging aid.
    """
    searched: list[Path] = []
    for rel in source_paths or ["sources"]:
        candidate = (project_root / rel / name).resolve()
        searched.append(candidate)
        if (candidate / MANIFEST_FILE).is_file():
            return candidate
    baked = _baked_source_dir() / name
    searched.append(baked)
    if (baked / MANIFEST_FILE).is_file():
        return baked
    locations = "\n  ".join(str(p) for p in searched)
    raise DiscoveryError(
        f"source {name!r} not found; looked for a {MANIFEST_FILE} in:\n  {locations}"
    )


def find_destination_folder(
    name: str,
    project_root: Path,
    destination_paths: list[str] | None = None,
) -> Path:
    """Resolve a DESTINATION NAME to its folder — project-local beats baked.

    The destination-side analogue of :func:`find_source_folder`: walks each
    ``destination_paths`` directory under ``project_root`` first, then the
    baked ``dtex/destinations/``. Project-local shadows baked (docs/03 §5).
    """
    searched: list[Path] = []
    for rel in destination_paths or ["destinations"]:
        candidate = (project_root / rel / name).resolve()
        searched.append(candidate)
        if (candidate / MANIFEST_FILE).is_file():
            return candidate
    baked = _baked_destination_dir() / name
    searched.append(baked)
    if (baked / MANIFEST_FILE).is_file():
        return baked
    locations = "\n  ".join(str(p) for p in searched)
    raise DiscoveryError(
        f"destination {name!r} not found; looked for a {MANIFEST_FILE} in:\n  {locations}"
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
    unique_name = f"_dtex_connector_{path.stem}_{uuid.uuid4().hex}"
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
    :func:`~dtex.registry.registration_scope`, so every ``@stream`` /
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
# The public discovery entry points — one per kind (docs/06 post-8.B)
# ---------------------------------------------------------------------------


def resolve_source(
    name: str,
    project_root: Path,
    source_paths: list[str] | None = None,
    *,
    validate: bool = True,
) -> LoadedConnector:
    """Resolve a SOURCE NAME to a validated :class:`LoadedConnector` — docs/02 §1.

    The full discovery step for one source: locate its folder
    (:func:`find_source_folder` — project-local beats baked), parse + import
    it (:func:`_load_connector_folder`), then run discovery-time validation
    (:func:`validate_connector`) unless ``validate=False``.

    Raises :class:`DiscoveryError` on any failure; the runner catches it and
    records a ``FAILED`` run rather than letting it crash. Additionally raises
    if the folder turns out to declare ``kind: destination`` — a source name
    must resolve to a source.
    """
    folder = find_source_folder(name, project_root, source_paths)
    loaded = _load_connector_folder(folder)
    if loaded.manifest.kind is not ConnectorKind.SOURCE:
        raise DiscoveryError(
            f"{name!r} resolved to a {loaded.manifest.kind.value}, not a source "
            f"(docs/03 §2.1); only sources may be referenced from a config's "
            f"'source:' field"
        )
    if validate:
        validate_connector(loaded)
    return loaded


def resolve_destination(
    name: str,
    project_root: Path,
    destination_paths: list[str] | None = None,
    *,
    validate: bool = True,
) -> LoadedConnector:
    """Resolve a DESTINATION NAME to a validated :class:`LoadedConnector` — docs/02 §1.

    The destination-side analogue of :func:`resolve_source`: locate the folder,
    import it, validate it. Raises :class:`DiscoveryError` if the folder
    declares ``kind: source`` — a destination name must resolve to a destination.
    """
    folder = find_destination_folder(name, project_root, destination_paths)
    loaded = _load_connector_folder(folder)
    if loaded.manifest.kind is not ConnectorKind.DESTINATION:
        raise DiscoveryError(
            f"{name!r} resolved to a {loaded.manifest.kind.value}, not a "
            f"destination (docs/03 §2.1); only destinations may be referenced "
            f"from a config's 'destination:' field"
        )
    if validate:
        validate_connector(loaded)
    return loaded
