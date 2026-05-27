# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Source / destination / config enumeration helpers for the CLI.

``dtex list`` and ``dtex validate`` need an "enumerate every discoverable
component" call; the engine's :mod:`dtex.engine.discovery` resolves only *by
name*, and :mod:`dtex.engine.configs` only loads configs. This module supplies
the enumeration: it walks the same project-local + baked roots the engine
walks (separately for sources and destinations after the stage 8.B split),
parses each ``register.yaml`` into a :class:`~dtex.types.ConnectorManifest`,
and yields the result. It is **not** engine logic — no data ever moves here.

For configs: :func:`discover_all_configs` re-uses
:func:`dtex.engine.configs.discover_configs` directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from dtex.engine import configs as cfgs
from dtex.engine import discovery as disc
from dtex.types import ConnectorKind, ConnectorManifest, PipelineConfig


@dataclass(frozen=True)
class DiscoveredConnector:
    """One source or destination found on disk — its folder, manifest, origin.

    ``origin`` is ``"project"`` for a folder under the project's source/
    destination paths and ``"baked"`` for one shipped inside the ``dtex``
    package, so ``dtex list`` can show where a connector came from.
    """

    name: str
    folder: Path
    manifest: ConnectorManifest
    origin: str

    @property
    def kind(self) -> ConnectorKind:
        """The connector's kind — convenience accessor."""
        return self.manifest.kind


def _walk_roots(
    roots: list[tuple[Path, str]],
    found: dict[str, DiscoveredConnector],
) -> None:
    """Walk each ``(root, origin)`` pair, populating ``found`` with discoveries.

    A project-local folder that matches a baked name shadows the baked entry
    (docs/03 §5) — the engine's own resolution rule. A folder whose
    ``register.yaml`` will not parse is skipped silently, mirroring the engine's
    "a broken connector should not block listing the good ones" rule; it fails
    loudly later if a run or ``validate`` actually selects it.
    """
    for root, origin in roots:
        if not root.is_dir():
            continue
        for child in sorted(root.iterdir()):
            manifest_path = child / disc.MANIFEST_FILE
            if not manifest_path.is_file():
                continue
            try:
                raw = yaml.safe_load(manifest_path.read_text())
                manifest = ConnectorManifest.from_dict(raw)
            except (yaml.YAMLError, ValueError, TypeError, OSError):
                continue
            if manifest.name in found:
                continue
            found[manifest.name] = DiscoveredConnector(
                name=manifest.name,
                folder=child,
                manifest=manifest,
                origin=origin,
            )


def discover_all_sources(
    project_root: Path, source_paths: list[str] | None = None
) -> list[DiscoveredConnector]:
    """Enumerate every SOURCE reachable from a project — docs/03 §5, docs/06.

    Walks the project-local ``source_paths`` (default ``["sources"]``) first,
    then the baked ``dtex/sources/``. Project-local shadows baked. Folders
    whose ``register.yaml`` declares ``kind: destination`` are filtered out —
    a destination accidentally placed under ``sources/`` is ignored from this
    listing (``discover_all_destinations`` will pick it up if and only if it
    lives under a destination path).
    """
    project = [(project_root / rel, "project") for rel in (source_paths or ["sources"])]
    baked = [(disc._baked_source_dir(), "baked")]
    found: dict[str, DiscoveredConnector] = {}
    _walk_roots([*project, *baked], found)
    return sorted(
        (c for c in found.values() if c.manifest.kind is ConnectorKind.SOURCE),
        key=lambda c: c.name,
    )


def discover_all_destinations(
    project_root: Path, destination_paths: list[str] | None = None
) -> list[DiscoveredConnector]:
    """Enumerate every DESTINATION reachable from a project — docs/03 §5, docs/06.

    Destination-side analogue of :func:`discover_all_sources`: walks the
    project-local ``destination_paths`` (default ``["destinations"]``) then
    the baked ``dtex/destinations/``. Folders whose ``register.yaml`` declares
    ``kind: source`` are filtered out for the same reason.
    """
    project = [
        (project_root / rel, "project")
        for rel in (destination_paths or ["destinations"])
    ]
    baked = [(disc._baked_destination_dir(), "baked")]
    found: dict[str, DiscoveredConnector] = {}
    _walk_roots([*project, *baked], found)
    return sorted(
        (c for c in found.values() if c.manifest.kind is ConnectorKind.DESTINATION),
        key=lambda c: c.name,
    )


def discover_all_configs(
    project_root: Path, config_paths: list[str] | None = None
) -> list[PipelineConfig]:
    """Enumerate every PIPELINE CONFIG reachable from a project — docs/12.

    Delegates to :func:`dtex.engine.configs.discover_configs` and returns the
    parsed :class:`PipelineConfig` objects in sorted-name order, ready for
    ``dtex list --kind config`` rendering.
    """
    return sorted(
        cfgs.discover_configs(project_root, config_paths).values(),
        key=lambda c: c.name,
    )
