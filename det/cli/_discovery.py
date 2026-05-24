"""Connector enumeration helpers for the CLI's ``list`` / ``validate`` commands.

The engine's :mod:`det.engine.discovery` resolves a connector *by name*
(:func:`resolve_connector`) and answers "which connectors carry tag X"
(:func:`connectors_with_tag`), but it has no public "enumerate every connector"
call. ``det list`` and ``det validate --all`` need exactly that.

This module supplies that enumeration. It is **not** engine logic — it only
walks the same connector roots the engine's discovery walks, parses each
``register.yaml`` into a :class:`~det.types.ConnectorManifest`, and yields
the result. The parsing is the engine's own
:class:`~det.types.ConnectorManifest.from_dict`; the walking mirrors
:func:`det.engine.discovery.connectors_with_tag` (which is the documented
"filter over discovered connectors"). No data ever moves here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from det.engine import discovery as disc
from det.types import ConnectorManifest


@dataclass(frozen=True)
class DiscoveredConnector:
    """One connector found on disk — its folder, manifest, and origin label.

    ``origin`` is ``"project"`` for a folder under the project's
    ``connector_paths`` and ``"baked"`` for one shipped inside the ``det``
    package, so ``det list`` can show where a connector came from.
    """

    name: str
    folder: Path
    manifest: ConnectorManifest
    origin: str


def discover_all(
    project_root: Path, connector_paths: list[str] | None = None
) -> list[DiscoveredConnector]:
    """Enumerate every connector reachable from a project — for ``det list``.

    Walks the project-local ``connector_paths`` directories and the baked
    connector roots (the exact roots
    :func:`det.engine.discovery.connectors_with_tag` walks), parses each
    folder's ``register.yaml``, and returns one
    :class:`DiscoveredConnector` per folder that parses cleanly.

    Resolution order matches the engine (docs/03 §5): project-local shadows a
    same-named baked connector, so a name seen under ``connector_paths`` first
    is not re-added from the baked roots. A folder whose ``register.yaml`` will
    not parse is skipped silently — mirroring ``connectors_with_tag``'s "a
    broken connector should not block listing the good ones"; it fails loudly
    later if a run or ``validate`` actually selects it.
    """
    project_roots = [
        (project_root / rel, "project") for rel in (connector_paths or ["connectors"])
    ]
    # disc._baked_dirs() is the engine's own (connectors/, destinations/) pair —
    # reused here so the CLI's listing matches what the engine would resolve.
    baked_roots = [(d, "baked") for d in disc._baked_dirs()]

    found: dict[str, DiscoveredConnector] = {}
    for root, origin in [*project_roots, *baked_roots]:
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
            # Project-local beats baked: a name already found is not overwritten.
            if manifest.name in found:
                continue
            found[manifest.name] = DiscoveredConnector(
                name=manifest.name,
                folder=child,
                manifest=manifest,
                origin=origin,
            )
    return sorted(found.values(), key=lambda c: c.name)
