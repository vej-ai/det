# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Configs — stage 8.B's parser for ``configs/*.yml`` (docs/06, docs/12).

A *config* is the runtime unit: one config = one pipeline (source +
destination + target + params + select). The CLI's ``-p/--conf`` names a
config; the engine resolves it to a :class:`~dtex.types.PipelineConfig` and
runs its lifecycle. This module owns:

* **Discovery** — :func:`discover_configs` walks each
  :class:`~dtex.engine.config.ProjectConfig.config_paths` directory (default:
  ``configs/``) and parses every ``*.yml``/``*.yaml`` file.
* **Two file shapes**:

  - One config per file: top-level keys ``name`` / ``source`` /
    ``destination`` / ``target`` / ``params`` / ``destination_params`` /
    ``select`` / ``schedule`` (docs/12).
  - Many configs per file: a top-level ``configs:`` list of those mappings,
    so a project can group related pipelines (e.g. one file per source).

  Discovery accepts both interchangeably; the file name has no semantic role.

* **Validation** — required keys (``name``, ``source``, ``destination``)
  enforced by :class:`PipelineConfig.from_dict`; duplicate names across files
  are a hard error (:func:`discover_configs`); unknown top-level keys are a
  hard error (catches typos like ``destintion``).

* **Lookup** — :func:`load_config` resolves one config by name through the
  same discovery, raising a clear :class:`~dtex.engine.config.ConfigError` for
  an unknown name (listing the configs the project does define).
"""

from __future__ import annotations

from pathlib import Path

import yaml

from dtex.engine.config import ConfigError
from dtex.types import PipelineConfig


def _parse_one_file(path: Path) -> list[PipelineConfig]:
    """Parse one ``configs/<name>.yml`` file into its zero-or-more configs.

    Two accepted shapes (docs/12):

    * A single-config mapping with ``name`` / ``source`` / ``destination``
      keys at the top level → one :class:`PipelineConfig`.
    * A multi-config mapping ``{"configs": [<mapping>, ...]}`` → one
      :class:`PipelineConfig` per list entry.

    Anything else is a :class:`ConfigError` naming the file, so a typo in the
    shape never silently drops configs.
    """
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
    if raw == {} or raw is None:
        return []
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{path} must parse to a mapping (a single config) or a mapping "
            f"with a 'configs:' list"
        )

    if "configs" in raw and isinstance(raw["configs"], list):
        # Multi-config file. The 'configs:' key may not coexist with the
        # single-config top-level keys — that would be ambiguous.
        forbidden = {"name", "source", "destination"} & set(raw)
        if forbidden:
            raise ConfigError(
                f"{path}: a 'configs:' list and top-level "
                f"{', '.join(sorted(forbidden))} are mutually exclusive — "
                f"either name one config inline or list many under 'configs:'"
            )
        configs: list[PipelineConfig] = []
        for index, item in enumerate(raw["configs"]):
            if not isinstance(item, dict):
                raise ConfigError(
                    f"{path}: configs[{index}] must be a mapping"
                )
            try:
                configs.append(PipelineConfig.from_dict(item))
            except (ValueError, TypeError) as exc:
                raise ConfigError(f"{path}: configs[{index}]: {exc}") from exc
        return configs

    # Single-config file.
    try:
        return [PipelineConfig.from_dict(raw)]
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"{path}: {exc}") from exc


def discover_configs(
    project_root: Path, config_paths: list[str] | None = None
) -> dict[str, PipelineConfig]:
    """Discover every ``PipelineConfig`` reachable from a project — docs/12.

    Walks each directory in ``config_paths`` (default ``["configs"]``) under
    ``project_root``, parses every ``*.yml`` and ``*.yaml`` file via
    :func:`_parse_one_file`, and returns ``{name: PipelineConfig}``.

    Duplicate config names — whether across files or within one multi-config
    file — are a hard error. The first definition's file name is included in
    the message so the conflict is debuggable.
    """
    discovered: dict[str, PipelineConfig] = {}
    origins: dict[str, Path] = {}
    for rel in config_paths or ["configs"]:
        root = project_root / rel
        if not root.is_dir():
            continue
        files = sorted(
            [*root.glob("*.yml"), *root.glob("*.yaml")],
            key=lambda p: p.name,
        )
        for path in files:
            for config in _parse_one_file(path):
                if config.name in discovered:
                    first = origins[config.name]
                    raise ConfigError(
                        f"duplicate config name {config.name!r}: defined in "
                        f"{first} and again in {path}"
                    )
                discovered[config.name] = config
                origins[config.name] = path
    return discovered


def load_config(
    name: str,
    project_root: Path,
    config_paths: list[str] | None = None,
) -> PipelineConfig:
    """Look up one config by name — the engine's RESOLVE-time entry point.

    Calls :func:`discover_configs` and returns ``configs[name]`` or raises
    :class:`ConfigError` listing the configs the project does define, so a
    typo'd ``-p/--conf`` fails clearly.
    """
    configs = discover_configs(project_root, config_paths)
    if name not in configs:
        known = ", ".join(sorted(configs)) or "(none defined)"
        raise ConfigError(
            f"config {name!r} not found; known configs: {known}"
        )
    return configs[name]
