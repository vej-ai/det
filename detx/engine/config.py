# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Config resolution — stage 2 (RESOLVE) of the run lifecycle (docs/02, docs/03 §6).

This module merges every config layer into the frozen
:class:`~detx.types.RunConfig` the runner carries and the immutable
:class:`~detx.types.Config` objects connector code receives. After RESOLVE,
nothing reads ambient config (docs/02).

**Param precedence** (docs/03 §6, lowest → highest):

1. ``register.yaml`` ``params[].default`` — the connector's own defaults.
2. ``detx_project.yml`` ``vars`` — project-wide overrides.
3. The active config's ``params:`` block — per-pipeline overrides (docs/12).
4. environment variables — ``SIMPLE_E_PARAM_<NAME>``.
5. CLI flags / ``run()`` kwargs — per-invocation, highest.

**Destination params** (docs/06 post-8.B): the destination's connection params
come from ``profiles.yml[<destination_name>].targets[<target>]``; per-config
``destination_params:`` overrides apply on top; CLI
``--destination-param k=v`` / ``run(destination_params_override=…)`` wins last.

**Secrets** are resolved from a source's ``register.yaml`` ``secrets[]``
list. Exactly two resolver forms exist (a locked decision, docs/03 §2.5):
``${env.X}`` reads ``os.environ['X']``; ``${profile.X.Y}`` reads key ``Y`` of
the active target's ``profiles.<target>.X`` block (docs/06 post-8.B). Secret
*values* are never logged.

# NOTE: stage 8.B made ``profiles.yml`` destination-keyed instead of
# target-keyed. The two-resolver-form contract was locked, so the engine
# preserves ``${profile.X.Y}`` semantics by adding a parallel top-level
# ``profiles:`` block to ``profiles.yml`` keyed by target. See
# :class:`Profiles` and :func:`resolve_secret_ref`. Destination connection
# rows and source secret blocks live in separate top-level sections of the
# same file; neither contaminates the other.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from detx.engine.discovery import PROJECT_FILE
from detx.secrets import (
    SecretResolutionError,
    resolve_secret_url,
)
from detx.types import (
    Config,
    ConnectorManifest,
    ParamSpec,
    ParamType,
    PipelineConfig,
    SecretRef,
)

# Environment variables that contribute a param value use this prefix, so the
# engine never has to guess whether an arbitrary env var is meant as config.
# ``SIMPLE_E_PARAM_PAGE_SIZE`` sets the ``page_size`` param (docs/03 §6 layer 4).
_ENV_PARAM_PREFIX = "SIMPLE_E_PARAM_"


class ConfigError(Exception):
    """A config layer could not be parsed, or a value failed type-checking.

    Raised for: an unparseable ``detx_project.yml`` / ``profiles.yml`` /
    ``configs/*.yml``, an unknown target, a required param with no value, a
    param value that will not coerce to its declared
    :class:`~detx.types.ParamType`, or an unresolvable secret ref. The runner
    converts it into a ``FAILED`` :class:`~detx.types.RunResult`.
    """


# ---------------------------------------------------------------------------
# Project + profiles file parsing (docs/06)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProjectConfig:
    """The parsed ``detx_project.yml`` — docs/06 §detx_project.yml.

    Stage 8.B trimmed this file: ``default_destination`` and ``default_target``
    moved to ``profiles.yml`` (each destination carries its own
    ``default_target``); the runtime unit is a *config* (``configs/<name>.yml``)
    so the project file no longer needs to point at a default destination. The
    project file now carries identity + ``vars:`` defaults + the directories
    discovery walks.
    """

    name: str
    version: str = "0.1.0"
    source_paths: tuple[str, ...] = ("sources",)
    destination_paths: tuple[str, ...] = ("destinations",)
    config_paths: tuple[str, ...] = ("configs",)
    vars: Mapping[str, Any] = ()  # type: ignore[assignment]
    working_dir: str = ".detx"
    root: Path = Path()

    @classmethod
    def load(cls, project_root: Path) -> ProjectConfig:
        """Parse ``<project_root>/detx_project.yml`` — docs/06.

        A missing file or a non-mapping document raises :class:`ConfigError`;
        every key falls back to its documented default when absent (docs/06
        §Full schema).
        """
        path = project_root / PROJECT_FILE
        if not path.is_file():
            raise ConfigError(f"project config not found: {path}")
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigError(f"{path} must parse to a mapping")

        # # NOTE: stage 8.B replaced the single ``connector_paths`` with split
        # ``source_paths`` + ``destination_paths`` + ``config_paths`` — one
        # list per kind. The split mirrors the project layout split. The legacy
        # ``connector_paths`` key, if present, is read once as a fallback for
        # both source and destination search lists (so a pre-8.B project file
        # still parses), but the canonical form is the split.
        legacy = raw.get("connector_paths")
        legacy_tuple: tuple[str, ...] | None = (
            tuple(str(p) for p in legacy) if legacy else None
        )

        def _paths(key: str, default: tuple[str, ...]) -> tuple[str, ...]:
            value = raw.get(key)
            if value is None:
                return legacy_tuple if legacy_tuple is not None else default
            return tuple(str(p) for p in value)

        return cls(
            name=str(raw.get("name", project_root.name)),
            version=str(raw.get("version", "0.1.0")),
            source_paths=_paths("source_paths", ("sources",)),
            destination_paths=_paths("destination_paths", ("destinations",)),
            config_paths=_paths("config_paths", ("configs",)),
            vars=dict(raw.get("vars") or {}),
            working_dir=str(raw.get("working_dir", ".detx")),
            root=project_root,
        )


@dataclass(frozen=True)
class DestinationTargets:
    """The parsed ``profiles.yml[<destination>]`` block — docs/06 post-8.B.

    Each destination registered in ``profiles.yml`` carries a
    ``default_target`` and a ``targets`` mapping of target name → connection
    params. The :class:`PipelineConfig` names which target a run uses; missing
    target ⇒ this block's ``default_target``.
    """

    destination: str
    default_target: str | None
    targets: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class Profiles:
    """The parsed ``profiles.yml`` — docs/06 §profiles.yml (post-8.B shape).

    Stage 8.B made ``profiles.yml`` destination-keyed (dbt-outputs style):
    each top-level destination name maps to its ``default_target`` +
    ``targets`` map of connection params. A parallel top-level ``profiles:``
    block, keyed by target name, carries source-secret rows so
    ``${profile.<block>.<key>}`` still resolves under the locked two-resolver-
    forms contract (docs/03 §2.5).

    Stage 8e added a top-level ``threads:`` integer — the project-wide
    pipeline-level concurrency budget (default 1, sequential). It is read
    by :func:`~detx.engine.run_tag` to size the worker pool; the per-
    destination cap declared by each destination's
    ``@destination.max_concurrent_writes`` hook narrows it further. Mirrors
    dbt's ``threads:`` knob.

    ``profiles.yml`` is optional: a project with only default-driven config
    (DuckDB's ``path`` defaults, no secrets) runs without one. :meth:`load`
    returns an empty :class:`Profiles` when the file is absent.

    Example shape (docs/06 post-8.B + stage 8e)::

        threads: 4                    # NEW (stage 8e); default 1 if omitted

        duckdb:                       # destination-keyed
          default_target: dev
          targets:
            dev:  { path: ".detx/warehouse.duckdb" }
            prod: { path: "/var/data/detx/warehouse.duckdb" }

        profiles:                     # source-secret-keyed, per-target
          dev:
            shiphero:
              api_token: ${env.SHIPHERO_TOKEN_DEV}
          prod:
            shiphero:
              api_token: ${env.SHIPHERO_TOKEN_PROD}
    """

    destinations: Mapping[str, DestinationTargets] = ()  # type: ignore[assignment]
    secret_profiles: Mapping[str, Mapping[str, Mapping[str, Any]]] = ()  # type: ignore[assignment]
    threads: int = 1
    path: Path | None = None

    # # NOTE: ``destinations`` keys are destination connector names; for each
    # one ``default_target`` and ``targets`` come straight from the file.
    # ``secret_profiles`` carries the parallel top-level ``profiles:`` block:
    # ``secret_profiles[<target>][<block_name>][<key>]`` is what
    # ``${profile.<block_name>.<key>}`` reads (after the engine picks the
    # active target from the config). The two layers are deliberately
    # separate top-level keys so a destination credential row and a source
    # secret block cannot collide.

    @classmethod
    def load(cls, project_root: Path) -> Profiles:
        """Parse ``<project_root>/profiles.yml`` — docs/06 post-8.B + stage 8e.

        Absent file ⇒ empty :class:`Profiles` (a valid state, ``threads=1``).
        A present-but-unparseable file raises :class:`ConfigError`.

        Top-level keys: each destination connector name (with ``targets:`` /
        ``default_target:``); the optional ``profiles:`` block; the optional
        ``threads:`` integer (stage 8e). Any other top-level key is rejected
        only when it doesn't fit the destination-block shape (which itself
        rejects scalars) — so a typo'd top-level key still surfaces clearly.

        # NOTE: ``threads`` is checked BEFORE the destination-block branch
        # because the existing branch's "value must be a dict" rule would
        # reject a scalar (``threads: 4``) with the wrong message. Putting
        # the threads recognition first keeps the per-destination errors
        # untouched and the threads error precise.
        """
        path = project_root / "profiles.yml"
        if not path.is_file():
            return cls(destinations={}, secret_profiles={}, threads=1, path=None)
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigError(f"{path} must parse to a mapping")

        destinations: dict[str, DestinationTargets] = {}
        secret_profiles: dict[str, dict[str, dict[str, Any]]] = {}
        threads = 1
        for key, value in raw.items():
            if key == "threads":
                # Validate it's a positive integer — stage 8e (pipeline
                # parallelism budget). dbt's ``threads:`` accepts the same
                # shape; we mirror.
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ConfigError(
                        f"{path}: 'threads' must be a positive integer "
                        f"(got {value!r})"
                    )
                if value < 1:
                    raise ConfigError(
                        f"{path}: 'threads' must be >= 1 (got {value})"
                    )
                threads = value
                continue
            if key == "profiles":
                if value is None:
                    continue
                if not isinstance(value, dict):
                    raise ConfigError(
                        f"{path}: top-level 'profiles' must be a mapping of "
                        f"target name → block name → key/value"
                    )
                for tgt_name, tgt_value in value.items():
                    if tgt_value is None:
                        secret_profiles[str(tgt_name)] = {}
                        continue
                    if not isinstance(tgt_value, dict):
                        raise ConfigError(
                            f"{path}: profiles[{tgt_name!r}] must be a mapping"
                        )
                    blocks: dict[str, dict[str, Any]] = {}
                    for block_name, block_value in tgt_value.items():
                        if block_value is None:
                            blocks[str(block_name)] = {}
                            continue
                        if not isinstance(block_value, dict):
                            raise ConfigError(
                                f"{path}: profiles[{tgt_name!r}][{block_name!r}] "
                                f"must be a mapping"
                            )
                        blocks[str(block_name)] = dict(block_value)
                    secret_profiles[str(tgt_name)] = blocks
                continue
            # Otherwise treat the key as a destination connector name.
            if not isinstance(value, dict):
                raise ConfigError(
                    f"{path}: destination block {key!r} must be a mapping with a "
                    f"'targets:' sub-mapping"
                )
            targets_raw = value.get("targets") or {}
            if not isinstance(targets_raw, dict):
                raise ConfigError(
                    f"{path}: destination {key!r} 'targets' must be a mapping of "
                    f"target name → connection params"
                )
            targets: dict[str, Mapping[str, Any]] = {
                str(t): dict(v or {}) for t, v in targets_raw.items()
            }
            default = value.get("default_target")
            destinations[str(key)] = DestinationTargets(
                destination=str(key),
                default_target=None if default is None else str(default),
                targets=targets,
            )

        return cls(
            destinations=destinations,
            secret_profiles=secret_profiles,
            threads=threads,
            path=path,
        )

    def destination(self, name: str) -> DestinationTargets:
        """Return the named destination's ``DestinationTargets`` — docs/06.

        An unknown destination name raises :class:`ConfigError` listing the
        destination blocks the file does define, so a typo'd config
        ``destination:`` fails clearly.
        """
        if name not in self.destinations:
            known = ", ".join(sorted(self.destinations)) or "(none defined)"
            raise ConfigError(
                f"destination {name!r} has no block in profiles.yml; "
                f"known destinations: {known}"
            )
        return self.destinations[name]

    def target_params(self, destination: str, target: str) -> Mapping[str, Any]:
        """Return one destination's named target's connection params — docs/06.

        Raises :class:`ConfigError` if the target is undefined for that
        destination, listing the targets that *are* defined.
        """
        block = self.destination(destination)
        if target not in block.targets:
            known = ", ".join(sorted(block.targets)) or "(none defined)"
            raise ConfigError(
                f"target {target!r} is not defined under destination "
                f"{destination!r} in profiles.yml; known targets: {known}"
            )
        return block.targets[target]


def resolve_target_name(
    requested: str | None, destination: str, profiles: Profiles
) -> str:
    """Decide which destination target a run uses — docs/06 §Target selection.

    Precedence: an explicit ``--target`` / ``run(target_override=)`` value, else
    the destination block's ``default_target`` in ``profiles.yml``, else (when
    the destination has only one target) that single target. With no
    destination block at all the synthetic name ``"default"`` is used so a
    profile-free project still has a target label.
    """
    if requested is not None:
        return requested
    if destination in profiles.destinations:
        block = profiles.destinations[destination]
        if block.default_target is not None:
            return block.default_target
        if len(block.targets) == 1:
            return next(iter(block.targets))
        if not block.targets:
            return "default"
        raise ConfigError(
            f"destination {destination!r} has no default_target in profiles.yml "
            f"and the config omits 'target:'; pick one of: "
            f"{', '.join(sorted(block.targets))}"
        )
    return "default"


# ---------------------------------------------------------------------------
# Param resolution — the layered precedence of docs/03 §6
# ---------------------------------------------------------------------------


def _coerce_param(name: str, value: Any, spec: ParamSpec) -> Any:
    """Coerce ``value`` to ``spec``'s declared :class:`ParamType` — docs/03 §6.

    docs/03 §6: "the engine ... type-checks every value against its
    ``ParamSpec``". A value already of the right Python type passes through; a
    string from an env var or YAML scalar is coerced (``"50"`` → ``50`` for an
    ``int`` param). A value that cannot coerce raises :class:`ConfigError`
    naming the param — never a silently mis-typed config.
    """
    if value is None:
        return None
    try:
        if spec.type is ParamType.INT:
            return int(value)
        if spec.type is ParamType.FLOAT:
            return float(value)
        if spec.type is ParamType.BOOL:
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "on"}:
                return True
            if text in {"false", "0", "no", "off"}:
                return False
            raise ValueError(f"{value!r} is not a boolean")
        return str(value)
    except (ValueError, TypeError) as exc:
        raise ConfigError(
            f"param {name!r} value {value!r} is not a valid {spec.type.value}: {exc}"
        ) from exc


def resolve_params(
    specs: Mapping[str, ParamSpec],
    project_vars: Mapping[str, Any],
    config_params: Mapping[str, Any],
    overrides: Mapping[str, Any],
    *,
    connector_name: str,
) -> dict[str, Any]:
    """Resolve a connector's params through the docs/03 §6 precedence layers.

    Layers, lowest → highest:

    1. ``specs[].default`` — ``register.yaml`` param defaults.
    2. ``project_vars`` — ``detx_project.yml`` ``vars``.
    3. ``config_params`` — the active :class:`PipelineConfig`'s ``params``
       (for a source) or ``destination_params`` + the destination's
       ``profiles.yml`` target connection params (for a destination), already
       narrowed by the caller.
    4. environment variables ``SIMPLE_E_PARAM_<NAME>``.
    5. ``overrides`` — CLI flags / ``run()`` kwargs.

    Every resolved value is type-coerced against its :class:`ParamSpec`
    (:func:`_coerce_param`). A ``required`` param that resolves to nothing on
    every layer raises :class:`ConfigError`. A higher layer may also introduce
    a *param the connector did not declare* (e.g. a destination routing param
    like DuckDB's ``path``); such a value is kept verbatim and uncoerced — the
    engine cannot type-check what no :class:`ParamSpec` describes.
    """
    resolved: dict[str, Any] = {}

    # Layers 1-3 + 5, restricted to declared params (type-checked).
    for pname, spec in specs.items():
        value: Any = spec.default
        if pname in project_vars:
            value = project_vars[pname]
        if pname in config_params:
            value = config_params[pname]
        env_key = f"{_ENV_PARAM_PREFIX}{pname.upper()}"
        if env_key in os.environ:
            value = os.environ[env_key]
        if pname in overrides:
            value = overrides[pname]
        coerced = _coerce_param(pname, value, spec)
        if coerced is None and spec.required:
            raise ConfigError(
                f"connector {connector_name!r}: required param {pname!r} has no "
                f"value and no default (docs/03 §2.4)"
            )
        resolved[pname] = coerced

    # Undeclared params arriving from a higher layer (destination routing params
    # such as DuckDB's `path`/`dataset`, or run() convenience kwargs). Kept
    # verbatim — there is no ParamSpec to coerce against.
    for source in (project_vars, config_params, overrides):
        for pname, value in source.items():
            if pname not in specs:
                resolved[pname] = value

    return resolved


# ---------------------------------------------------------------------------
# Secret resolution — the two resolver forms of docs/03 §2.5 (locked)
# ---------------------------------------------------------------------------


def resolve_secret_ref(
    ref: SecretRef, target_name: str, profiles: Profiles
) -> str:
    """Resolve one ``register.yaml`` secret ref to its value — docs/03 §2.5.

    Three resolver forms exist (docs/03 §2.5 + docs/08 §3, Q3 resolved at
    stage 9a):

    * ``${env.X}`` — reads environment variable ``X``. Universal v1 baseline.
    * ``${profile.X.Y}`` — reads key ``Y`` of the active target's profile
      ``profiles.yml[profiles][<target>][X]`` block (docs/06 post-8.B). The
      ``X`` is a logical block name — typically the source connector name —
      and the engine narrows by the active target the config selected.
    * ``secret://<scheme>/<path>[#<field>]`` — dispatches to a pluggable
      :class:`~detx.secrets.SecretResolver` (docs/08 §3). The resolver itself
      is plugin-loaded — built-in env / profile resolution stays unchanged,
      ``secret://`` is the extensibility surface (stage 9a). The
      :mod:`detx.secrets` package handles the URL grammar and the registry.

    A ref whose env var / profile key is missing raises :class:`ConfigError`
    naming the secret's *logical name* and the *ref form* — never the value,
    and never a fragment that could leak a credential. ``${profile.X.Y}`` also
    supports a nested ``${env.VAR}`` inside the profile value, so a
    ``profiles.yml`` can itself stay free of literal secrets (docs/06).

    # NOTE: stage 9a wires ``secret://`` ONLY at this surface (the
    # ``register.yaml`` ``secrets[].ref`` value). Two other places where a
    # ``secret://`` URL could plausibly appear — INSIDE a profile value
    # (``profiles.yml[profiles][<target>][X][Y]: secret://...``) and as a
    # destination target-param value (``profiles.yml[<dest>].targets[<t>].password:
    # secret://...``) — are NOT wired in 9a. Both are reasonable future-work
    # items; the current behavior is "literal string passthrough" for both,
    # which matches today's ``${env.X}`` nested-resolution scope (only one
    # level deep, only inside ``${profile.X.Y}``).
    """
    inner = ref.ref.strip()
    if inner.startswith(SecretRef.SECRET_URL_PREFIX):
        # ``secret://`` dispatch — stage 9a (docs/08 §3). The URL grammar +
        # resolver lookup live in :mod:`detx.secrets`; we only translate the
        # plugin layer's exception into the engine's :class:`ConfigError`
        # so the runner's existing error-handling stays uniform.
        try:
            return resolve_secret_url(inner)
        except SecretResolutionError as exc:
            raise ConfigError(
                f"secret {ref.name!r}: {exc}"
            ) from exc

    if inner.startswith(SecretRef.ENV_PREFIX):
        var = inner[len(SecretRef.ENV_PREFIX) : -1].strip()
        if var not in os.environ:
            raise ConfigError(
                f"secret {ref.name!r}: environment variable {var!r} "
                f"(referenced by {ref.ref}) is not set"
            )
        return os.environ[var]

    # ${profile.X.Y}
    path = inner[len(SecretRef.PROFILE_PREFIX) : -1].strip()
    parts = path.split(".")
    if len(parts) != 2:
        raise ConfigError(
            f"secret {ref.name!r}: ref {ref.ref} must be ${{profile.<block>.<key>}}"
        )
    block_name, key = parts
    target_block = profiles.secret_profiles.get(target_name) or {}
    block = target_block.get(block_name)
    if not isinstance(block, Mapping) or key not in block:
        raise ConfigError(
            f"secret {ref.name!r}: profile key {block_name}.{key} "
            f"(referenced by {ref.ref}) is not present in profiles.yml's "
            f"profiles.{target_name} block"
        )
    value = block[key]
    # A profile value may itself be a nested ${env.VAR} so profiles.yml stays
    # literal-secret-free (docs/06). Resolve one level of indirection.
    text = str(value)
    if text.startswith(SecretRef.ENV_PREFIX) and text.endswith("}"):
        var = text[len(SecretRef.ENV_PREFIX) : -1].strip()
        if var not in os.environ:
            raise ConfigError(
                f"secret {ref.name!r}: profile {block_name}.{key} points at "
                f"environment variable {var!r}, which is not set"
            )
        return os.environ[var]
    return text


def resolve_secrets(
    manifest: ConnectorManifest, target_name: str, profiles: Profiles
) -> dict[str, str]:
    """Resolve every ``register.yaml`` secret of a connector — docs/03 §2.5, §6.

    Returns the logical-name → value map handed to :class:`Config` as its
    ``secrets``. Connector code reads it via ``config.secrets["api_token"]``
    (docs/03 §3). Each ref goes through :func:`resolve_secret_ref`; a missing
    one fails the run with a non-leaking message.
    """
    return {
        ref.name: resolve_secret_ref(ref, target_name, profiles)
        for ref in manifest.secrets
    }


# ---------------------------------------------------------------------------
# The connector Config builder (post-8.B)
# ---------------------------------------------------------------------------


def build_source_config(
    manifest: ConnectorManifest,
    project: ProjectConfig,
    pipeline: PipelineConfig,
    *,
    target_name: str,
    profiles: Profiles,
    overrides: Mapping[str, Any],
) -> Config:
    """Build the immutable :class:`Config` for a SOURCE connector — docs/03 §3, §6.

    Resolves the source's params through every precedence layer
    (:func:`resolve_params` — ``register.yaml`` defaults → project ``vars`` →
    the pipeline's ``params:`` block → env → ``overrides``) and its secrets
    through the two resolver forms (:func:`resolve_secrets`), producing the
    single frozen :class:`Config` object the source body receives — params
    readable as attributes, secrets by subscript (docs/03 §3).
    """
    params = resolve_params(
        manifest.params,
        project.vars,
        pipeline.params,
        overrides,
        connector_name=manifest.name,
    )
    secrets = resolve_secrets(manifest, target_name, profiles)
    return Config(params=params, secrets=secrets)


def build_destination_config(
    manifest: ConnectorManifest,
    project: ProjectConfig,
    pipeline: PipelineConfig,
    *,
    target_name: str,
    profiles: Profiles,
    overrides: Mapping[str, Any],
) -> Config:
    """Build the immutable :class:`Config` for a DESTINATION connector — docs/03 §3.

    Layered precedence (lowest → highest):

    1. ``register.yaml`` ``params[].default`` — destination's own defaults.
    2. ``detx_project.yml`` ``vars`` — project-wide overrides.
    3. ``profiles.yml[<destination>].targets[<target>]`` — the destination's
       connection params for this target (docs/06 post-8.B).
    4. ``PipelineConfig.destination_params`` — per-config overrides (docs/12).
    5. environment variables ``SIMPLE_E_PARAM_<NAME>``.
    6. ``overrides`` — CLI ``--destination-param`` / kwargs (highest).

    Layers 3 + 4 are folded into the ``config_params`` mapping passed to
    :func:`resolve_params`; the precedence within them is preserved by merging
    in order (later wins).
    """
    target_params: dict[str, Any] = {}
    if manifest.name in profiles.destinations and target_name in (
        profiles.destinations[manifest.name].targets
    ):
        target_params.update(profiles.destinations[manifest.name].targets[target_name])
    target_params.update(pipeline.destination_params)
    params = resolve_params(
        manifest.params,
        project.vars,
        target_params,
        overrides,
        connector_name=manifest.name,
    )
    # # NOTE: destinations almost never declare secrets in v1; if a future
    # destination does, the same secret-resolution chain as for sources
    # applies (the active target's profiles.<target> block).
    secrets = resolve_secrets(manifest, target_name, profiles)
    return Config(params=params, secrets=secrets)
