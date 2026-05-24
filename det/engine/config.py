"""Config resolution — stage 2 (RESOLVE) of the run lifecycle (docs/02, docs/03 §6).

This module merges every config layer into the frozen
:class:`~det.types.RunConfig` the runner carries and the immutable
:class:`~det.types.Config` objects connector code receives. After RESOLVE,
nothing reads ambient config (docs/02).

**Precedence** (docs/03 §6, lowest → highest):

1. ``register.yaml`` ``params[].default`` — the connector's own defaults.
2. ``det_project.yml`` ``vars`` — project-wide overrides.
3. ``profiles.yml`` active target — per-environment overrides.
4. environment variables — ``SIMPLE_E_PARAM_<NAME>``.
5. CLI flags / ``run()`` kwargs — per-invocation, highest.

**Secrets** are resolved from a connector's ``register.yaml`` ``secrets[]``
list. Exactly two resolver forms exist (a locked decision, docs/03 §2.5):
``${env.X}`` reads ``os.environ['X']``; ``${profile.X.Y}`` reads key ``Y`` of
the active target's ``profiles.X`` block. Secret *values* are never logged.

**Destination resolution** — a source's ``register.yaml`` ``destination``
binding names the destination connector; absent, the project's
``default_destination`` applies (docs/03 §2.3, docs/06).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from det.engine.discovery import PROJECT_FILE
from det.types import (
    Config,
    ConnectorManifest,
    ParamSpec,
    ParamType,
    SecretRef,
)

# Environment variables that contribute a param value use this prefix, so the
# engine never has to guess whether an arbitrary env var is meant as config.
# ``SIMPLE_E_PARAM_PAGE_SIZE`` sets the ``page_size`` param (docs/03 §6 layer 4).
_ENV_PARAM_PREFIX = "SIMPLE_E_PARAM_"


class ConfigError(Exception):
    """A config layer could not be parsed, or a value failed type-checking.

    Raised for: an unparseable ``det_project.yml`` / ``profiles.yml``, an
    unknown ``--target``, a required param with no value, a param value that
    will not coerce to its declared :class:`~det.types.ParamType`, or an
    unresolvable secret ref. The runner converts it into a ``FAILED``
    :class:`~det.types.RunResult`.
    """


# ---------------------------------------------------------------------------
# Project + profiles file parsing (docs/06)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProjectConfig:
    """The parsed ``det_project.yml`` — docs/06 §det_project.yml.

    Carries the seven documented keys the engine reads at RESOLVE time. Frozen:
    project config is fixed once the file is read.
    """

    name: str
    version: str = "0.1.0"
    connector_paths: tuple[str, ...] = ("connectors",)
    default_destination: str | None = None
    default_target: str | None = None
    vars: Mapping[str, Any] = ()  # type: ignore[assignment]
    working_dir: str = ".det"
    root: Path = Path()

    @classmethod
    def load(cls, project_root: Path) -> ProjectConfig:
        """Parse ``<project_root>/det_project.yml`` — docs/06.

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
        paths_raw = raw.get("connector_paths") or ["connectors"]
        return cls(
            name=str(raw.get("name", project_root.name)),
            version=str(raw.get("version", "0.1.0")),
            connector_paths=tuple(str(p) for p in paths_raw),
            default_destination=(
                None
                if raw.get("default_destination") is None
                else str(raw["default_destination"])
            ),
            default_target=(
                None if raw.get("default_target") is None else str(raw["default_target"])
            ),
            vars=dict(raw.get("vars") or {}),
            working_dir=str(raw.get("working_dir", ".det")),
            root=project_root,
        )


@dataclass(frozen=True)
class Profiles:
    """The parsed ``profiles.yml`` — docs/06 §profiles.yml.

    ``profiles.yml`` is optional: a project with only default-driven config
    (DuckDB's ``path`` defaults, no secrets) runs without one. :meth:`load`
    returns an empty :class:`Profiles` when the file is absent.
    """

    targets: Mapping[str, Mapping[str, Any]] = ()  # type: ignore[assignment]

    @classmethod
    def load(cls, project_root: Path) -> Profiles:
        """Parse ``<project_root>/profiles.yml`` — docs/06.

        Absent file ⇒ empty profiles (a valid state). A present-but-unparseable
        file raises :class:`ConfigError`.
        """
        path = project_root / "profiles.yml"
        if not path.is_file():
            return cls(targets={})
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"{path} is not valid YAML: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigError(f"{path} must parse to a mapping")
        targets = raw.get("targets") or {}
        if not isinstance(targets, dict):
            raise ConfigError(f"{path}: 'targets' must be a mapping of target name → config")
        return cls(targets={str(k): dict(v or {}) for k, v in targets.items()})

    def target(self, name: str) -> Mapping[str, Any]:
        """Return one named target's config block — docs/06 §Target selection.

        An unknown target name raises :class:`ConfigError` listing the targets
        the file does define, so a typo'd ``--target`` fails clearly.
        """
        if name not in self.targets:
            known = ", ".join(sorted(self.targets)) or "(none defined)"
            raise ConfigError(
                f"target {name!r} is not defined in profiles.yml; known targets: {known}"
            )
        return self.targets[name]


def resolve_target_name(
    requested: str | None, project: ProjectConfig, profiles: Profiles
) -> str:
    """Decide which ``profiles.yml`` target a run uses — docs/06 §Target selection.

    Precedence: an explicit ``run(target=...)`` / ``--target`` value, else
    ``det_project.yml`` ``default_target``, else the first target declared
    in ``profiles.yml``. With no profiles file at all the synthetic name
    ``"default"`` is used so a profile-free project still has a target label.
    """
    if requested is not None:
        return requested
    if project.default_target is not None:
        return project.default_target
    if profiles.targets:
        return next(iter(profiles.targets))
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
    target_block: Mapping[str, Any],
    overrides: Mapping[str, Any],
    *,
    connector_name: str,
) -> dict[str, Any]:
    """Resolve a connector's params through the docs/03 §6 precedence layers.

    Layers, lowest → highest:

    1. ``specs[].default`` — ``register.yaml`` param defaults.
    2. ``project_vars`` — ``det_project.yml`` ``vars``.
    3. ``target_block`` config for this connector — ``profiles.yml`` active
       target (a ``destinations.<name>`` or ``profiles.<name>`` block, already
       narrowed by the caller).
    4. environment variables ``SIMPLE_E_PARAM_<NAME>``.
    5. ``overrides`` — CLI flags / ``run()`` kwargs.

    Every resolved value is type-coerced against its :class:`ParamSpec`
    (:func:`_coerce_param`). A ``required`` param that resolves to nothing on
    every layer raises :class:`ConfigError`. A higher layer may also introduce a
    *param the connector did not declare* (e.g. a destination routing param like
    DuckDB's ``path``); such a value is kept verbatim and uncoerced — the engine
    cannot type-check what no :class:`ParamSpec` describes.
    """
    resolved: dict[str, Any] = {}

    # Layers 1-3 + 5, restricted to declared params (type-checked).
    for pname, spec in specs.items():
        value: Any = spec.default
        if pname in project_vars:
            value = project_vars[pname]
        if pname in target_block:
            value = target_block[pname]
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
    for source in (project_vars, target_block, overrides):
        for pname, value in source.items():
            if pname not in specs:
                resolved[pname] = value

    return resolved


# ---------------------------------------------------------------------------
# Secret resolution — the two resolver forms of docs/03 §2.5 (locked)
# ---------------------------------------------------------------------------


def resolve_secret_ref(ref: SecretRef, target_block: Mapping[str, Any]) -> str:
    """Resolve one ``register.yaml`` secret ref to its value — docs/03 §2.5.

    Exactly two resolver forms exist (locked decision, docs/03 §2.5):

    * ``${env.X}`` — reads environment variable ``X``.
    * ``${profile.X.Y}`` — reads key ``Y`` of the active target's
      ``profiles.X`` block.

    A ref whose env var / profile key is missing raises :class:`ConfigError`
    naming the secret's *logical name* and the *ref form* — never the value, and
    never a fragment that could leak a credential. Both forms support a nested
    ``${env.VAR}`` inside a profile value, so a ``profiles.yml`` can itself stay
    free of literal secrets (docs/06).
    """
    inner = ref.ref.strip()
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
    profiles_block = target_block.get("profiles") or {}
    block = profiles_block.get(block_name)
    if not isinstance(block, Mapping) or key not in block:
        raise ConfigError(
            f"secret {ref.name!r}: profile key {block_name}.{key} "
            f"(referenced by {ref.ref}) is not present in the active target"
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
    manifest: ConnectorManifest, target_block: Mapping[str, Any]
) -> dict[str, str]:
    """Resolve every ``register.yaml`` secret of a connector — docs/03 §2.5, §6.

    Returns the logical-name → value map handed to :class:`Config` as its
    ``secrets``. Connector code reads it via ``config.secrets["api_token"]``
    (docs/03 §3). Each ref goes through :func:`resolve_secret_ref`; a missing
    one fails the run with a non-leaking message.
    """
    return {ref.name: resolve_secret_ref(ref, target_block) for ref in manifest.secrets}


# ---------------------------------------------------------------------------
# The connector Config builder
# ---------------------------------------------------------------------------


def _target_block_for(
    target_block: Mapping[str, Any], section: str, name: str
) -> Mapping[str, Any]:
    """Narrow a ``profiles.yml`` target to one connector's config block — docs/06.

    docs/06: a target carries ``destinations.<name>`` blocks (destination
    credentials/routing) and ``profiles.<name>`` blocks (secret material). A
    *source* connector draws its per-env config from ``profiles.<name>``; a
    *destination* draws its from ``destinations.<name>``. ``section`` selects
    which; an absent block is an empty mapping (a valid state).
    """
    container = target_block.get(section) or {}
    if not isinstance(container, Mapping):
        return {}
    block = container.get(name) or {}
    return block if isinstance(block, Mapping) else {}


def build_config(
    manifest: ConnectorManifest,
    project: ProjectConfig,
    target_block: Mapping[str, Any],
    *,
    section: str,
    overrides: Mapping[str, Any],
) -> Config:
    """Build the immutable :class:`Config` for one connector — docs/03 §3, §6.

    Resolves the connector's params through every precedence layer
    (:func:`resolve_params`) and its secrets through the two resolver forms
    (:func:`resolve_secrets`), producing the single frozen :class:`Config`
    object the connector body receives — params readable as attributes, secrets
    by subscript (docs/03 §3).

    ``section`` is ``"destinations"`` for a destination connector, ``"profiles"``
    for a source — it selects which ``profiles.yml`` sub-block (docs/06) feeds
    layer 3. ``overrides`` is the per-invocation layer (CLI / ``run()`` kwargs).
    """
    connector_block = _target_block_for(target_block, section, manifest.name)
    params = resolve_params(
        manifest.params,
        project.vars,
        connector_block,
        overrides,
        connector_name=manifest.name,
    )
    secrets = resolve_secrets(manifest, target_block)
    return Config(params=params, secrets=secrets)


def resolve_destination_name(
    manifest: ConnectorManifest, project: ProjectConfig
) -> str:
    """Decide which destination a source's streams land in — docs/03 §2.3, docs/06.

    A source's ``register.yaml`` ``destination.connector`` binding wins; absent,
    the project's ``default_destination`` applies. With neither, the run cannot
    proceed — :class:`ConfigError` is raised, because a source with nowhere to
    write is not a runnable configuration.
    """
    if manifest.destination is not None:
        return manifest.destination.connector
    if project.default_destination is not None:
        return project.default_destination
    raise ConfigError(
        f"connector {manifest.name!r} declares no 'destination' binding and the "
        f"project sets no 'default_destination' — nowhere to write (docs/03 §2.3)"
    )
