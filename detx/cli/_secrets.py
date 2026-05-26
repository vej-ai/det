# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""``detx secrets test`` — verify that every declared secret reference resolves.

Stage 9a CLI surface (docs/07). The command resolves every
``register.yaml`` ``secrets[].ref`` for each config the user selects (or
every config in the project) and prints one line per reference with its
resolution status — ``✓`` for success, ``✗`` plus the error otherwise.

The whole point of the command is "tell me whether my creds are wired up
right" without leaking what those creds *are*: this module NEVER prints
a resolved value, only the reference URL string (which is what the
operator wrote in ``profiles.yml`` / ``register.yaml`` and is safe to
echo).

# NOTE: design decision — ``detx secrets test`` is the place where we
# DELIBERATELY resolve every reference up front, even though normal
# ``detx run`` resolution is lazy. The whole purpose of the command is the
# verification round-trip, so eager resolution is correct. If a resolver
# is genuinely expensive (a GCP SDK init), the user pays for it ONCE per
# scheme per invocation (the per-process instance cache in
# :mod:`detx.secrets`). That's the same cost a real run would pay.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from detx.cli._discovery import discover_all_configs
from detx.engine import config as cfg
from detx.engine import discovery as disc
from detx.secrets import load_project_plugins
from detx.types import PipelineConfig, SecretRef


@dataclass(frozen=True)
class ReferenceCheck:
    """One per-reference verification result for ``detx secrets test``.

    * ``config`` — the pipeline config the reference came from.
    * ``connector`` — which side of the pipeline (``source`` or ``destination``).
    * ``connector_name`` — the actual source/destination connector name.
    * ``secret_name`` — the logical name from ``register.yaml`` ``secrets[].name``.
    * ``ref`` — the reference STRING (the value the operator wrote — safe to
      print). NEVER the resolved value.
    * ``ok`` — whether resolution succeeded.
    * ``error`` — the single-line failure summary on ``ok=False``; ``None``
      otherwise. Truncated to the exception's own message so a stack trace
      is not surfaced in CLI output.
    """

    config: str
    connector: str
    connector_name: str
    secret_name: str
    ref: str
    ok: bool
    error: str | None = None


def _resolve_one(
    ref: SecretRef, target_name: str, profiles: cfg.Profiles
) -> tuple[bool, str | None]:
    """Resolve one secret ref and return ``(ok, error_or_none)``.

    Wraps :func:`detx.engine.config.resolve_secret_ref` so every exception
    class — :class:`ConfigError` (env var unset, unknown profile key) and
    anything else the resolver layer surfaces — becomes a ``(False, msg)``
    pair instead of propagating. The resolved value is intentionally
    DROPPED here — we only need to know whether resolution succeeded.

    # NOTE: ``BLE001`` lint waiver — the whole point of the command is to
    # surface ANY exception class as a per-reference failure. A future
    # resolver could raise an SDK-specific class we can't enumerate, and
    # forgetting to wrap it would leak the traceback to the user's screen.
    """
    try:
        cfg.resolve_secret_ref(ref, target_name, profiles)
    except Exception as exc:  # noqa: BLE001 — see NOTE
        # The exception's str() is the operator-facing message
        # (resolve_secret_ref / ConfigError never inline the resolved value
        # because resolution failed BEFORE a value existed).
        return False, str(exc)
    return True, None


def check_config(
    pipeline: PipelineConfig,
    project_root: Path,
    project: cfg.ProjectConfig,
    profiles: cfg.Profiles,
    *,
    target_override: str | None = None,
) -> list[ReferenceCheck]:
    """Resolve every reference for one config + return the per-ref results.

    Drives both the source's and the destination's ``register.yaml``
    ``secrets[]`` lists through :func:`_resolve_one`. The active target
    is decided exactly as the runner would: an explicit ``target_override``
    wins, else the config's ``target:``, else the destination's
    ``default_target`` (:func:`detx.engine.config.resolve_target_name`).

    A config whose source or destination cannot even be RESOLVED (a typo'd
    connector name, a missing manifest) returns a single failure record
    naming the discovery problem — so the user sees ✗ at the config
    level instead of a silent skip.

    # NOTE: design decision — ``--target prod`` against a config whose
    # destination has no ``prod`` target is a HARD failure (one ✗ row
    # listing the targets that ARE defined). The alternative ("skip
    # silently") would mask exactly the misconfiguration the command exists
    # to catch.
    """
    checks: list[ReferenceCheck] = []
    # Resolve target name first — failure here is config-level, not per-ref.
    try:
        target_name = cfg.resolve_target_name(
            target_override if target_override is not None else pipeline.target,
            pipeline.destination,
            profiles,
        )
    except cfg.ConfigError as exc:
        checks.append(
            ReferenceCheck(
                config=pipeline.name,
                connector="(target)",
                connector_name=pipeline.destination,
                secret_name="(target)",
                ref="(target resolution)",
                ok=False,
                error=str(exc),
            )
        )
        return checks

    # Source side
    try:
        source = disc.resolve_source(
            pipeline.source, project_root, list(project.source_paths)
        )
    except (disc.DiscoveryError, cfg.ConfigError) as exc:
        checks.append(
            ReferenceCheck(
                config=pipeline.name,
                connector="source",
                connector_name=pipeline.source,
                secret_name="(discovery)",
                ref="(source discovery)",
                ok=False,
                error=str(exc),
            )
        )
    else:
        for ref in source.manifest.secrets:
            ok, error = _resolve_one(ref, target_name, profiles)
            checks.append(
                ReferenceCheck(
                    config=pipeline.name,
                    connector="source",
                    connector_name=pipeline.source,
                    secret_name=ref.name,
                    ref=ref.ref,
                    ok=ok,
                    error=error,
                )
            )

    # Destination side
    try:
        dest = disc.resolve_destination(
            pipeline.destination, project_root, list(project.destination_paths)
        )
    except (disc.DiscoveryError, cfg.ConfigError) as exc:
        checks.append(
            ReferenceCheck(
                config=pipeline.name,
                connector="destination",
                connector_name=pipeline.destination,
                secret_name="(discovery)",
                ref="(destination discovery)",
                ok=False,
                error=str(exc),
            )
        )
    else:
        for ref in dest.manifest.secrets:
            ok, error = _resolve_one(ref, target_name, profiles)
            checks.append(
                ReferenceCheck(
                    config=pipeline.name,
                    connector="destination",
                    connector_name=pipeline.destination,
                    secret_name=ref.name,
                    ref=ref.ref,
                    ok=ok,
                    error=error,
                )
            )
    return checks


def check_project(
    project_dir: Path | str | None = None,
    *,
    config_name: str | None = None,
    target_override: str | None = None,
) -> tuple[list[ReferenceCheck], int, int]:
    """Drive ``detx secrets test`` end to end — returns (checks, ok_count, fail_count).

    * ``project_dir`` — project root, or any dir under it; walked up to
      find ``detx_project.yml``.
    * ``config_name`` — when given, only this config's references are
      checked (the ``-p`` / ``--conf`` flag). Otherwise every discovered
      config is checked.
    * ``target_override`` — overrides the config's ``target:`` (the
      ``--target`` flag). Applies uniformly to every checked config.

    The project's ``detx_plugins.py`` is imported BEFORE any resolution,
    matching ``detx run``'s behavior so a project-local custom
    ``secret://`` scheme is registered before we try to look it up.

    Returns the per-reference list and the success/failure counts; the
    caller (the click command) formats output and decides exit code.
    """
    project_root = disc.find_project_root(project_dir)
    # Load project-local resolver registrations BEFORE any resolution. The
    # same call ``run()`` makes; without it, a ``secret://my-scheme/...``
    # registered in detx_plugins.py would resolve as "unknown scheme".
    load_project_plugins(project_root)

    project = cfg.ProjectConfig.load(project_root)
    profiles = cfg.Profiles.load(project_root)

    pipelines: Iterable[PipelineConfig]
    if config_name is not None:
        from detx.engine import configs as cfgs

        pipelines = [
            cfgs.load_config(
                config_name, project_root, list(project.config_paths)
            )
        ]
    else:
        pipelines = discover_all_configs(project_root, list(project.config_paths))

    all_checks: list[ReferenceCheck] = []
    for pipeline in pipelines:
        all_checks.extend(
            check_config(
                pipeline,
                project_root,
                project,
                profiles,
                target_override=target_override,
            )
        )
    ok_count = sum(1 for c in all_checks if c.ok)
    fail_count = sum(1 for c in all_checks if not c.ok)
    return all_checks, ok_count, fail_count
