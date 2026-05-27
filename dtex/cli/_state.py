# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Incremental-state inspection + reset for ``dtex state``.

dtex keeps incremental state in the destination's ``_dtex_state`` table
(docs/05 §5). The CLI never owns state — it borrows the destination's own
hooks:

* **list** — opens the destination via its ``@destination.open`` hook and calls
  the ``@destination.read_state`` hook. Fully abstract: any Tier-A destination
  works, the CLI never touches SQL.
* **reset** — opens the destination the same way, then issues a targeted
  ``DELETE FROM _dtex_state``. This is the one place the CLI reaches past
  the hook contract.

  # NOTE: there is no ``delete_state`` / ``reset_state`` hook in the
  # destination contract (docs/03 §3.4 / docs/05 §1) — the engine never needed
  # one. Adding one is an engine change, and the task says the CLI is a thin
  # shell with NO new engine logic. So ``reset`` does the cleanest thing
  # available: it executes a parameterized DELETE on the connection the
  # destination's ``open`` hook returns. That assumes a SQL-ish destination
  # exposing a ``conn.conn.execute(sql, params)`` cursor and a
  # ``_dtex_state`` table — true for DuckDB, the only Tier-A destination
  # shipped in v1 (docs/05 §2). A non-SQL destination would need the engine
  # hook; reset fails cleanly (caught by the CLI) rather than silently. This
  # is an accepted v1 limitation, flagged here rather than hidden.

Stage 8.B made *configs* the runtime unit; state operations now take a config
NAME instead of a source name. The config resolves the (source, destination,
target) triple — but state rows themselves are still keyed by *source* name
in ``_dtex_state`` (a property of where data lives, not how it was extracted).
A re-run under a different config that names the same source resumes off
the same rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dtex.engine import config as cfg
from dtex.engine import configs as cfgs
from dtex.engine import discovery as disc
from dtex.types import Config, StateRecord

# The engine-owned state table name (docs/05 §5.1).
_STATE_TABLE = "_dtex_state"


class StateError(Exception):
    """A state operation could not complete — surfaced cleanly by the CLI."""


@dataclass
class _ResolvedDestination:
    """The destination hooks + an open connection for a state operation."""

    source_name: str
    name: str
    hooks: Mapping[str, Any]
    conn: Any


def _resolve_destination(
    config_name: str,
    *,
    project_dir: str | Path | None,
    target: str | None,
    destination_params: Mapping[str, Any] | None,
) -> tuple[Path, _ResolvedDestination]:
    """Resolve + open the destination a config binds to.

    Runs the same discovery + config path the engine's ``run`` does (stages
    1-3, DISCOVER → RESOLVE → INIT DEST), but stops once the destination is
    open: a state op needs a live connection, not a run. Returns the project
    root and the open destination handle. The caller must ``close`` it.
    """
    project_root = disc.find_project_root(project_dir)
    project = cfg.ProjectConfig.load(project_root)
    profiles = cfg.Profiles.load(project_root)
    pipeline = cfgs.load_config(
        config_name, project_root, list(project.config_paths)
    )
    target_name = cfg.resolve_target_name(
        target if target is not None else pipeline.target,
        pipeline.destination,
        profiles,
    )

    source = disc.resolve_source(
        pipeline.source, project_root, list(project.source_paths)
    )
    dest = disc.resolve_destination(
        pipeline.destination, project_root, list(project.destination_paths)
    )

    dest_config = cfg.build_destination_config(
        dest.manifest,
        project,
        pipeline,
        target_name=target_name,
        profiles=profiles,
        overrides=dict(destination_params or {}),
    )

    hooks: dict[str, Any] = {}
    for hook_name in ("open", "close", "read_state"):
        hook = dest.registry.hook(hook_name)
        if hook is None:
            raise StateError(
                f"destination {pipeline.destination!r} is missing the "
                f"@destination.{hook_name} hook required for state operations"
            )
        hooks[hook_name] = hook.func

    conn = hooks["open"](Config(params=dict(dest_config.params)))
    return project_root, _ResolvedDestination(
        source_name=source.manifest.name,
        name=pipeline.destination,
        hooks=hooks,
        conn=conn,
    )


def list_state(
    config_name: str,
    *,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> list[StateRecord]:
    """Return the ``_dtex_state`` rows for one config's source.

    Opens the bound destination and calls its ``read_state`` hook — the same
    call the engine makes at run start (docs/05 §1). The connection is always
    closed. An empty list means the source has never committed state (or its
    state was reset).
    """
    _, dest = _resolve_destination(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        records = dest.hooks["read_state"](dest.conn, dest.source_name)
        return list(records)
    finally:
        dest.hooks["close"](dest.conn)


def reset_state(
    config_name: str,
    *,
    stream: str | None = None,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> int:
    """Clear ``_dtex_state`` rows so the next run of this config is a full re-extract.

    Deletes the ``(source)`` rows — or the single ``(source, stream)`` row
    when ``stream`` is given — from the destination's ``_dtex_state`` table.
    The next run then finds no prior cursor and seeds from each stream's
    ``initial_value`` (docs/03 §3.2), exactly as a first run does. Loaded data
    is untouched — this is the surgical alternative to ``--full-refresh``.

    Returns the number of rows deleted. See the module ``# NOTE:`` for why
    this issues a DELETE directly rather than going through a destination
    hook.
    """
    _, dest = _resolve_destination(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        raw = getattr(dest.conn, "conn", None)
        if raw is None or not hasattr(raw, "execute"):
            raise StateError(
                f"destination {dest.name!r} does not expose a SQL connection; "
                f"`state reset` supports SQL-backed Tier-A destinations only "
                f"(DuckDB in v1). Use `dtex run --full-refresh` instead."
            )
        prior = dest.hooks["read_state"](dest.conn, dest.source_name)
        if stream is not None:
            count_before = sum(1 for r in prior if r.stream == stream)
        else:
            count_before = len(list(prior))
        params: list[Any] = [dest.source_name]
        sql = f"DELETE FROM {_STATE_TABLE} WHERE connector = ?"
        if stream is not None:
            sql += " AND stream = ?"
            params.append(stream)
        raw.execute(sql, params)
        return count_before
    finally:
        dest.hooks["close"](dest.conn)
