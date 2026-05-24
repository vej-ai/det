"""Incremental-state inspection + reset for ``det state``.

det keeps incremental state in the destination's ``_det_state`` table
(docs/05 §5). The CLI never owns state — it borrows the destination's own
hooks:

* **list** — opens the destination via its ``@destination.open`` hook and calls
  the ``@destination.read_state`` hook. Fully abstract: any Tier-A destination
  works, the CLI never touches SQL.
* **reset** — opens the destination the same way, then issues a targeted
  ``DELETE FROM _det_state``. This is the one place the CLI reaches past
  the hook contract.

  # NOTE: there is no ``delete_state`` / ``reset_state`` hook in the
  # destination contract (docs/03 §3.4 / docs/05 §1) — the engine never needed
  # one. Adding one is an engine change, and the task says the CLI is a thin
  # shell with NO new engine logic. So ``reset`` does the cleanest thing
  # available: it executes a parameterized DELETE on the connection the
  # destination's ``open`` hook returns. That assumes a SQL-ish destination
  # exposing a ``conn.conn.execute(sql, params)`` cursor and a
  # ``_det_state`` table — true for DuckDB, the only Tier-A destination
  # shipped in v1 (docs/05 §2). A non-SQL destination would need the engine
  # hook; reset fails cleanly (caught by the CLI) rather than silently. This
  # is an accepted v1 limitation, flagged here rather than hidden.

This module resolves the destination through the engine's *public* discovery +
config surface — it never re-implements discovery.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.engine import config as cfg
from det.engine import discovery as disc
from det.types import Config, StateRecord

# The engine-owned state table name (docs/05 §5.1). Mirrors the constant the
# DuckDB destination defines privately — repeated here because ``reset`` issues
# the DELETE itself and there is no public accessor for it.
_STATE_TABLE = "_det_state"


class StateError(Exception):
    """A state operation could not complete — surfaced cleanly by the CLI."""


@dataclass
class _ResolvedDestination:
    """The destination hooks + an open connection for a state operation."""

    name: str
    hooks: Mapping[str, Any]
    conn: Any


def _resolve_destination(
    connector: str,
    *,
    project_dir: str | Path | None,
    target: str | None,
    destination_params: Mapping[str, Any] | None,
) -> tuple[Path, _ResolvedDestination]:
    """Resolve + open the destination a source connector binds to.

    Runs the same discovery + config path the engine's ``run`` does (stages
    1-3, DISCOVER → RESOLVE → INIT DEST), but stops once the destination is
    open: a state op needs a live connection, not a run. Returns the project
    root and the open destination handle. The caller must ``close`` it.
    """
    project_root = disc.find_project_root(project_dir)
    project = cfg.ProjectConfig.load(project_root)
    profiles = cfg.Profiles.load(project_root)
    target_name = cfg.resolve_target_name(target, project, profiles)
    target_block = profiles.target(target_name) if profiles.targets else {}

    source = disc.resolve_connector(
        connector, project_root, list(project.connector_paths)
    )
    if source.manifest.kind.value != "source":
        raise StateError(
            f"connector {connector!r} is a {source.manifest.kind.value}, not a "
            f"source; state is scoped per source connector"
        )
    destination_name = cfg.resolve_destination_name(source.manifest, project)
    dest = disc.resolve_connector(
        destination_name, project_root, list(project.connector_paths)
    )

    routing = dict(source.manifest.destination.routing) if source.manifest.destination else {}
    dest_config = cfg.build_config(
        dest.manifest,
        project,
        target_block,
        section="destinations",
        overrides={**routing, **dict(destination_params or {})},
    )

    hooks: dict[str, Any] = {}
    for hook_name in ("open", "close", "read_state"):
        hook = dest.registry.hook(hook_name)
        if hook is None:
            raise StateError(
                f"destination {destination_name!r} is missing the @destination."
                f"{hook_name} hook required for state operations"
            )
        hooks[hook_name] = hook.func

    conn = hooks["open"](Config(params=dict(dest_config.params)))
    return project_root, _ResolvedDestination(
        name=destination_name, hooks=hooks, conn=conn
    )


def list_state(
    connector: str,
    *,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> list[StateRecord]:
    """Return the ``_det_state`` rows for one source connector.

    Opens the bound destination and calls its ``read_state`` hook — the same
    call the engine makes at run start (docs/05 §1). The connection is always
    closed. An empty list means the connector has never committed state (or
    its state was reset).
    """
    _, dest = _resolve_destination(
        connector,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        records = dest.hooks["read_state"](dest.conn, connector)
        return list(records)
    finally:
        dest.hooks["close"](dest.conn)


def reset_state(
    connector: str,
    *,
    stream: str | None = None,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> int:
    """Clear ``_det_state`` rows so the next run is a full re-extract.

    Deletes the ``(connector)`` rows — or the single ``(connector, stream)``
    row when ``stream`` is given — from the destination's ``_det_state``
    table. The next run then finds no prior cursor and seeds from each stream's
    ``initial_value`` (docs/03 §3.2), exactly as a first run does. Loaded data
    is untouched — this is the surgical alternative to ``--full-refresh``.

    Returns the number of rows deleted. See the module ``# NOTE:`` for why this
    issues a DELETE directly rather than going through a destination hook.
    """
    _, dest = _resolve_destination(
        connector,
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
                f"(DuckDB in v1). Use `det run --full-refresh` instead."
            )
        # The _det_state table is created lazily on first read. Calling
        # the destination's own read_state hook here both creates it (so the
        # DELETE below never hits an "unknown table") and tells us how many
        # rows the reset will clear — without the CLI assuming the table
        # already exists.
        prior = dest.hooks["read_state"](dest.conn, connector)
        if stream is not None:
            count_before = sum(1 for r in prior if r.stream == stream)
        else:
            count_before = len(list(prior))
        params: list[Any] = [connector]
        sql = f"DELETE FROM {_STATE_TABLE} WHERE connector = ?"
        if stream is not None:
            sql += " AND stream = ?"
            params.append(stream)
        # DuckDB autocommits a bare DML statement; the DELETE is durable once
        # close() runs. No explicit COMMIT is issued (none is needed).
        raw.execute(sql, params)
        return count_before
    finally:
        dest.hooks["close"](dest.conn)
