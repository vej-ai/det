# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Run-history inspection for ``dtex runs`` — docs/09 §4, stage 8a.

``dtex runs list`` queries the destination's ``_dtex_runs`` audit table; ``dtex
runs show`` adds the per-run JSONL log file to the picture. Both reuse the
destination's own ``@destination.open`` / ``@destination.close`` hooks the
same way :mod:`dtex.cli._state` does for ``dtex state list``.

# NOTE: design call — *run records are per-destination*. The audit table
# lives in the destination's storage (docs/09 §4.1: Tier A destinations host
# ``_dtex_runs`` alongside ``_dtex_state``). Different configs in one project
# can target different destinations; a project-wide "list every run" would
# need to open every destination and union the rows, which is not just
# fan-out — it is fan-out with potentially incompatible schemas (BigQuery,
# Postgres, DuckDB at once). The simplest predictable contract is:
#
#   ``dtex runs list -p <config>`` (config name disambiguates the destination)
#
# This mirrors ``dtex state list`` exactly and avoids inventing a multi-
# destination merge story v1 cannot honour. A future ``--destination <name>``
# flag is the natural relaxation; until then `-p <config>` is required.

# NOTE: like :mod:`dtex.cli._state`, this module reaches past the destination
# hook contract — there is no ``read_run_records`` hook (docs/09 §4 only
# specifies the write side). Queries go through ``conn.conn.execute`` on a
# SQL-backed Tier-A connection, which is true for DuckDB (the v1 destination
# with the capability). A non-SQL destination would need a hook; the CLI
# fails cleanly with the limitation flagged here. Accepted v1 tradeoff.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dtex.cli._state import StateError
from dtex.engine import config as cfg
from dtex.engine import configs as cfgs
from dtex.engine import discovery as disc
from dtex.types import Capability, Config, RunStatus

# The engine-owned run-record audit table — docs/09 §4.
_RUNS_TABLE = "_dtex_runs"


@dataclass(frozen=True)
class RunRow:
    """One row from ``_dtex_runs`` — the shape the CLI prints + admin reads."""

    run_id: str
    config: str
    source: str
    destination: str
    target: str
    status: RunStatus
    started_at: datetime | None
    ended_at: datetime | None
    duration_s: float
    rows_loaded: int
    full_refresh: bool
    error_type: str | None
    error_message: str | None
    streams: list[dict[str, Any]]


@dataclass
class _ResolvedDestination:
    """A live destination connection + the config's source name, for queries."""

    project_root: Path
    name: str
    source_name: str
    hooks: Mapping[str, Any]
    capabilities: set[Capability]
    conn: Any


def _resolve(
    config_name: str,
    *,
    project_dir: str | Path | None,
    target: str | None,
    destination_params: Mapping[str, Any] | None,
) -> _ResolvedDestination:
    """Resolve the config → open the destination → return the live handle.

    The same stage-1-through-3 path as :func:`dtex.run`, stopping once the
    destination is open. The caller must ``close`` the connection.
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
    for hook_name in ("capabilities", "open", "close"):
        hook = dest.registry.hook(hook_name)
        if hook is None:
            raise StateError(
                f"destination {pipeline.destination!r} is missing the "
                f"@destination.{hook_name} hook required for runs operations"
            )
        hooks[hook_name] = hook.func
    capabilities = set(hooks["capabilities"]())

    conn = hooks["open"](Config(params=dict(dest_config.params)))
    return _ResolvedDestination(
        project_root=project_root,
        name=pipeline.destination,
        source_name=source.manifest.name,
        hooks=hooks,
        capabilities=capabilities,
        conn=conn,
    )


def _decode_json(value: Any) -> Any:
    """Parse a JSON-text value from DuckDB back into Python."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _row_to_runrow(row: tuple[Any, ...]) -> RunRow:
    """Build a :class:`RunRow` from a ``_dtex_runs`` row in column order.

    Column order matches the ``SELECT`` in :func:`list_runs` / :func:`get_run`.
    """
    streams_raw = _decode_json(row[13])
    if not isinstance(streams_raw, list):
        streams_raw = []
    return RunRow(
        run_id=str(row[0]),
        config=str(row[1]),
        source=str(row[2]),
        destination=str(row[3]),
        target=str(row[4]),
        status=RunStatus.parse(row[5]),
        started_at=row[6] if isinstance(row[6], datetime) else None,
        ended_at=row[7] if isinstance(row[7], datetime) else None,
        duration_s=float(row[8] or 0.0),
        rows_loaded=int(row[9] or 0),
        full_refresh=bool(row[10]),
        error_type=None if row[11] is None else str(row[11]),
        error_message=None if row[12] is None else str(row[12]),
        streams=list(streams_raw),
    )


def _query_runs(conn: Any, *, run_id: str | None, limit: int | None) -> list[RunRow]:
    """SELECT ``_dtex_runs`` rows, optionally filtered to a single ``run_id``."""
    raw = getattr(conn, "conn", None)
    if raw is None or not hasattr(raw, "execute"):
        raise StateError(
            "destination does not expose a SQL connection; `runs` supports "
            "SQL-backed Tier-A destinations only (DuckDB in v1)."
        )

    sql = (
        "SELECT run_id, config, source, destination, target, status, "
        "       started_at, ended_at, duration_s, rows_loaded, full_refresh, "
        "       error_type, error_message, streams_json "
        f"FROM {_RUNS_TABLE} "
    )
    params: list[Any] = []
    if run_id is not None:
        sql += "WHERE run_id = ? "
        params.append(run_id)
    sql += "ORDER BY started_at DESC"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    try:
        rows = raw.execute(sql, params).fetchall()
    except Exception as exc:  # noqa: BLE001 — surfaced cleanly.
        # The table may not yet exist on a brand-new project — interpret as
        # "no runs yet" rather than a hard error.
        msg = str(exc)
        if _RUNS_TABLE in msg and ("does not exist" in msg or "not found" in msg.lower()):
            return []
        raise StateError(f"reading {_RUNS_TABLE}: {exc}") from exc
    return [_row_to_runrow(r) for r in rows]


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def list_runs(
    config_name: str,
    *,
    limit: int | None = None,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> list[RunRow]:
    """Return recent runs for the destination this config binds to — docs/09 §4."""
    dest = _resolve(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        if Capability.RUN_RECORDS not in dest.capabilities:
            raise StateError(
                f"destination {dest.name!r} does not declare "
                f"Capability.RUN_RECORDS; no _dtex_runs table is written"
            )
        return _query_runs(dest.conn, run_id=None, limit=limit)
    finally:
        dest.hooks["close"](dest.conn)


def get_run(
    config_name: str,
    run_id: str,
    *,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> tuple[RunRow | None, Path | None]:
    """Return one run's :class:`RunRow` + the path to its JSONL log file.

    Either may be ``None`` independently — a run with a record on disk but
    no log file (or vice versa) is unusual but possible if logs were
    cleaned. The CLI handles each case gracefully.
    """
    dest = _resolve(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        if Capability.RUN_RECORDS not in dest.capabilities:
            raise StateError(
                f"destination {dest.name!r} does not declare "
                f"Capability.RUN_RECORDS; no _dtex_runs table is written"
            )
        rows = _query_runs(dest.conn, run_id=run_id, limit=1)
    finally:
        dest.hooks["close"](dest.conn)

    record = rows[0] if rows else None
    log_path = dest.project_root / ".dtex" / "logs" / run_id / "run.jsonl"
    if not log_path.exists():
        log_path = None  # type: ignore[assignment]
    return record, log_path


def read_log_lines(path: Path) -> list[dict[str, Any]]:
    """Read a JSONL log file into a list of dicts; malformed lines are skipped."""
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                # A truncated tail line from a crashed run — skip silently;
                # the readable lines above are the recoverable forensics.
                continue
    return events
