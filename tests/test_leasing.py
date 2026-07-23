"""Tests for stream-level run leasing — docs/05 §5.5.

Layers, cheapest first:
    * LeaseRecord.is_live — the staleness/liveness rule (pure, no I/O).
    * DuckDB lease hooks — acquire (compare-and-set), heartbeat/release,
      stale-reclaim, and the two-run race, driven against a real in-memory
      DuckDB (the same backend the rest of the suite uses).
    * _LeaseCoordinator — skip-when-leased, the max_parallel cap, and that a
      build only releases leases it holds.
    * ProjectConfig.concurrency — parsing + validation of the project setting.
    * End-to-end backward-compat — a run with no live lease behaves exactly
      as before (every stream runs), and the LEASE capability path is inert
      for a project with no concurrency config.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from dtex import Config, LeaseRecord, LeaseStatus
from dtex.types import LEASE_STALE_SECONDS
from tests.conftest import DUCKDB_CONNECTOR_DIR, LoadedConnector, load_connector

# ===========================================================================
# LeaseRecord.is_live — the pure liveness rule
# ===========================================================================


def _now() -> datetime:
    return datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)


def test_running_lease_with_fresh_heartbeat_is_live() -> None:
    lease = LeaseRecord(
        connector="c", stream="s", run_id="r1",
        status=LeaseStatus.RUNNING, heartbeat_at=_now() - timedelta(seconds=60),
    )
    assert lease.is_live(now=_now()) is True


def test_running_lease_with_stale_heartbeat_is_dead() -> None:
    lease = LeaseRecord(
        connector="c", stream="s", run_id="r1",
        status=LeaseStatus.RUNNING,
        heartbeat_at=_now() - timedelta(seconds=LEASE_STALE_SECONDS + 1),
    )
    assert lease.is_live(now=_now()) is False


def test_terminal_lease_is_never_live() -> None:
    for status in (LeaseStatus.DONE, LeaseStatus.FAILED):
        lease = LeaseRecord(
            connector="c", stream="s", run_id="r1",
            status=status, heartbeat_at=_now(),
        )
        assert lease.is_live(now=_now()) is False


def test_lease_with_no_heartbeat_is_dead() -> None:
    lease = LeaseRecord(connector="c", stream="s", run_id="r1", status=LeaseStatus.RUNNING)
    assert lease.is_live(now=_now()) is False


def test_lease_record_roundtrips_through_row() -> None:
    lease = LeaseRecord(
        connector="ckdb", stream="message", run_id="run-abc",
        status=LeaseStatus.RUNNING,
        acquired_at=_now(), heartbeat_at=_now(),
    )
    back = LeaseRecord.from_row(lease.to_row())
    assert back == lease


# ===========================================================================
# DuckDB lease hooks — real backend
# ===========================================================================


@pytest.fixture
def duckdb_destination() -> LoadedConnector:
    return load_connector(DUCKDB_CONNECTOR_DIR)


def _hooks(dest: LoadedConnector) -> dict[str, Callable[..., Any]]:
    return {name: dest.registry.hook(name).func for name in dest.registry.hook_names}  # type: ignore[union-attr]


def _conn(dest: LoadedConnector, path: str) -> Any:
    return _hooks(dest)["open"](Config(params={"path": path}))


def _lease(stream: str, run_id: str, *, status: LeaseStatus = LeaseStatus.RUNNING,
           beat: datetime | None = None) -> LeaseRecord:
    now = beat or datetime.now(UTC)
    return LeaseRecord(
        connector="ckdb", stream=stream, run_id=run_id,
        status=status, acquired_at=now, heartbeat_at=now,
    )


# The lease hooks are batched (docs/05 §5.5): acquire_leases takes a list and
# returns the set of won stream names; heartbeat_leases / release_leases take
# a list. These thin single-record wrappers keep the per-record assertions
# below readable while exercising the real batched hooks.
def _acquire(h: dict[str, Any], conn: Any, lease: LeaseRecord) -> bool:
    return lease.stream in h["acquire_leases"](conn, [lease])


def _release(h: dict[str, Any], conn: Any, lease: LeaseRecord) -> None:
    h["release_leases"](conn, [lease])


def test_acquire_on_free_stream_wins(duckdb_destination: LoadedConnector, tmp_path: Path) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    assert _acquire(h, conn, _lease("message", "r1")) is True
    leases = h["read_leases"](conn, "ckdb")
    assert len(leases) == 1
    assert leases[0].stream == "message"
    assert leases[0].run_id == "r1"
    assert leases[0].status is LeaseStatus.RUNNING


def test_batched_acquire_returns_only_the_won_streams(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    """One acquire_leases call over a mixed set returns exactly the winners."""
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    # r1 already holds 'chat' live; a batched r2 acquire of {chat, message}
    # must win only 'message'.
    assert _acquire(h, conn, _lease("chat", "r1")) is True
    won = h["acquire_leases"](conn, [_lease("chat", "r2"), _lease("message", "r2")])
    assert won == {"message"}
    by_stream = {le.stream: le.run_id for le in h["read_leases"](conn, "ckdb")}
    assert by_stream == {"chat": "r1", "message": "r2"}


def test_second_run_cannot_acquire_live_lease(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    """The core guarantee: a live lease blocks another run's acquire."""
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    assert _acquire(h, conn, _lease("message", "r1")) is True
    # r2 tries the same stream while r1's lease is fresh → loses.
    assert _acquire(h, conn, _lease("message", "r2")) is False
    # r1 still owns it.
    assert h["read_leases"](conn, "ckdb")[0].run_id == "r1"


def test_stale_lease_is_reclaimable(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    """A crashed run's stale lease is overwritten by a new acquire."""
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    stale = datetime.now(UTC) - timedelta(seconds=LEASE_STALE_SECONDS + 60)
    assert _acquire(h, conn, _lease("message", "r1", beat=stale)) is True
    # r2 acquires because r1's heartbeat is stale (r1 "crashed").
    assert _acquire(h, conn, _lease("message", "r2")) is True
    leases = h["read_leases"](conn, "ckdb")
    assert leases[0].run_id == "r2"


def test_release_sets_terminal_status_and_frees_stream(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    _acquire(h, conn, _lease("message", "r1"))
    _release(h, conn, _lease("message", "r1", status=LeaseStatus.DONE))
    lease = h["read_leases"](conn, "ckdb")[0]
    assert lease.status is LeaseStatus.DONE
    assert lease.is_live(now=datetime.now(UTC)) is False
    # A fresh run can now take the (released) stream.
    assert _acquire(h, conn, _lease("message", "r2")) is True


def test_heartbeat_keeps_lease_live(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    # Acquire with an almost-stale heartbeat, then refresh it via heartbeat_leases.
    old = datetime.now(UTC) - timedelta(seconds=LEASE_STALE_SECONDS - 5)
    _acquire(h, conn, _lease("message", "r1", beat=old))
    h["heartbeat_leases"](conn, [_lease("message", "r1", status=LeaseStatus.RUNNING)])
    lease = h["read_leases"](conn, "ckdb")[0]
    assert lease.status is LeaseStatus.RUNNING
    assert lease.is_live(now=datetime.now(UTC)) is True


def test_release_only_touches_own_run(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    """A release guarded on run_id cannot stomp a lease reclaimed by another run."""
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    stale = datetime.now(UTC) - timedelta(seconds=LEASE_STALE_SECONDS + 60)
    _acquire(h, conn, _lease("message", "r1", beat=stale))
    _acquire(h, conn, _lease("message", "r2"))  # reclaims from stale r1
    # r1 belatedly tries to release its (now-lost) lease → no effect on r2.
    _release(h, conn, _lease("message", "r1", status=LeaseStatus.FAILED))
    lease = h["read_leases"](conn, "ckdb")[0]
    assert lease.run_id == "r2"
    assert lease.status is LeaseStatus.RUNNING


# ===========================================================================
# _LeaseCoordinator — skip logic + max_parallel cap
# ===========================================================================


def _coordinator(hooks: dict[str, Any], conn: Any, run_id: str,
                 max_parallel: int | None) -> Any:
    from dtex.engine.runner import _LeaseCoordinator

    return _LeaseCoordinator(
        hooks, conn, connector="ckdb", run_id=run_id,
        max_parallel=max_parallel, log=logging.getLogger("test.lease"),
    )


def test_coordinator_skips_stream_leased_by_other_run(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    # r1 holds message.
    _acquire(h, conn, _lease("message", "r1"))
    # r2's coordinator reads leases at construction → sees message live, so a
    # batched acquire_all wins only the free stream.
    coord = _coordinator(h, conn, "r2", None)
    won = coord.acquire_all(["message", "chat"])
    assert won == {"chat"}


def test_coordinator_max_parallel_caps_acquisitions(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    coord = _coordinator(h, conn, "r1", 2)
    # A single batched acquire over three candidates honors the cap: the first
    # two in declared order win, the third is left for a later build.
    won = coord.acquire_all(["a", "b", "c"])
    assert won == {"a", "b"}


def test_coordinator_release_is_noop_for_unheld_stream(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    _acquire(h, conn, _lease("message", "r1"))
    coord = _coordinator(h, conn, "r2", None)
    coord.acquire_all(["message"])   # wins nothing — r1 holds it live
    # r2 releasing everything it holds (nothing) must not touch r1's lease.
    coord.release_all({})
    assert h["read_leases"](conn, "ckdb")[0].run_id == "r1"


# ===========================================================================
# ProjectConfig.concurrency — parsing + validation
# ===========================================================================


def _write_project(tmp_path: Path, body: str) -> Path:
    (tmp_path / "dtex_project.yml").write_text(body)
    return tmp_path


def test_concurrency_absent_is_empty_and_unbounded(tmp_path: Path) -> None:
    from dtex.engine.config import ProjectConfig

    p = ProjectConfig.load(_write_project(tmp_path, "name: proj\n"))
    assert dict(p.concurrency) == {}
    assert p.max_parallel_for("ckdb") is None


def test_concurrency_parses_per_source_cap(tmp_path: Path) -> None:
    from dtex.engine.config import ProjectConfig

    p = ProjectConfig.load(
        _write_project(tmp_path, "name: proj\nconcurrency:\n  ckdb: 2\n  other: 5\n")
    )
    assert p.max_parallel_for("ckdb") == 2
    assert p.max_parallel_for("other") == 5
    assert p.max_parallel_for("absent") is None


@pytest.mark.parametrize("bad", ["ckdb: 0", "ckdb: -1", "ckdb: true", "ckdb: 1.5", "ckdb: foo"])
def test_concurrency_rejects_non_positive_int(tmp_path: Path, bad: str) -> None:
    from dtex.engine.config import ConfigError, ProjectConfig

    with pytest.raises(ConfigError, match="positive integer"):
        ProjectConfig.load(_write_project(tmp_path, f"name: proj\nconcurrency:\n  {bad}\n"))


def test_concurrency_rejects_non_mapping(tmp_path: Path) -> None:
    from dtex.engine.config import ConfigError, ProjectConfig

    with pytest.raises(ConfigError, match="must be a mapping"):
        ProjectConfig.load(_write_project(tmp_path, "name: proj\nconcurrency: 3\n"))


# ===========================================================================
# Backward compatibility — a destination without a live lease runs everything
# ===========================================================================


def test_no_prior_leases_means_every_stream_acquirable(
    duckdb_destination: LoadedConnector, tmp_path: Path
) -> None:
    """Fresh project, no leases: the coordinator acquires every stream (today's behavior)."""
    h = _hooks(duckdb_destination)
    conn = _conn(duckdb_destination, str(tmp_path / "w.duckdb"))
    coord = _coordinator(h, conn, "r1", None)
    assert coord.acquire_all(["a", "b", "c", "d"]) == {"a", "b", "c", "d"}
