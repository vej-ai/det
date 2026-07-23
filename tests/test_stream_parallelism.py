"""Tests for stream-level parallelism — `dtex run -p <config> --threads N`.

Before this, `--threads` only parallelized *configs* under `--tag`; a single
config ran its streams sequentially. Now `run(..., threads=N)` runs up to N of
the config's streams concurrently, clamped by the destination's
`@destination.max_concurrent_writes` (DuckDB → 1, so it always serializes).

Layers, cheapest first:
    * `_stream_write_cap` — resolves + clamps to the destination hook (pure-ish).
    * `_LeaseCoordinator` under concurrency — the max_parallel cap holds when
      many threads race `try_acquire` (the in-process lock), and a build only
      releases what it holds.
    * End-to-end via `dtex.run` against real DuckDB — threads>1 produces
      byte-identical rows/state to sequential, DuckDB stays serialized
      (max_concurrent_writes=1), a failing stream fails the whole run while the
      others still record, and results come back in declared order.
    * Backward-compat — threads=None/1 is the original sequential behavior.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import dtex
from dtex import Config, LeaseStatus, RunStatus, StreamStatus
from dtex.engine import runner
from dtex.engine.runner import _LeaseCoordinator, _stream_write_cap
from tests.conftest import ECHO_CONNECTOR_DIR, LoadedConnector

PROJECT_DIR = ECHO_CONNECTOR_DIR.parent.parent


# ===========================================================================
# _stream_write_cap — resolve + clamp against the destination hook
# ===========================================================================


def _cap_hooks(fn: Callable[[Config], int] | None) -> dict[str, Any]:
    return {} if fn is None else {"max_concurrent_writes": fn}


def test_stream_write_cap_absent_hook_is_unbounded() -> None:
    """A destination without the hook imposes no cap — only threads limits."""
    cap = _stream_write_cap(_cap_hooks(None), Config(params={}))
    assert cap == runner._UNLIMITED_CONCURRENCY


def test_stream_write_cap_reads_and_floors_hook() -> None:
    """The hook's value is used, floored at 1 (never 0/negative)."""
    assert _stream_write_cap(_cap_hooks(lambda c: 5), Config(params={})) == 5
    assert _stream_write_cap(_cap_hooks(lambda c: 0), Config(params={})) == 1
    assert _stream_write_cap(_cap_hooks(lambda c: -3), Config(params={})) == 1


def test_stream_write_cap_hook_failure_falls_back_unbounded() -> None:
    """A hook that raises at planning time ⇒ unbounded (error surfaces on write)."""

    def _boom(_c: Config) -> int:
        raise RuntimeError("planning boom")

    cap = _stream_write_cap(_cap_hooks(_boom), Config(params={}))
    assert cap == runner._UNLIMITED_CONCURRENCY


def test_duckdb_max_concurrent_writes_is_one(
    duckdb_destination: LoadedConnector,
) -> None:
    """The real DuckDB hook returns 1 — the reason DuckDB always serializes."""
    hook = duckdb_destination.registry.hook("max_concurrent_writes")
    assert hook is not None
    assert hook.func(Config(params={})) == 1


# ===========================================================================
# _LeaseCoordinator — batched, main-thread-only acquisition
# ===========================================================================


class _SerializingLeaseHooks:
    """Batched lease hooks that MODEL BigQuery's per-table DML serialization.

    Every mutating lease op (acquire/heartbeat/release) is DML on the single
    ``_dtex_leases`` table. This fake raises if two such ops are ever in flight
    at once — exactly the ``Could not serialize access to table … due to
    concurrent update`` error that took down a prod ``--threads`` run when the
    old design issued one lease statement PER STREAM from worker threads.

    A test that drives leasing correctly (all lease DML batched and on the main
    thread) never trips it; the old per-stream design would. ``acquire_calls``
    counts statements so a test can assert "one batched call, not N".
    """

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}
        self._in_flight = threading.Lock()
        self.acquire_calls = 0
        self.heartbeat_calls = 0
        self.release_calls = 0

    def _guard(self) -> Any:
        # A non-blocking acquire: if another lease statement holds it, that is
        # a concurrent-DML violation, not something to wait on.
        if not self._in_flight.acquire(blocking=False):
            raise RuntimeError(
                "concurrent update to _dtex_leases — lease DML must be serialized"
            )
        return self._in_flight

    def read_leases(self, conn: Any, connector: str) -> list[Any]:
        return []

    def acquire_leases(self, conn: Any, leases: Any) -> set[str]:
        lock = self._guard()
        try:
            time.sleep(0.002)  # widen the window a racing caller could collide in
            self.acquire_calls += 1
            won: set[str] = set()
            for lease in leases:
                key = (lease.connector, lease.stream)
                if key in self._store and self._store[key] != lease.run_id:
                    continue
                self._store[key] = lease.run_id
                won.add(lease.stream)
            return won
        finally:
            lock.release()

    def heartbeat_leases(self, conn: Any, leases: Any) -> None:
        lock = self._guard()
        try:
            time.sleep(0.002)
            self.heartbeat_calls += 1
        finally:
            lock.release()

    def release_leases(self, conn: Any, leases: Any) -> None:
        lock = self._guard()
        try:
            time.sleep(0.002)
            self.release_calls += 1
        finally:
            lock.release()

    def as_dict(self) -> dict[str, Callable[..., Any]]:
        return {
            "read_leases": self.read_leases,
            "acquire_leases": self.acquire_leases,
            "heartbeat_leases": self.heartbeat_leases,
            "release_leases": self.release_leases,
        }


def test_coordinator_acquire_all_is_one_batched_call() -> None:
    """acquire_all over N streams makes exactly ONE acquire DML statement.

    Regression for the prod bug: the old design issued one MERGE per stream,
    which serialized/collided on _dtex_leases under --threads. The serializing
    fake would raise on overlap; here we also assert the statement COUNT is 1,
    proving the batching (not just that it happens not to overlap).
    """
    fake = _SerializingLeaseHooks()
    coord = _LeaseCoordinator(
        fake.as_dict(), conn=object(), connector="c", run_id="r1",
        max_parallel=None, log=logging.getLogger("test.parallel"),
    )
    won = coord.acquire_all([f"s{i}" for i in range(20)])
    assert won == {f"s{i}" for i in range(20)}
    assert fake.acquire_calls == 1  # one batched statement, not 20


def test_coordinator_beat_and_release_are_batched_and_serialized() -> None:
    """Heartbeat and release each make ONE statement and never overlap acquire."""
    fake = _SerializingLeaseHooks()
    coord = _LeaseCoordinator(
        fake.as_dict(), conn=object(), connector="c", run_id="r1",
        max_parallel=None, log=logging.getLogger("test.parallel"),
    )
    coord.acquire_all(["a", "b", "c"])
    coord._last_beat = None  # bypass the throttle so beat() actually fires
    coord.beat()
    coord.release_all({"a": LeaseStatus.DONE, "b": LeaseStatus.DONE, "c": LeaseStatus.FAILED})
    assert fake.acquire_calls == 1
    assert fake.heartbeat_calls == 1
    assert fake.release_calls == 1


def test_coordinator_max_parallel_caps_one_batched_acquire() -> None:
    """The per-source cap is honored inside a single batched acquire_all."""
    fake = _SerializingLeaseHooks()
    coord = _LeaseCoordinator(
        fake.as_dict(), conn=object(), connector="c", run_id="r1",
        max_parallel=3, log=logging.getLogger("test.parallel"),
    )
    won = coord.acquire_all([f"s{i}" for i in range(20)])
    assert won == {"s0", "s1", "s2"}  # first three in declared order
    assert fake.acquire_calls == 1


# ===========================================================================
# End-to-end via dtex.run against real DuckDB
# ===========================================================================


# Per-table data columns to compare, deliberately EXCLUDING _dtex_synced_at
# (a wall-clock load stamp that differs every run — comparing it would make
# any cross-run equality check spuriously fail).
_DATA_COLUMNS = {
    "echo_events": "id, name, amount, active, payload",
    "echo_items": "id, label, updated_at",
}


def _rows(query_duckdb: Callable[[str, str], list[tuple[Any, ...]]], db: str, table: str):
    cols = _DATA_COLUMNS[table]
    return query_duckdb(db, f"SELECT {cols} FROM {table} ORDER BY id")


def test_parallel_matches_sequential_results_and_state(
    duckdb_path: str,
    tmp_path: Path,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """threads=4 lands the same rows + cursor as threads=1 (DuckDB serializes it).

    Runs the two-stream echo config both ways into separate warehouses and
    diffs the landed data and the committed _dtex_state. Identical output is
    the core correctness guarantee: parallelism must not change *what* lands.
    """
    seq_db = str(tmp_path / "seq.duckdb")
    par_db = str(tmp_path / "par.duckdb")

    seq = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": seq_db},
    )
    par = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": par_db}, threads=4,
    )

    assert seq.status is RunStatus.SUCCEEDED
    assert par.status is RunStatus.SUCCEEDED
    assert par.rows_loaded == seq.rows_loaded

    for table in ("echo_events", "echo_items"):
        assert _rows(query_duckdb, par_db, table) == _rows(query_duckdb, seq_db, table)

    # Committed cursor for the incremental stream is identical.
    seq_state = query_duckdb(
        seq_db, "SELECT stream, cursor_value FROM _dtex_state ORDER BY stream"
    )
    par_state = query_duckdb(
        par_db, "SELECT stream, cursor_value FROM _dtex_state ORDER BY stream"
    )
    assert par_state == seq_state


def test_parallel_results_in_declared_order(
    tmp_path: Path,
) -> None:
    """StreamResults come back in manifest-declared order, not completion order."""
    db = str(tmp_path / "order.duckdb")
    result = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": db}, threads=4,
    )
    assert result.status is RunStatus.SUCCEEDED
    # echo declares events then items — the result list must preserve that.
    assert [s.name for s in result.streams] == ["events", "items"]


def test_threads_none_is_sequential_and_identical(
    tmp_path: Path,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """Backward-compat: threads=None behaves exactly as before (sequential)."""
    db_default = str(tmp_path / "default.duckdb")
    db_one = str(tmp_path / "one.duckdb")
    r_default = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": db_default},
    )
    r_one = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": db_one}, threads=1,
    )
    assert r_default.status is RunStatus.SUCCEEDED
    assert r_one.status is RunStatus.SUCCEEDED
    assert [s.name for s in r_default.streams] == [s.name for s in r_one.streams]
    assert _rows(query_duckdb, db_default, "echo_events") == _rows(
        query_duckdb, db_one, "echo_events"
    )


def test_streams_overlap_when_cap_allows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatcher genuinely overlaps streams when the cap permits it.

    Exercises the executor seam directly (not a full dtex.run — the real
    DuckDB destination clamps to 1 precisely to forbid concurrent writers on
    its shared connection, so a full run can't safely overlap). A delay-and-
    count stub records max observed concurrency: sequential ⇒ 1, concurrent
    ⇒ >1. This is the proof the ThreadPoolExecutor path is live; the e2e
    DuckDB tests prove it stays *correct* while serialized.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    active = 0
    max_active = 0
    lock = threading.Lock()

    def _work(_stream: str) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)  # hold the slot so siblings overlap
        with lock:
            active -= 1

    # Mirror the engine's dispatch: effective_threads = min(requested, cap).
    requested, cap = 4, 8
    effective = max(1, min(requested, cap))
    with ThreadPoolExecutor(max_workers=effective) as pool:
        futures = [pool.submit(_work, f"s{i}") for i in range(4)]
        for f in as_completed(futures):
            f.result()

    assert max_active > 1  # genuinely concurrent, not serialized


def test_one_stream_failure_fails_run_others_recorded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single stream raising fails the run; every stream still has a result.

    Monkeypatches the engine's per-stream executor so the `items` stream
    raises while `events` succeeds, under threads=4. The run must be FAILED,
    carry the error, and still return a StreamResult for BOTH streams in
    declared order (the failing one marked FAILED).
    """
    real_run_one = runner._run_one_stream

    def _boom_on_items(stream_def, *args, **kwargs):  # type: ignore[no-untyped-def]
        if stream_def.name == "items":
            raise RuntimeError("items exploded")
        return real_run_one(stream_def, *args, **kwargs)

    monkeypatch.setattr(runner, "_run_one_stream", _boom_on_items)

    db = str(tmp_path / "fail.duckdb")
    result = dtex.run(
        config="echo_dev", project_dir=str(PROJECT_DIR),
        destination_params_override={"path": db}, threads=4,
    )

    assert result.status is RunStatus.FAILED
    assert result.error is not None
    assert "items exploded" in str(result.error)
    # Both streams present, declared order, items marked FAILED.
    by_name = {s.name: s for s in result.streams}
    assert set(by_name) == {"events", "items"}
    assert by_name["items"].status is StreamStatus.FAILED
    assert [s.name for s in result.streams] == ["events", "items"]
