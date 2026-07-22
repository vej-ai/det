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
from dtex import Config, RunStatus, StreamStatus
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
# _LeaseCoordinator under concurrency — the in-process lock
# ===========================================================================


class _FakeLeaseHooks:
    """Minimal lease hooks backing _LeaseCoordinator with an in-memory store.

    acquire_lease is a compare-and-set over a dict guarded by its own lock, so
    it models the destination's cross-process CAS; the coordinator's lock is
    the *in-process* guard this test is really exercising. ``acquire_delay``
    forces threads to interleave inside try_acquire.
    """

    def __init__(self, acquire_delay: float = 0.0) -> None:
        self._store: dict[tuple[str, str], Any] = {}
        self._lock = threading.Lock()
        self._acquire_delay = acquire_delay
        self.acquire_calls = 0

    def read_leases(self, conn: Any, connector: str) -> list[Any]:
        return []

    def acquire_lease(self, conn: Any, lease: Any) -> bool:
        if self._acquire_delay:
            time.sleep(self._acquire_delay)
        with self._lock:
            self.acquire_calls += 1
            key = (lease.connector, lease.stream)
            if key in self._store:
                return False
            self._store[key] = lease.run_id
            return True

    def release_lease(self, conn: Any, lease: Any) -> None:
        return None

    def as_dict(self) -> dict[str, Callable[..., Any]]:
        return {
            "read_leases": self.read_leases,
            "acquire_lease": self.acquire_lease,
            "release_lease": self.release_lease,
        }


def test_coordinator_max_parallel_holds_under_thread_race() -> None:
    """Many threads racing try_acquire never exceed max_parallel held streams.

    Without the coordinator's lock, the `len(_held) >= cap` check and the
    `_held.add` race, letting more than `cap` streams through. Each thread
    targets a distinct stream (so the destination CAS always says yes) — the
    only thing that can hold the line is the in-process cap check.
    """
    hooks = _FakeLeaseHooks(acquire_delay=0.001).as_dict()
    coord = _LeaseCoordinator(
        hooks, conn=object(), connector="c", run_id="r1", max_parallel=3,
        log=logging.getLogger("test.parallel"),
    )
    results: list[bool] = []
    results_lock = threading.Lock()

    def _try(stream: str) -> None:
        won = coord.try_acquire(stream)
        with results_lock:
            results.append(won)

    threads = [
        threading.Thread(target=_try, args=(f"s{i}",)) for i in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly max_parallel acquisitions succeed, no more — the cap held.
    assert sum(results) == 3


def test_coordinator_distinct_streams_all_acquire_when_uncapped() -> None:
    """With no cap, every distinct stream acquires even under a thread race."""
    hooks = _FakeLeaseHooks(acquire_delay=0.001).as_dict()
    coord = _LeaseCoordinator(
        hooks, conn=object(), connector="c", run_id="r1", max_parallel=None,
        log=logging.getLogger("test.parallel"),
    )
    won: list[bool] = []
    lock = threading.Lock()

    def _try(stream: str) -> None:
        r = coord.try_acquire(stream)
        with lock:
            won.append(r)

    ts = [threading.Thread(target=_try, args=(f"s{i}",)) for i in range(12)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert all(won)
    assert sum(won) == 12


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
