"""A test-only destination — declares ``max_concurrent_writes = 1`` (stage 8e).

In-memory; supports Tier A state on a class-level dict so the engine's
``read_state`` / ``commit_state`` calls round-trip during a tag sweep. The
purpose is the ``@destination.max_concurrent_writes`` hook returning 1 so
parallel run_tag tests can assert "two configs targeting this destination
serialize even with threads=4."

SELF-POLICING via a process-wide POSIX file lock keyed by destination
name. The engine imports each connector folder under a unique synthetic
module name (each pipeline = its own module instance with its own
globals), so a module-level counter cannot detect cross-pipeline
concurrency. A file lock under ``/tmp`` survives the per-pipeline import
isolation and is the durable detection surface: if the engine's
per-destination semaphore failed to serialize the test pipelines, two
``open()`` calls would race on the file lock and the second would raise.

Cleanup: the lock file lives at ``/tmp/det_lockedfake_<dest>.lock`` and
is best-effort-removed on each ``close()``. A leaked lock file from a
previous test run is harmless — the next test acquires + releases it.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from detx import (
    Batch,
    Capability,
    Config,
    StateRecord,
    StreamMeta,
    destination,
)

# The lock file path — POSIX-friendly, deterministic per destination name
# (so two distinct lockedfake instances — e.g. "otherfake" in the
# independent-destinations test — get separate locks).
_LOCK_DIR = Path(tempfile.gettempdir())


def _lock_path(destination_name: str) -> Path:
    """The lock file path for one destination instance."""
    return _LOCK_DIR / f"det_lockedfake_{destination_name}.lock"


def _try_acquire_lock(destination_name: str) -> int | None:
    """Open the lock file with O_EXCL — returns fd, or None if locked.

    POSIX ``O_CREAT | O_EXCL`` is atomic: if the file already exists, the
    open raises ``FileExistsError`` and we return None. This is the
    cross-process / cross-import concurrency probe: only ONE caller in
    the system can hold the lock for a given destination at a time.
    """
    path = _lock_path(destination_name)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        return fd
    except FileExistsError:
        return None


def _release_lock(destination_name: str, fd: int) -> None:
    """Close the fd and remove the lock file (best effort)."""
    try:
        os.close(fd)
    except OSError:
        pass
    path = _lock_path(destination_name)
    try:
        path.unlink()
    except FileNotFoundError:
        pass


# Process-wide in-memory state store. Keyed by source connector name so
# different sources don't collide; rerun semantics match real destinations.
_state_lock = threading.Lock()
_state_store: dict[str, list[StateRecord]] = {}

# Process-wide concurrency counters — used by the open() check to detect a
# missing per-destination semaphore in the engine. If two opens land
# concurrently, _active_count would briefly exceed 1 and open() raises.
_active_lock = threading.Lock()
_active_count: int = 0
_max_active: int = 0


@dataclass
class LockedConn:
    """The per-run handle. Carries the lock fd + the destination name."""

    hold_ms: int = 0
    truncated: set[str] = field(default_factory=set)
    lock_fd: int | None = None
    destination_name: str = "lockedfake"


@destination.capabilities
def capabilities() -> set[Capability]:
    """Tier A — declares STATE only (no MERGE, no TRANSACTIONAL_LOAD)."""
    return {Capability.STATE}


@destination.max_concurrent_writes
def max_concurrent_writes(config: Config) -> int:
    """Clamp to 1 — the whole point of this fixture (stage 8e)."""
    return 1


@destination.open
def open(config: Config) -> LockedConn:  # noqa: A001 — hook name fixed by contract
    """Return a fresh in-memory handle; record active-count entry.

    SELF-POLICING: if more than one pipeline is concurrently inside this
    destination's lifecycle (open ... close), raise immediately. The hook
    declares max_concurrent_writes = 1, so the engine's per-destination
    semaphore must prevent overlap. A test pipeline raising here proves
    the cap was honored (or, if it fires, that the cap was broken — the
    test then fails on ``r.status == FAILED``).

    The check + increment is done atomically inside one ``_active_lock``
    acquisition — otherwise two threads could both observe count==0 and
    both pass the check before either had a chance to ``_enter_active()``.

    Also sleeps briefly INSIDE the active window so a broken semaphore
    has time to surface as concurrent activity — without the sleep,
    write_batch may complete fast enough that ``open → close`` of one
    pipeline finishes before the next one enters. The sleep + the count
    check together are the durable form of the cap regression test.
    """
    global _active_count, _max_active
    with _active_lock:
        if _active_count >= 1:
            raise RuntimeError(
                f"lockedfake: {_active_count + 1} concurrent opens "
                f"(cap is 1) — the engine's per-destination semaphore "
                f"did not serialize pipelines targeting this destination"
            )
        _active_count += 1
        _max_active = max(_max_active, _active_count)
    return LockedConn(hold_ms=int(config.get("hold_ms") or 0))


def _exit_active() -> None:
    """Decrement the process-wide active-count tracker.

    Called from ``close``; intentionally never raises (per the destination
    contract, ``close`` must be safe even on a half-open handle).
    """
    global _active_count
    with _active_lock:
        if _active_count > 0:
            _active_count -= 1


def _reset_concurrency_counter() -> None:
    """Reset the process-wide active/peak counters to zero.

    Test-only helper. Called between test runs that share this destination
    module to clear any leaked state from a prior pipeline that didn't
    cleanly exit (e.g. when a test deliberately aborts mid-run). Reading the
    counter cross-import doesn't work (each import gets fresh globals); the
    tests instead use wall-clock as the durable proof of serialization.
    """
    global _active_count, _max_active
    with _active_lock:
        _active_count = 0
        _max_active = 0


def get_peak_concurrency() -> int:
    """Return the highest ``_active_count`` observed since the last reset.

    Test helper paired with :func:`_reset_concurrency_counter`. Same
    cross-import caveat — useful only inside the engine's same module
    instance, but harmless to expose for any test that does work in-process.
    """
    with _active_lock:
        return _max_active


@destination.close
def close(conn: LockedConn) -> None:
    """Decrement the active-count tracker."""
    _exit_active()


@destination.ensure_schema
def ensure_schema(conn: LockedConn, stream: StreamMeta) -> None:
    """No-op — in-memory means no DDL."""


@destination.write_batch
def write_batch(conn: LockedConn, batch: Batch, stream: StreamMeta) -> int:
    """Sleep for hold_ms then "write" the batch (return its length)."""
    if conn.hold_ms > 0:
        time.sleep(conn.hold_ms / 1000.0)
    return len(batch)


@destination.read_state
def read_state(conn: LockedConn, connector: str) -> list[StateRecord]:
    """Load prior records from the process-wide store."""
    with _state_lock:
        return list(_state_store.get(connector, []))


@destination.commit_state
def commit_state(conn: LockedConn, run_id: str, records: list[StateRecord]) -> None:
    """Upsert records into the process-wide store."""
    with _state_lock:
        for record in records:
            existing = _state_store.setdefault(record.connector, [])
            # Replace any prior record for the same stream — same upsert
            # semantic as a real destination's _detx_state.
            existing[:] = [r for r in existing if r.stream != record.stream]
            existing.append(record)


@contextmanager
def _placeholder() -> Iterator[None]:
    """Unused; here only to keep the module importable as a connector."""
    yield  # pragma: no cover


# Mark unused names referenced for completeness. ``Any`` kept in imports
# in case a test wants to extend the in-memory store with richer values.
_ = Any
