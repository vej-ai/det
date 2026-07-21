"""Tests for mid-stream state flushing — docs/05 §5.2.

Proves the resume invariant that stops an interrupted `append` stream from
re-appending its overlap as duplicates (the CKDB `message` incident):

    * a stream that raises mid-loop has already persisted its in-progress
      state (the connector's resume pointer) before the crash;
    * the flush happens strictly AFTER a batch's write is durable
      (commit-after-write ordering);
    * flushes are throttled — many fast batches produce far fewer commits
      than batches;
    * a destination without a ``commit_state`` hook runs unchanged (no
      commits, no error);
    * a FULL_REFRESH-this-run incremental stream never flushes state.

These drive ``_run_one_stream`` directly with fake hooks that record an
ordered event log, the lightest way to observe commit timing/ordering
without a warehouse.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import pytest

from dtex import Batch, Config, CursorType, stream
from dtex.engine import runner
from dtex.engine.runner import _run_one_stream
from dtex.registry import StreamRegistration
from dtex.types import (
    Field,
    FieldType,
    Incremental,
    PipelineConfig,
    RunConfig,
    Schema,
    StateRecord,
    StreamDef,
    StreamMode,
    StreamRunConfig,
    WriteDisposition,
)

LOG = logging.getLogger("test.state_flush")


# ---------------------------------------------------------------------------
# Fakes — a source exposing .registry.stream(name), and hooks with an event log
# ---------------------------------------------------------------------------


class _FakeRegistry:
    def __init__(self, registration: StreamRegistration) -> None:
        self._registration = registration

    def stream(self, name: str) -> StreamRegistration | None:
        return self._registration if name == self._registration.name else None


class _FakeSource:
    def __init__(self, registration: StreamRegistration) -> None:
        self.registry = _FakeRegistry(registration)


def _make_source(gen_func: Any, name: str = "rows") -> _FakeSource:
    """Wrap a generator as a registered @stream for the engine.

    Applies the real ``@stream`` decorator (a no-op registration-wise outside a
    scope, but it stamps the injectable list ``compute_injection`` reads), then
    builds the ``StreamRegistration`` the engine binds to.
    """
    decorated = stream(name=name)(gen_func)
    inject = decorated.__dtex_inject__  # type: ignore[attr-defined]
    reg = StreamRegistration(name=name, func=decorated, inject=inject)
    return _FakeSource(reg)


def _hooks_with_log(
    events: list[tuple[str, Any]],
    *,
    include_commit_state: bool = True,
    write_batch_raises_on: int | None = None,
) -> dict[str, Any]:
    """Build a fake destination hook set that appends to ``events``.

    ``write_batch_raises_on`` — if set, the Nth (1-based) write_batch call
    raises, simulating a mid-stream crash *after* prior batches landed.
    """
    state = {"writes": 0}

    def capabilities() -> set[Any]:
        return set()

    def open_(config: Any) -> Any:
        return object()

    def ensure_schema(conn: Any, meta: Any) -> None:
        events.append(("ensure_schema", meta.table))

    def write_batch(conn: Any, batch: Batch, meta: Any) -> int:
        state["writes"] += 1
        if write_batch_raises_on is not None and state["writes"] == write_batch_raises_on:
            events.append(("write_batch_raise", state["writes"]))
            raise RuntimeError("simulated mid-stream crash")
        events.append(("write_batch", len(batch)))
        return len(batch)

    def close(conn: Any) -> None:
        pass

    hooks: dict[str, Any] = {
        "capabilities": capabilities,
        "open": open_,
        "ensure_schema": ensure_schema,
        "write_batch": write_batch,
        "close": close,
    }
    if include_commit_state:

        def commit_state(conn: Any, run_id: str, records: list[StateRecord]) -> None:
            # Record the resume pointer the flush is persisting so a test can
            # assert what mid-stream state was captured.
            blob = dict(records[0].state_blob)
            events.append(("commit_state", blob.get("pk")))

        hooks["commit_state"] = commit_state
    return hooks


def _run_config(full_refresh: bool = False) -> RunConfig:
    return RunConfig(
        run_id="run-test",
        pipeline="p",
        connector="src",
        target="dev",
        config=Config(params={}, secrets={}),
        full_refresh=full_refresh,
    )


def _pipeline(mode: StreamMode | None = None) -> PipelineConfig:
    streams: dict[str, StreamRunConfig] = {}
    if mode is not None:
        streams["rows"] = StreamRunConfig(mode=mode)
    return PipelineConfig(
        name="p",
        source="src",
        destination="dst",
        streams=streams,
        all_streams=not streams,
    )


def _incremental_stream_def() -> StreamDef:
    schema = Schema(
        fields=(
            Field(name="id", type=FieldType.INTEGER),
            Field(name="updated_at", type=FieldType.INTEGER),
        )
    )
    return StreamDef(
        name="rows",
        table="rows_table",
        primary_key=("id",),
        write_disposition=WriteDisposition.APPEND,
        incremental=Incremental(cursor_field="updated_at", cursor_type=CursorType.INT),
        schema=schema,
    )


# ---------------------------------------------------------------------------
# (a) interrupted stream persists its resume pointer before the crash
# ---------------------------------------------------------------------------


def test_interrupted_stream_persisted_state_before_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stream that raises mid-loop has already flushed its resume pointer.

    Without mid-stream flushing, the connector's ``state.set('pk', ...)`` calls
    would live only in memory and die with the crash; the restart would resume
    from the far-behind persisted pointer and re-append. With flushing (here
    forced every batch via interval=0), the pointer for the last durable batch
    is on disk before the crash.
    """
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for i in range(1, 6):
            state.set("pk", i)  # connector's resume pointer
            cursor.observe(i * 10)
            yield [{"id": i, "updated_at": i * 10}]

    events: list[tuple[str, Any]] = []
    # Crash on the 4th write: batches 1-3 landed, 4 fails mid-stream.
    hooks = _hooks_with_log(events, write_batch_raises_on=4)
    source = _make_source(gen)

    with pytest.raises(RuntimeError, match="simulated mid-stream crash"):
        _run_one_stream(
            _incremental_stream_def(),
            source,  # type: ignore[arg-type]
            hooks,
            conn=object(),
            run_config=_run_config(),
            pipeline=_pipeline(),
            prior=None,
            log=LOG,
        )

    commits = [pk for kind, pk in events if kind == "commit_state"]
    # At least one flush landed before the crash, and it captured a resume
    # pointer for an already-written batch (pk 3 — the last durable batch).
    assert commits, "expected a mid-stream state flush before the crash"
    assert commits[-1] == 3, f"resume pointer should be the last durable batch, got {commits}"


# ---------------------------------------------------------------------------
# (b) commit-after-write ordering
# ---------------------------------------------------------------------------


def test_state_flush_happens_after_write_not_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every commit_state is preceded by the write_batch it records.

    The event log must never show a commit_state for a batch before that
    batch's write_batch — otherwise a crash between flush and write would
    strand the resume pointer past rows that never landed.
    """
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for i in range(1, 4):
            state.set("pk", i)
            cursor.observe(i * 10)
            yield [{"id": i, "updated_at": i * 10}]

    events: list[tuple[str, Any]] = []
    hooks = _hooks_with_log(events)
    source = _make_source(gen)

    _run_one_stream(
        _incremental_stream_def(),
        source,  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=None,
        log=LOG,
    )

    # Walk the log: no commit may appear before the first write, and every
    # commit must be preceded by at least one durable write (commit-after-
    # write). The final end-of-stream commit legitimately follows the last
    # write, so we assert "≥1 write seen" rather than a strict per-index
    # pairing — the ordering property is "rows durable, then state", not a
    # 1:1 count.
    writes = 0
    saw_commit = False
    for kind, _ in events:
        if kind == "write_batch":
            writes += 1
        elif kind == "commit_state":
            saw_commit = True
            assert writes >= 1, f"commit fired before any write landed: {events}"
    assert saw_commit, "expected at least the end-of-stream commit"
    # And the very first data event after ensure_schema is a write, never a
    # commit — a direct check that state never leads its batch.
    data_events = [k for k, _ in events if k in ("write_batch", "commit_state")]
    assert data_events[0] == "write_batch", f"first data event must be a write: {events}"


# ---------------------------------------------------------------------------
# (c) throttling — many fast batches produce far fewer commits than batches
# ---------------------------------------------------------------------------


def test_state_flush_is_throttled(monkeypatch: pytest.MonkeyPatch) -> None:
    """With the default interval, a burst of fast batches flushes rarely mid-run.

    Many batches complete within one interval, so only the final end-of-stream
    commit is expected (plus possibly one mid-stream flush, never one-per-batch).
    """
    # Keep the real (large) interval so the fast loop never crosses it.
    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for i in range(1, 51):
            state.set("pk", i)
            cursor.observe(i * 10)
            yield [{"id": i, "updated_at": i * 10}]

    events: list[tuple[str, Any]] = []
    hooks = _hooks_with_log(events)
    source = _make_source(gen)

    _run_one_stream(
        _incremental_stream_def(),
        source,  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=None,
        log=LOG,
    )

    writes = sum(1 for k, _ in events if k == "write_batch")
    commits = sum(1 for k, _ in events if k == "commit_state")
    assert writes == 50
    # Far fewer commits than batches — the throttle collapsed the 50-batch
    # burst to at most a couple of writes (mid-stream flushes) plus the final.
    assert commits <= 2, f"expected throttled commits, got {commits} for {writes} batches"
    assert commits >= 1, "the end-of-stream commit must always fire"


# ---------------------------------------------------------------------------
# (d) backward-compat — no commit_state hook → no commits, no error
# ---------------------------------------------------------------------------


def test_no_commit_state_hook_runs_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """A destination without commit_state runs unchanged — no flush, no error."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for i in range(1, 4):
            state.set("pk", i)
            cursor.observe(i * 10)
            yield [{"id": i, "updated_at": i * 10}]

    events: list[tuple[str, Any]] = []
    hooks = _hooks_with_log(events, include_commit_state=False)
    source = _make_source(gen)

    result = _run_one_stream(
        _incremental_stream_def(),
        source,  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=None,
        log=LOG,
    )

    assert result.rows_loaded == 3
    assert not any(k == "commit_state" for k, _ in events)


# ---------------------------------------------------------------------------
# (e) FULL_REFRESH incremental stream never flushes state
# ---------------------------------------------------------------------------


def test_full_refresh_incremental_stream_does_not_flush_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """skip_state (incremental stream run as FULL_REFRESH) suppresses all commits.

    The §3.1 rule: such a run must not touch _dtex_state, so a sibling
    incremental config keeps its cursor. That gates the mid-stream flushes too.
    """
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for i in range(1, 4):
            state.set("pk", i)
            cursor.observe(i * 10)
            yield [{"id": i, "updated_at": i * 10}]

    events: list[tuple[str, Any]] = []
    hooks = _hooks_with_log(events)
    source = _make_source(gen)

    _run_one_stream(
        _incremental_stream_def(),
        source,  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(full_refresh=True),
        pipeline=_pipeline(mode=StreamMode.FULL_REFRESH),
        prior=None,
        log=LOG,
    )

    assert not any(k == "commit_state" for k, _ in events), (
        "a FULL_REFRESH incremental stream must not write _dtex_state"
    )
