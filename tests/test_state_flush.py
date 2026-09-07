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
    cursor_values: list[Any] | None = None,
) -> dict[str, Any]:
    """Build a fake destination hook set that appends to ``events``.

    ``write_batch_raises_on`` — if set, the Nth (1-based) write_batch call
    raises, simulating a mid-stream crash *after* prior batches landed.
    ``cursor_values`` — if given, every committed ``cursor_value`` (mid-stream
    flushes and the final commit) is appended to it, in order.
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
            if cursor_values is not None:
                cursor_values.append(records[0].cursor_value)

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


# ---------------------------------------------------------------------------
# (f) the persisted cursor never moves backwards (vej-ai/dtex#1)
# ---------------------------------------------------------------------------


def _prior(cursor_value: Any, cursor_type: CursorType = CursorType.INT) -> StateRecord:
    return StateRecord(
        connector="src", stream="rows", cursor_value=cursor_value, cursor_type=cursor_type
    )


def _stream_def(cursor_type: CursorType = CursorType.INT) -> StreamDef:
    base = _incremental_stream_def()
    return StreamDef(
        name=base.name,
        table=base.table,
        primary_key=base.primary_key,
        write_disposition=base.write_disposition,
        incremental=Incremental(cursor_field="updated_at", cursor_type=cursor_type),
        schema=base.schema,
    )


def test_partial_lookback_rewalk_never_moves_cursor_backwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stream at cursor 100 re-walks its lookback from 70 and dies at 90.

    Every mid-stream flush before the crash used to persist the max of the
    PARTIAL walk (70, 80, 90) — earlier than the 100 the run started from —
    so a platform kill moved the cursor backwards and paged a freshness
    monitor for a pipeline that was fine. The floor is the prior cursor.
    """
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        assert cursor.start_value() == 100  # no incremental.lookback declared here
        for value in (70, 80, 90, 110, 120):  # connector-owned re-walk below the cursor
            cursor.observe(value)
            yield [{"id": value, "updated_at": value}]

    events: list[tuple[str, Any]] = []
    committed: list[Any] = []
    hooks = _hooks_with_log(events, write_batch_raises_on=4, cursor_values=committed)

    with pytest.raises(RuntimeError, match="simulated mid-stream crash"):
        _run_one_stream(
            _stream_def(),
            _make_source(gen),  # type: ignore[arg-type]
            hooks,
            conn=object(),
            run_config=_run_config(),
            pipeline=_pipeline(),
            prior=_prior(100),
            log=LOG,
        )

    assert committed, "expected mid-stream flushes before the crash"
    assert committed == [100, 100, 100], committed


def test_cursor_floor_is_prior_when_rewalk_observes_less_and_advances_otherwise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def run_with(observed: list[int], prior: StateRecord | None) -> tuple[Any, list[Any]]:
        def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
            for value in observed:
                cursor.observe(value)
                yield [{"id": value, "updated_at": value}]

        events: list[tuple[str, Any]] = []
        committed: list[Any] = []
        result = _run_one_stream(
            _stream_def(),
            _make_source(gen),  # type: ignore[arg-type]
            _hooks_with_log(events, cursor_values=committed),
            conn=object(),
            run_config=_run_config(),
            pipeline=_pipeline(),
            prior=prior,
            log=LOG,
        )
        return result.cursor_after, committed

    # A full re-walk that tops out BELOW the prior (rows deleted at the
    # source, or a shorter window) keeps the prior high-water mark.
    after, committed = run_with([70, 80, 90], _prior(100))
    assert after == 100 and committed[-1] == 100
    # A walk that passes the prior advances normally.
    after, committed = run_with([70, 120], _prior(100))
    assert after == 120 and committed[-1] == 120
    # A virgin stream (no prior row) is unaffected.
    after, committed = run_with([70, 80], None)
    assert after == 80 and committed[-1] == 80
    # A stream that observed nothing keeps the prior (as before).
    after, committed = run_with([], _prior(100))
    assert after == 100 and committed[-1] == 100


def test_cursor_floor_compares_typed_values_across_the_json_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DATE cursor reads back from ``_dtex_state`` as an ISO string while
    the connector observes ``date`` objects — the clamp compares them typed
    rather than crashing on ``date >= str`` or committing the partial value.
    """
    from datetime import date

    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for day in (date(2026, 8, 19), date(2026, 8, 22)):
            cursor.observe(day)
            yield [{"id": day.day, "updated_at": day.day}]

    events: list[tuple[str, Any]] = []
    committed: list[Any] = []
    result = _run_one_stream(
        _stream_def(CursorType.DATE),
        _make_source(gen),  # type: ignore[arg-type]
        _hooks_with_log(events, cursor_values=committed),
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=_prior("2026-08-28", CursorType.DATE),
        log=LOG,
    )
    assert result.cursor_after == date(2026, 8, 28)
    assert all(v == date(2026, 8, 28) for v in committed), committed


def test_cursor_floor_falls_back_to_observed_when_incomparable(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A STRING cursor observing ints against a str prior: no crash, the
    observed value lands (the pre-clamp behaviour) and a warning says why."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        cursor.observe(5)
        yield [{"id": 5, "updated_at": 5}]

    committed: list[Any] = []
    with caplog.at_level(logging.WARNING, logger=LOG.name):
        result = _run_one_stream(
            _stream_def(CursorType.STRING),
            _make_source(gen),  # type: ignore[arg-type]
            _hooks_with_log([], cursor_values=committed),
            conn=object(),
            run_config=_run_config(),
            pipeline=_pipeline(),
            prior=_prior("abc", CursorType.STRING),
            log=LOG,
        )
    assert result.cursor_after == 5 and committed[-1] == 5
    assert any("not comparable" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# (g) the engine applies incremental.lookback to the resume point
# ---------------------------------------------------------------------------


def _lookback_stream_def(
    cursor_type: CursorType, lookback: str | None, ordered: bool = False
) -> StreamDef:
    base = _incremental_stream_def()
    return StreamDef(
        name=base.name,
        table=base.table,
        primary_key=base.primary_key,
        write_disposition=base.write_disposition,
        incremental=Incremental(
            cursor_field="updated_at", cursor_type=cursor_type, lookback=lookback, ordered=ordered
        ),
        schema=base.schema,
    )


def _start_value_seen(
    stream_def: StreamDef, prior: StateRecord | None, observed: list[Any]
) -> Any:
    """Run a one-batch stream and return the start value the engine handed it."""
    seen: list[Any] = []

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        seen.append(cursor.start_value())
        for value in observed:
            cursor.observe(value)
        yield [{"id": 1, "updated_at": observed[-1]}]

    _run_one_stream(
        stream_def,
        _make_source(gen),  # type: ignore[arg-type]
        _hooks_with_log([]),
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=prior,
        log=LOG,
    )
    return seen[0]


def test_lookback_is_subtracted_from_the_persisted_cursor() -> None:
    """A resumed run starts at cursor minus lookback, per cursor type."""
    from datetime import UTC, date, datetime, timedelta

    # int cursor, unit suffix -> seconds (a Unix-timestamp cursor).
    assert _start_value_seen(
        _lookback_stream_def(CursorType.INT, "6h"), _prior(1_000_000), [1_000_100]
    ) == 1_000_000 - 21600
    # int cursor, bare number -> subtracted as-is.
    assert _start_value_seen(
        _lookback_stream_def(CursorType.INT, "100"), _prior(1_000), [1_100]
    ) == 900
    # timestamp cursor persisted as the ISO string _dtex_state hands back.
    ts = datetime(2026, 9, 7, 12, 0, tzinfo=UTC)
    assert _start_value_seen(
        _lookback_stream_def(CursorType.TIMESTAMP, "2d"),
        _prior(ts.isoformat(), CursorType.TIMESTAMP),
        [ts + timedelta(hours=1)],
    ) == ts - timedelta(days=2)
    # date cursor: a 6h lookback rounds up to a whole day.
    assert _start_value_seen(
        _lookback_stream_def(CursorType.DATE, "6h"),
        _prior("2026-09-07", CursorType.DATE),
        [date(2026, 9, 8)],
    ) == date(2026, 9, 6)


def test_lookback_does_not_apply_to_initial_value_or_since() -> None:
    """The first run starts AT initial_value; a `since:` override is verbatim."""
    base = _incremental_stream_def()
    stream_def = StreamDef(
        name=base.name,
        table=base.table,
        primary_key=base.primary_key,
        write_disposition=base.write_disposition,
        incremental=Incremental(
            cursor_field="updated_at", cursor_type=CursorType.INT, lookback="100",
            initial_value="500",
        ),
        schema=base.schema,
    )
    assert _start_value_seen(stream_def, None, [600]) == 500

    seen: list[Any] = []

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        seen.append(cursor.start_value())
        yield [{"id": 1, "updated_at": 900}]

    pipeline = PipelineConfig(
        name="p",
        source="src",
        destination="dst",
        streams={"rows": StreamRunConfig(since=800)},
        all_streams=False,
    )
    _run_one_stream(
        stream_def,
        _make_source(gen),  # type: ignore[arg-type]
        _hooks_with_log([]),
        conn=object(),
        run_config=_run_config(),
        pipeline=pipeline,
        prior=_prior(1_000),
        log=LOG,
    )
    assert seen == [800]


# ---------------------------------------------------------------------------
# (h) mid-stream flushes advance the cursor only for `ordered` streams
# ---------------------------------------------------------------------------


def _run_flushing_stream(
    stream_def: StreamDef, observed: list[int], crash_on: int | None
) -> list[Any]:
    committed: list[Any] = []
    hooks = _hooks_with_log([], write_batch_raises_on=crash_on, cursor_values=committed)

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for value in observed:
            cursor.observe(value)
            yield [{"id": value, "updated_at": value}]

    run = lambda: _run_one_stream(  # noqa: E731
        stream_def,
        _make_source(gen),  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=_prior(100),
        log=LOG,
    )
    if crash_on is None:
        run()
    else:
        with pytest.raises(RuntimeError, match="simulated mid-stream crash"):
            run()
    return committed


def test_unordered_stream_keeps_prior_cursor_until_it_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A per-object walk yields 300 before it gets to 150: a flush that
    persisted 300 would let a crash skip 150 forever. The default (unordered)
    stream flushes the PRIOR cursor and advances only in the final commit."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)
    stream_def = _lookback_stream_def(CursorType.INT, None, ordered=False)
    committed = _run_flushing_stream(stream_def, [300, 150, 400], crash_on=None)
    # two mid-stream flushes at the prior value, then the final commit at the max
    assert committed == [100, 100, 100, 400], committed


def test_unordered_stream_that_crashes_leaves_the_prior_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)
    stream_def = _lookback_stream_def(CursorType.INT, None, ordered=False)
    committed = _run_flushing_stream(stream_def, [300, 150, 400], crash_on=3)
    assert committed == [100, 100], committed


def test_ordered_stream_flushes_the_observed_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    """`ordered: true` opts back into the resume-from-last-batch behaviour."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)
    stream_def = _lookback_stream_def(CursorType.INT, None, ordered=True)
    committed = _run_flushing_stream(stream_def, [110, 120, 130], crash_on=3)
    assert committed == [110, 120], committed


# ---------------------------------------------------------------------------
# (i) merge batches are collapsed to one row per primary key
# ---------------------------------------------------------------------------


def test_merge_batch_keeps_last_row_per_primary_key() -> None:
    base = _incremental_stream_def()
    stream_def = StreamDef(
        name=base.name,
        table=base.table,
        primary_key=("id",),
        write_disposition=WriteDisposition.MERGE,
        incremental=None,
        schema=base.schema,
    )
    written: list[Batch] = []
    hooks = _hooks_with_log([])
    real_write = hooks["write_batch"]

    def write_batch(conn: Any, batch: Batch, meta: Any) -> int:
        written.append(list(batch))
        return real_write(conn, batch, meta)

    hooks["write_batch"] = write_batch

    def gen(config: Config, state: Any, log: Any) -> Iterator[Batch]:
        yield [
            {"id": 1, "updated_at": 10},
            {"id": 2, "updated_at": 20},
            {"id": 1, "updated_at": 30},  # same key again — the freshest wins
        ]

    result = _run_one_stream(
        stream_def,
        _make_source(gen),  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=None,
        log=LOG,
    )
    assert written == [[{"id": 2, "updated_at": 20}, {"id": 1, "updated_at": 30}]]
    assert result.rows_extracted == 3 and result.rows_loaded == 2


def test_append_batch_is_not_deduped() -> None:
    stream_def = _incremental_stream_def()  # append
    written: list[Batch] = []
    hooks = _hooks_with_log([])
    real_write = hooks["write_batch"]

    def write_batch(conn: Any, batch: Batch, meta: Any) -> int:
        written.append(list(batch))
        return real_write(conn, batch, meta)

    hooks["write_batch"] = write_batch

    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        cursor.observe(10)
        yield [{"id": 1, "updated_at": 10}, {"id": 1, "updated_at": 10}]

    _run_one_stream(
        stream_def,
        _make_source(gen),  # type: ignore[arg-type]
        hooks,
        conn=object(),
        run_config=_run_config(),
        pipeline=_pipeline(),
        prior=None,
        log=LOG,
    )
    assert len(written[0]) == 2
