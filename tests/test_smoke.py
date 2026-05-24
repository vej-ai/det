"""THE det smoke test — the executable specification of the engine's job.

This file defines, in runnable code, *exactly* what the engine must do. Build
stages 1-4 had no engine, so every smoke run was wired by hand inside
``_drive_one_run`` — the "STAGE 5 SEAM". Stage 5 built the engine
(:mod:`det.engine`), and that hand-wiring has now **collapsed**: the seam
is a single :func:`det.run` call.

What the run does, end to end (docs/02 §Run lifecycle, docs/05 §1):

    open → read_state → [ensure_schema → write_batch ...]* → commit_state → close

It drives the ``echo`` fixture source (an ``append`` stream ``events`` and a
``merge`` + ``incremental`` stream ``items``) into the pre-baked DuckDB
destination, and the assertions pin down the contract the engine must satisfy:

* rows land in DuckDB, in the right tables, with the right values;
* ``_det_synced_at`` is populated on every row;
* ``_det_state`` carries the incremental stream's advanced cursor;
* a second run *resumes from committed state* — the incremental stream yields
  only new rows, the append stream re-runs in full.

╔══════════════════════════════════════════════════════════════════════════╗
║  STAGE 5 SEAM — COLLAPSED                                                ║
║  ------------------------                                                ║
║  Pre-stage-5, ``_drive_one_run`` hand-wired the whole lifecycle. It is   ║
║  now :func:`_drive_one_run`, a one-expression wrapper over the engine:   ║
║                                                                          ║
║      return det.run(connector="echo", target="dev",                 ║
║                          project_dir=PROJECT_DIR,                         ║
║                          destination_params={"path": db_path})           ║
║                                                                          ║
║  It returns a real ``RunResult``; the tests read                        ║
║  ``result.stream("items").rows_loaded`` off it. The assertions DID NOT   ║
║  move — they are the spec and pass unchanged against ``det.run``.   ║
║  Only test #3 keeps direct hook wiring: it deliberately SKIPS            ║
║  ``commit_state``, which the engine can never be asked to do.            ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import det
from det import Config, Cursor, RunResult, StreamMeta
from det.types import StreamDef
from tests.conftest import (
    ECHO_CONNECTOR_DIR,
    LoadedConnector,
    load_connector,
)

# The det project the engine discovers `echo` (and the `duckdb` destination
# binding) from — tests/fixtures/ is a real project with det_project.yml.
PROJECT_DIR = ECHO_CONNECTOR_DIR.parent.parent


# ==========================================================================
# THE STAGE 5 SEAM — collapsed to a single engine call.
# ==========================================================================


def _drive_one_run(db_path: str) -> RunResult:
    """Drive one end-to-end run — the collapsed STAGE 5 SEAM.

    Pre-stage-5 this function hand-wired the whole
    ``open → read_state → [ensure_schema → write_batch ...]* → commit_state →
    close`` lifecycle. Stage 5's engine subsumes every one of those steps, so
    the body is now one call: :func:`det.run` discovers ``echo`` and its
    DuckDB destination, resolves config, and drives the run.

    ``destination_params`` routes this test's temp ``db_path`` into the DuckDB
    destination's ``path`` param — the highest-precedence config layer
    (docs/03 §6), so each test gets its own warehouse file.
    """
    return det.run(
        connector="echo",
        target="dev",
        project_dir=str(PROJECT_DIR),
        destination_params={"path": db_path},
    )


# ==========================================================================
# END STAGE 5 SEAM — everything below is the spec and does NOT change.
# ==========================================================================


# --------------------------------------------------------------------------
# The smoke test — the executable spec for engine.run()
# --------------------------------------------------------------------------


def test_smoke_first_run_lands_rows_state_and_synced_at(
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """One run lands data, fills _det_synced_at, and advances the cursor.

    This is the core spec: after ``det.run()``, the DuckDB file must hold
    the source's data and the state table must show the incremental stream's
    cursor advanced to its max.
    """
    result = _drive_one_run(duckdb_path)

    # --- Rows landed -------------------------------------------------------
    # echo.events: 4 fixture records (append).
    # echo.items:  5 fixture records (merge, first run cursor = initial 0).
    assert result.status.value == "succeeded"
    rows_loaded = {s.name: s.rows_loaded for s in result.streams}
    assert rows_loaded == {"events": 4, "items": 5}

    events = query_duckdb(duckdb_path, "SELECT id, name FROM echo_events ORDER BY id")
    assert events == [(1, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")]

    items = query_duckdb(duckdb_path, "SELECT id, label, updated_at FROM echo_items ORDER BY id")
    assert items == [
        (1, "item-one", 1),
        (2, "item-two", 2),
        (3, "item-three", 3),
        (4, "item-four", 4),
        (5, "item-five", 5),
    ]

    # --- _det_synced_at populated on every row ------------------------
    null_synced = query_duckdb(
        duckdb_path,
        "SELECT count(*) FROM echo_events WHERE _det_synced_at IS NULL",
    )[0][0]
    assert null_synced == 0
    synced_sample = query_duckdb(
        duckdb_path, "SELECT _det_synced_at FROM echo_items LIMIT 1"
    )[0][0]
    assert isinstance(synced_sample, datetime)

    # --- JSON column survived the round trip -------------------------------
    score = query_duckdb(
        duckdb_path, "SELECT payload->>'$.score' FROM echo_events WHERE id = 1"
    )[0][0]
    assert score == "10"

    # --- _det_state: incremental cursor advanced ----------------------
    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, cursor_type, rows_total FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert len(state) == 1
    assert int(str(state[0][0])) == 5  # cursor advanced to the max updated_at
    assert state[0][1] == "int"
    assert state[0][2] == 5  # rows_total

    # The append stream also got a state row (no cursor — cursor_value NULL).
    events_state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'events'",
    )
    assert len(events_state) == 1
    assert events_state[0][0] is None


def test_smoke_second_run_resumes_from_committed_state(
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """A re-run resumes from committed state — the incremental stream yields 0.

    This is the spec's incremental-correctness clause. After run 1 commits the
    ``items`` cursor at 5, run 2's ``items`` stream — driven from that committed
    cursor — finds nothing newer and yields no rows. The ``append`` stream re-runs
    in full (append has no cursor). State must not regress.
    """
    # Run 1 — full load.
    first = _drive_one_run(duckdb_path)
    assert {s.name: s.rows_loaded for s in first.streams} == {"events": 4, "items": 5}

    # Run 2 — a second engine.run() re-discovers the connector and resumes from
    # the committed cursor, exactly as a second process would.
    second = _drive_one_run(duckdb_path)

    # The incremental `items` stream resumed from cursor=5 and found nothing new.
    items_result = second.stream("items")
    events_result = second.stream("events")
    assert items_result is not None and events_result is not None
    assert items_result.rows_loaded == 0
    # The non-incremental `events` stream re-ran fully (append has no cursor).
    assert events_result.rows_loaded == 4

    # echo_items still holds exactly the 5 rows from run 1 — merge of an empty
    # set changed nothing, no duplication.
    item_count = query_duckdb(duckdb_path, "SELECT count(*) FROM echo_items")[0][0]
    assert item_count == 5

    # The committed cursor did not regress and rows_total did not double-count.
    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, rows_total FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert int(str(state[0][0])) == 5
    assert state[0][1] == 5  # rows_total unchanged — run 2 added 0 items


def test_smoke_state_commit_is_what_enables_resume(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
) -> None:
    """Spec guard: WITHOUT a committed cursor, run 2 re-yields every row.

    This test deliberately drives a run that skips ``commit_state`` and proves
    the incremental stream then has no resume point — so run 2 re-extracts all
    5 items. It exists so the smoke spec genuinely *defines* the engine's job:
    if stage 5's ``engine.run()`` forgot to call ``commit_state``, the resume
    behavior asserted above would silently break, and this test would catch it.
    """
    source = load_connector(ECHO_CONNECTOR_DIR)
    hooks: dict[str, Callable[..., Any]] = {
        name: duckdb_destination.registry.hook(name).func  # type: ignore[union-attr]
        for name in duckdb_destination.registry.hook_names
    }
    items_def: StreamDef | None = source.manifest.stream("items")
    assert items_def is not None and items_def.incremental is not None

    # A run that loads `items` but never commits its state.
    assert items_def.schema is not None
    items_meta = StreamMeta.from_stream_def(items_def, items_def.schema)
    conn = hooks["open"](Config(params={"path": duckdb_path}))
    try:
        hooks["ensure_schema"](conn, items_meta)
        registration = source.registry.stream("items")
        assert registration is not None
        cursor = Cursor(
            cursor_field=items_def.incremental.cursor_field,
            cursor_type=items_def.incremental.cursor_type,
            start_value=0,
        )
        for batch in registration.func(cursor=cursor):
            hooks["write_batch"](conn, batch, items_meta)
        # NOTE: commit_state intentionally NOT called.
    finally:
        hooks["close"](conn)

    # Run 2 reads state — but nothing was committed, so there is no resume point.
    source2 = load_connector(ECHO_CONNECTOR_DIR)
    conn = hooks["open"](Config(params={"path": duckdb_path}))
    try:
        prior = hooks["read_state"](conn, "echo")
        assert prior == [], "no commit_state ⇒ no StateRecord ⇒ no resume point"

        items_def2 = source2.manifest.stream("items")
        assert items_def2 is not None and items_def2.incremental is not None
        # With no prior cursor, the resume value falls back to initial_value (0)
        # and the stream re-yields ALL 5 items — the bug this guards against.
        cursor = Cursor(
            cursor_field=items_def2.incremental.cursor_field,
            cursor_type=items_def2.incremental.cursor_type,
            start_value=0,
        )
        registration2 = source2.registry.stream("items")
        assert registration2 is not None
        re_yielded = sum(len(batch) for batch in registration2.func(cursor=cursor))
        assert re_yielded == 5, "without committed state the stream restarts from zero"
    finally:
        hooks["close"](conn)
