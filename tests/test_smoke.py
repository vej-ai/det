"""THE det smoke test — the executable specification of the engine's job.

Stage 5 built the engine; stage 8.B made *configs* the runtime unit. The seam
:func:`_drive_one_run` is now a one-line call to :func:`det.run` with a
config NAME (the fixture's ``echo_dev``) — discovery + RESOLVE + the full
lifecycle collapse into that one call.

What the run does, end to end (docs/02 §Run lifecycle, docs/05 §1)::

    open → read_state → [ensure_schema → write_batch ...]* → commit_state → close

It drives the ``echo`` fixture source (an ``append`` stream ``events`` and a
``merge`` + ``incremental`` stream ``items``) into the pre-baked DuckDB
destination (bound via the ``echo_dev`` config), and the assertions pin down
the contract the engine must satisfy.
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

# The det project the engine discovers `echo` and the `echo_dev` config from —
# tests/fixtures/ is a real project with det_project.yml + configs/echo.yml.
PROJECT_DIR = ECHO_CONNECTOR_DIR.parent.parent


# ==========================================================================
# The seam — one call to det.run(config=…) drives the full lifecycle.
# ==========================================================================


def _drive_one_run(db_path: str) -> RunResult:
    """Drive one end-to-end run via the fixture's ``echo_dev`` config.

    ``destination_params_override`` routes this test's temp ``db_path`` into
    the DuckDB destination's ``path`` param — the highest-precedence
    destination-config layer (docs/12), so each test gets its own warehouse
    file.
    """
    return det.run(
        config="echo_dev",
        project_dir=str(PROJECT_DIR),
        destination_params_override={"path": db_path},
    )


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

    null_synced = query_duckdb(
        duckdb_path,
        "SELECT count(*) FROM echo_events WHERE _det_synced_at IS NULL",
    )[0][0]
    assert null_synced == 0
    synced_sample = query_duckdb(
        duckdb_path, "SELECT _det_synced_at FROM echo_items LIMIT 1"
    )[0][0]
    assert isinstance(synced_sample, datetime)

    score = query_duckdb(
        duckdb_path, "SELECT payload->>'$.score' FROM echo_events WHERE id = 1"
    )[0][0]
    assert score == "10"

    # _det_state.connector is the SOURCE name, not the config name — state is
    # a property of where the data was extracted from, not which pipeline.
    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, cursor_type, rows_total FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert len(state) == 1
    assert int(str(state[0][0])) == 5
    assert state[0][1] == "int"
    assert state[0][2] == 5

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
    """A re-run resumes from committed state — the incremental stream yields 0."""
    first = _drive_one_run(duckdb_path)
    assert {s.name: s.rows_loaded for s in first.streams} == {"events": 4, "items": 5}

    second = _drive_one_run(duckdb_path)

    items_result = second.stream("items")
    events_result = second.stream("events")
    assert items_result is not None and events_result is not None
    assert items_result.rows_loaded == 0
    assert events_result.rows_loaded == 4

    item_count = query_duckdb(duckdb_path, "SELECT count(*) FROM echo_items")[0][0]
    assert item_count == 5

    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, rows_total FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert int(str(state[0][0])) == 5
    assert state[0][1] == 5


def test_smoke_state_commit_is_what_enables_resume(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
) -> None:
    """Spec guard: WITHOUT a committed cursor, run 2 re-yields every row."""
    source = load_connector(ECHO_CONNECTOR_DIR)
    hooks: dict[str, Callable[..., Any]] = {
        name: duckdb_destination.registry.hook(name).func  # type: ignore[union-attr]
        for name in duckdb_destination.registry.hook_names
    }
    items_def: StreamDef | None = source.manifest.stream("items")
    assert items_def is not None and items_def.incremental is not None

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

    source2 = load_connector(ECHO_CONNECTOR_DIR)
    conn = hooks["open"](Config(params={"path": duckdb_path}))
    try:
        prior = hooks["read_state"](conn, "echo")
        assert prior == [], "no commit_state ⇒ no StateRecord ⇒ no resume point"

        items_def2 = source2.manifest.stream("items")
        assert items_def2 is not None and items_def2.incremental is not None
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
