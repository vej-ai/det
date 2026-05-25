"""Direct unit tests of the DuckDB destination hooks — docs/05.

Exercises ``det/destinations/duckdb/`` hook by hook, without an engine:
``capabilities`` / ``open`` / ``ensure_schema`` / ``write_batch`` (all three
write dispositions) / ``read_state`` / ``commit_state`` / ``close``, plus
schema evolution, JSON columns and identifier safety.

The hooks are obtained the way the engine will obtain them — from the
:class:`~det.registry.ConnectorRegistry` the connector folder's decorators
populated (loaded via the ``conftest`` harness).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from det import (
    Capability,
    Config,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    RunRecord,
    RunStatus,
    Schema,
    StateRecord,
    StreamMeta,
    StreamResult,
    WriteDisposition,
)
from det.destinations.duckdb.ddl import (
    duckdb_type,
    qualified_table,
    quote_identifier,
    validate_identifier,
)
from tests.conftest import LoadedConnector

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _hooks(dest: LoadedConnector) -> dict[str, Callable[..., Any]]:
    """Return the destination's hook functions keyed by hook name."""
    return {name: dest.registry.hook(name).func for name in dest.registry.hook_names}  # type: ignore[union-attr]


def _open(dest: LoadedConnector, path: str, **params: Any) -> Any:
    """Open the destination at ``path`` and return the DuckConn handle."""
    hooks = _hooks(dest)
    return hooks["open"](Config(params={"path": path, **params}))


_EVENTS_SCHEMA = Schema(
    fields=(
        Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
        Field(name="name", type=FieldType.STRING),
        Field(name="payload", type=FieldType.JSON),
    )
)


def _events_meta(
    disposition: WriteDisposition = WriteDisposition.APPEND,
    *,
    schema: Schema = _EVENTS_SCHEMA,
) -> StreamMeta:
    """Build a StreamMeta for the ``echo_events`` table — the hooks' one metadata arg."""
    return StreamMeta(
        table="echo_events", write_disposition=disposition, schema=schema
    )


# --------------------------------------------------------------------------
# capabilities — docs/05 §1
# --------------------------------------------------------------------------


def test_capabilities_declares_tier_a_merge_evolution(duckdb_destination: LoadedConnector) -> None:
    """DuckDB declares STATE, MERGE, SCHEMA_EVOLUTION, TRANSACTIONAL_LOAD and RUN_RECORDS."""
    caps = _hooks(duckdb_destination)["capabilities"]()
    assert caps == {
        Capability.STATE,
        Capability.MERGE,
        Capability.SCHEMA_EVOLUTION,
        Capability.TRANSACTIONAL_LOAD,
        Capability.RUN_RECORDS,
    }


def test_all_mandatory_and_state_hooks_registered(duckdb_destination: LoadedConnector) -> None:
    """DuckDB is Tier A: it defines the state hooks, not state_backend."""
    names = set(duckdb_destination.registry.hook_names)
    # Unconditionally mandatory hooks are all present.
    assert duckdb_destination.registry.missing_mandatory_hooks() == ()
    # Tier A ⇒ read_state / commit_state present, state_backend absent.
    assert {"read_state", "commit_state"} <= names
    assert "state_backend" not in names


# --------------------------------------------------------------------------
# ddl helpers — type mapping + identifier safety
# --------------------------------------------------------------------------


def test_field_type_mapping_covers_every_type() -> None:
    """Every FieldType maps to its documented DuckDB type — docs/05 §3.1."""
    assert duckdb_type(FieldType.STRING) == "VARCHAR"
    assert duckdb_type(FieldType.INTEGER) == "BIGINT"
    assert duckdb_type(FieldType.FLOAT) == "DOUBLE"
    assert duckdb_type(FieldType.BOOLEAN) == "BOOLEAN"
    assert duckdb_type(FieldType.TIMESTAMP) == "TIMESTAMP"
    assert duckdb_type(FieldType.DATE) == "DATE"
    assert duckdb_type(FieldType.JSON) == "JSON"
    # Total over the enum — no member is unmapped.
    for ft in FieldType:
        assert isinstance(duckdb_type(ft), str)


def test_identifier_validation_rejects_injection() -> None:
    """A non-identifier table/column name is rejected before it reaches SQL."""
    for bad in ('users"; DROP TABLE x; --', "has space", "1leading", "", "a-b"):
        with pytest.raises(ValueError, match="unsafe"):
            validate_identifier(bad, kind="table")


def test_identifier_validation_allows_underscore_prefixed() -> None:
    """Engine-owned names (_det_state, _det_synced_at) are valid."""
    assert validate_identifier("_det_state", kind="table") == "_det_state"
    assert validate_identifier("_det_synced_at", kind="column") == "_det_synced_at"


def test_quote_identifier_and_qualified_table() -> None:
    """Quoting wraps in double-quotes; qualified_table prefixes the schema."""
    assert quote_identifier("orders", kind="table") == '"orders"'
    assert qualified_table(None, "orders") == '"orders"'
    assert qualified_table("analytics", "orders") == '"analytics"."orders"'


# --------------------------------------------------------------------------
# ensure_schema — table creation, synced_at, evolution — docs/05 §3
# --------------------------------------------------------------------------


def test_ensure_schema_creates_table_with_synced_at(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """ensure_schema creates the table and appends _det_synced_at."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())
    hooks["close"](conn)

    cols = {
        r[0]
        for r in query_duckdb(
            duckdb_path,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'echo_events'",
        )
    }
    assert cols == {"id", "name", "payload", Schema.SYNCED_AT_COLUMN}


def test_ensure_schema_is_idempotent(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """Calling ensure_schema twice (a resumed run) does not error."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())
    hooks["ensure_schema"](conn, _events_meta())  # no raise
    hooks["close"](conn)


def test_ensure_schema_additive_evolution_adds_column(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """A new declared field is added via ALTER TABLE ADD COLUMN — docs/05 §3.2."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)

    hooks["ensure_schema"](conn, _events_meta())
    # Re-run with an extra declared column — additive evolution must add it.
    evolved = Schema(fields=(*_EVENTS_SCHEMA.fields, Field(name="amount", type=FieldType.FLOAT)))
    hooks["ensure_schema"](conn, _events_meta(schema=evolved))
    hooks["close"](conn)

    cols = {
        r[0]
        for r in query_duckdb(
            duckdb_path,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'echo_events'",
        )
    }
    assert "amount" in cols


# --------------------------------------------------------------------------
# write_batch — append — docs/05 §4
# --------------------------------------------------------------------------


def test_write_batch_append_accumulates(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """append: each batch inserts; rows accumulate across calls — docs/05 §4."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())

    n1 = hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())
    n2 = hooks["write_batch"](
        conn, [{"id": 2, "name": "b"}, {"id": 3, "name": "c"}], _events_meta()
    )
    hooks["close"](conn)

    assert (n1, n2) == (1, 2)
    count = query_duckdb(duckdb_path, "SELECT count(*) FROM echo_events")[0][0]
    assert count == 3


def test_write_batch_stamps_synced_at(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """write_batch fills _det_synced_at when a record lacks it — docs/03 §2.2.1."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())
    hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())
    hooks["close"](conn)

    synced = query_duckdb(
        duckdb_path, f"SELECT {Schema.SYNCED_AT_COLUMN} FROM echo_events"
    )[0][0]
    assert isinstance(synced, datetime)


def test_write_batch_empty_append_is_noop(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """An empty append batch writes nothing and returns 0."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())
    assert hooks["write_batch"](conn, [], _events_meta()) == 0
    hooks["close"](conn)


# --------------------------------------------------------------------------
# write_batch — merge — docs/05 §4
# --------------------------------------------------------------------------


_ITEMS_SCHEMA = Schema(
    fields=(
        Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
        Field(name="label", type=FieldType.STRING),
    )
)


def _items_meta(
    disposition: WriteDisposition = WriteDisposition.APPEND,
    *,
    primary_key: tuple[str, ...] = (),
) -> StreamMeta:
    """Build a StreamMeta for the ``echo_items`` table — the hooks' one metadata arg."""
    return StreamMeta(
        table="echo_items",
        write_disposition=disposition,
        schema=_ITEMS_SCHEMA,
        primary_key=primary_key,
    )


def test_write_batch_merge_upserts(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """merge: new keys insert, existing keys overwrite in place — docs/05 §4."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _items_meta())

    merge_meta = _items_meta(WriteDisposition.MERGE, primary_key=("id",))
    hooks["write_batch"](
        conn,
        [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}],
        merge_meta,
    )
    # Re-merge: id=1 overwritten, id=3 inserted, id=2 untouched.
    hooks["write_batch"](
        conn,
        [{"id": 1, "label": "ONE-v2"}, {"id": 3, "label": "three"}],
        merge_meta,
    )
    hooks["close"](conn)

    rows = dict(query_duckdb(duckdb_path, "SELECT id, label FROM echo_items"))
    assert rows == {1: "ONE-v2", 2: "two", 3: "three"}


def test_write_batch_merge_requires_primary_key(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """merge without a primary_key fails fast with a clear message."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _items_meta())
    # A merge StreamMeta with no primary_key — StreamMeta itself does not
    # validate, so this is constructible; write_batch is what must raise.
    with pytest.raises(ValueError, match="primary_key"):
        hooks["write_batch"](
            conn, [{"id": 1, "label": "x"}], _items_meta(WriteDisposition.MERGE)
        )
    hooks["close"](conn)


# --------------------------------------------------------------------------
# write_batch — replace — docs/05 §4
# --------------------------------------------------------------------------


def test_write_batch_replace_truncates_once_per_run(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """replace: first batch truncates, later batches in the SAME run append."""
    hooks = _hooks(duckdb_destination)

    # Run 1 — seed two rows via append.
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _items_meta())
    hooks["write_batch"](
        conn, [{"id": 9, "label": "old-a"}, {"id": 8, "label": "old-b"}], _items_meta()
    )
    hooks["close"](conn)

    # Run 2 — replace with two batches: the first truncates, the second must
    # NOT re-truncate (or batch 1's rows would vanish).
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _items_meta())
    replace_meta = _items_meta(WriteDisposition.REPLACE)
    hooks["write_batch"](conn, [{"id": 1, "label": "new-1"}], replace_meta)
    hooks["write_batch"](conn, [{"id": 2, "label": "new-2"}], replace_meta)
    hooks["close"](conn)

    rows = dict(query_duckdb(duckdb_path, "SELECT id, label FROM echo_items"))
    assert rows == {1: "new-1", 2: "new-2"}  # old rows gone, both new rows kept


def test_write_batch_replace_empty_batch_still_truncates(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """replace with an empty batch is a valid empty snapshot — it truncates."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _items_meta())
    hooks["write_batch"](conn, [{"id": 1, "label": "x"}], _items_meta())
    hooks["write_batch"](conn, [], _items_meta(WriteDisposition.REPLACE))
    hooks["close"](conn)

    count = query_duckdb(duckdb_path, "SELECT count(*) FROM echo_items")[0][0]
    assert count == 0


# --------------------------------------------------------------------------
# JSON columns
# --------------------------------------------------------------------------


def test_json_column_round_trip(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """dict / list values land in a JSON column and read back faithfully."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["ensure_schema"](conn, _events_meta())
    hooks["write_batch"](
        conn,
        [
            {"id": 1, "name": "a", "payload": {"tags": ["x", "y"], "n": 3}},
            {"id": 2, "name": "b", "payload": ["plain", "list"]},
        ],
        _events_meta(),
    )
    hooks["close"](conn)

    # Read the JSON column back out as extracted values to prove structure survived.
    tags = query_duckdb(
        duckdb_path,
        "SELECT payload->>'$.tags[0]' FROM echo_events WHERE id = 1",
    )[0][0]
    assert tags == "x"
    list_val = query_duckdb(
        duckdb_path,
        "SELECT payload->>'$[1]' FROM echo_events WHERE id = 2",
    )[0][0]
    assert list_val == "list"


# --------------------------------------------------------------------------
# read_state / commit_state — the _det_state table — docs/05 §5
# --------------------------------------------------------------------------


def test_read_state_empty_on_first_run(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """read_state returns [] before any state is committed (table created lazily)."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    assert hooks["read_state"](conn, "echo") == []
    hooks["close"](conn)


def test_state_table_has_eight_canonical_columns(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """_det_state has exactly the 8 columns of StateRecord.to_row()."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["read_state"](conn, "echo")  # lazily creates the table
    hooks["close"](conn)

    cols = {
        r[0]
        for r in query_duckdb(
            duckdb_path,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = '_det_state'",
        )
    }
    assert cols == set(
        StateRecord(connector="c", stream="s").to_row().keys()
    )


def test_commit_state_then_read_state_round_trip(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """A StateRecord survives commit_state -> read_state with every field intact."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)

    original = StateRecord(
        connector="echo",
        stream="items",
        cursor_value=5,
        cursor_type=CursorType.INT,
        state_blob={"page_token": "abc", "nested": {"k": [1, 2]}},
        last_run_id="run-001",
        rows_total=42,
        updated_at=datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC),
    )
    hooks["commit_state"](conn, "run-001", [original])
    hooks["close"](conn)

    # Reopen — a fresh connection, so this proves durable persistence.
    conn = _open(duckdb_destination, duckdb_path)
    loaded = hooks["read_state"](conn, "echo")
    hooks["close"](conn)

    assert len(loaded) == 1
    rec = loaded[0]
    assert rec.connector == "echo"
    assert rec.stream == "items"
    assert rec.cursor_value == 5
    assert rec.cursor_type is CursorType.INT
    assert rec.state_blob == {"page_token": "abc", "nested": {"k": [1, 2]}}
    assert rec.last_run_id == "run-001"
    assert rec.rows_total == 42
    # NOTE: docs/05 §3.1 maps the `timestamp` logical type to DuckDB's plain
    # `TIMESTAMP`, which is timezone-NAIVE — so the UTC offset is not stored.
    # The wall-clock instant round-trips exactly; the tzinfo does not. This is
    # the documented type mapping, not a bug. A future TIMESTAMP_TZ mapping
    # would preserve the offset.
    assert rec.updated_at == datetime(2026, 5, 22, 12, 0, 0)


def test_commit_state_round_trip_string_cursor_value(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """A *string* cursor_value commits and reads back intact.

    Regression: stage-7 connector builds (REST, Filesystem, ShipHero, Stripe
    where applicable) all hit the same bug — ``cursor_value`` lands in a
    DuckDB ``JSON`` column, and a bare scalar string like
    ``"2026-05-20T00:00:00"`` is not valid JSON text, so commit raised
    ``ConversionException``. The fix routes state binds through
    ``_encode_json_column`` (which serializes scalars too), distinct from the
    typed-column data-insert path. This test pins the fix in.
    """
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)

    rec = StateRecord(
        connector="src",
        stream="rows",
        cursor_value="2026-05-20T00:00:00",
        cursor_type=CursorType.STRING,
        rows_total=7,
    )
    hooks["commit_state"](conn, "run-str", [rec])
    hooks["close"](conn)

    conn = _open(duckdb_destination, duckdb_path)
    loaded = hooks["read_state"](conn, "src")
    hooks["close"](conn)

    assert len(loaded) == 1
    assert loaded[0].cursor_value == "2026-05-20T00:00:00"
    assert loaded[0].cursor_type is CursorType.STRING
    assert loaded[0].rows_total == 7


def test_commit_state_upserts_on_connector_stream_key(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """A second commit for the same (connector, stream) advances the row in place."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)

    hooks["commit_state"](
        conn,
        "run-1",
        [StateRecord(connector="echo", stream="items", cursor_value=3, rows_total=3)],
    )
    hooks["commit_state"](
        conn,
        "run-2",
        [StateRecord(connector="echo", stream="items", cursor_value=9, rows_total=12)],
    )
    hooks["close"](conn)

    rows = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, rows_total FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    # One row, not two — upserted on (connector, stream).
    assert len(rows) == 1
    assert int(str(rows[0][0])) == 9
    assert rows[0][1] == 12


def test_commit_state_stamps_run_id_and_timestamp(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """commit_state fills last_run_id / updated_at when the record left them unset."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["commit_state"](
        conn, "run-xyz", [StateRecord(connector="echo", stream="events")]
    )
    loaded = hooks["read_state"](conn, "echo")
    hooks["close"](conn)

    assert loaded[0].last_run_id == "run-xyz"
    assert isinstance(loaded[0].updated_at, datetime)


# --------------------------------------------------------------------------
# close
# --------------------------------------------------------------------------


def test_close_is_safe_to_call_twice(
    duckdb_destination: LoadedConnector, duckdb_path: str
) -> None:
    """close never raises — even on an already-closed connection (docs/05 §1)."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["close"](conn)
    hooks["close"](conn)  # second call must not raise


# --------------------------------------------------------------------------
# dataset (schema) routing
# --------------------------------------------------------------------------


def test_dataset_param_places_tables_in_schema(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """The `dataset` param puts loaded tables inside a DuckDB schema."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path, dataset="analytics")
    hooks["ensure_schema"](conn, _events_meta())
    hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())
    hooks["close"](conn)

    schema = query_duckdb(
        duckdb_path,
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name = 'echo_events'",
    )[0][0]
    assert schema == "analytics"


# --------------------------------------------------------------------------
# write_run_record — _det_runs audit table — docs/09 §4
# --------------------------------------------------------------------------


def _build_record(
    run_id: str = "run-abc123",
    *,
    status: RunStatus = RunStatus.SUCCEEDED,
    rows_loaded: int = 5,
    streams: tuple[StreamResult, ...] = (),
    error_type: str | None = None,
    error_message: str | None = None,
) -> RunRecord:
    """Build a RunRecord for the write_run_record hook tests."""
    started = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
    ended = datetime(2026, 5, 25, 12, 0, 5, tzinfo=UTC)
    return RunRecord(
        run_id=run_id,
        config="echo_dev",
        source="echo",
        destination="duckdb",
        target="dev",
        status=status,
        started_at=started,
        ended_at=ended,
        rows_loaded=rows_loaded,
        streams=streams,
        full_refresh=False,
        error_type=error_type,
        error_message=error_message,
    )


def test_write_run_record_creates_table_and_inserts_row(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """write_run_record lazily creates _det_runs and writes one row per RunRecord."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    streams = (
        StreamResult(name="events", rows_extracted=4, rows_loaded=4),
        StreamResult(name="items", rows_extracted=5, rows_loaded=5, cursor_after=5),
    )
    hooks["write_run_record"](conn, _build_record(streams=streams, rows_loaded=9))
    hooks["close"](conn)

    rows = query_duckdb(
        duckdb_path,
        "SELECT run_id, config, source, destination, target, status, "
        "rows_loaded, full_refresh, duration_s, error_type FROM _det_runs",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "run-abc123"
    assert row[1] == "echo_dev"
    assert row[2] == "echo"
    assert row[3] == "duckdb"
    assert row[4] == "dev"
    assert row[5] == "succeeded"
    assert row[6] == 9
    assert row[7] is False
    assert row[8] == 5.0  # 12:00:05 - 12:00:00
    assert row[9] is None


def test_write_run_record_persists_streams_json(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """The per-stream breakdown lands as a JSON array readable via JSON ops."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    streams = (StreamResult(name="items", rows_loaded=3, cursor_after=42),)
    hooks["write_run_record"](conn, _build_record(streams=streams, rows_loaded=3))
    hooks["close"](conn)

    name = query_duckdb(
        duckdb_path, "SELECT streams_json->>'$[0].name' FROM _det_runs"
    )[0][0]
    assert name == "items"


def test_write_run_record_is_idempotent_on_run_id(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """Writing the same run_id twice updates rather than duplicating — docs/09 §4."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["write_run_record"](conn, _build_record(rows_loaded=1))
    hooks["write_run_record"](conn, _build_record(rows_loaded=99))
    hooks["close"](conn)

    rows = query_duckdb(duckdb_path, "SELECT count(*), max(rows_loaded) FROM _det_runs")
    assert rows[0] == (1, 99)


def test_write_run_record_persists_error_fields_on_failure(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """A failed run stores error_type + error_message; status is 'failed'."""
    hooks = _hooks(duckdb_destination)
    conn = _open(duckdb_destination, duckdb_path)
    hooks["write_run_record"](
        conn,
        _build_record(
            status=RunStatus.FAILED,
            rows_loaded=0,
            error_type="HTTPError",
            error_message="boom",
        ),
    )
    hooks["close"](conn)

    row = query_duckdb(
        duckdb_path,
        "SELECT status, error_type, error_message FROM _det_runs",
    )[0]
    assert row == ("failed", "HTTPError", "boom")
