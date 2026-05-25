"""Tests for the pre-baked Postgres source connector.

Unit tests (always run, no Postgres needed):
    * type_mapping coverage + unknown-type error
    * identifier quoting safety (the ``"; DROP TABLE x"`` test)
    * keyset / query / full-scan SQL-construction shape + params
    * the ``@stream`` body batches and observes the cursor correctly, using a
      stub connection injected via ``monkeypatch``.

Integration tests (gated by ``POSTGRES_TEST_URL``):
    * end-to-end ``det.run`` into a tmp DuckDB, asserting all rows landed
      on run 1 and 0 new rows on run 2 (cursor resume). Skipped when the
      env var is unset so the suite stays green on a fresh checkout.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from det import (
    Config,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
)
from det.sources.postgres import client, source, type_mapping

# Path the engine's discovery uses to find the postgres connector folder.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
POSTGRES_CONNECTOR_DIR = REPO_ROOT / "det" / "sources" / "postgres"


# ===========================================================================
# type_mapping — every documented mapping, plus the unknown-type error.
# ===========================================================================


@pytest.mark.parametrize(
    "pg_type,expected",
    [
        # STRING family
        ("text", FieldType.STRING),
        ("varchar", FieldType.STRING),
        ("character varying", FieldType.STRING),
        ("char", FieldType.STRING),
        ("character", FieldType.STRING),
        ("bpchar", FieldType.STRING),
        # INTEGER family
        ("smallint", FieldType.INTEGER),
        ("int2", FieldType.INTEGER),
        ("integer", FieldType.INTEGER),
        ("int", FieldType.INTEGER),
        ("int4", FieldType.INTEGER),
        ("bigint", FieldType.INTEGER),
        ("int8", FieldType.INTEGER),
        ("serial", FieldType.INTEGER),
        ("serial4", FieldType.INTEGER),
        ("bigserial", FieldType.INTEGER),
        ("serial8", FieldType.INTEGER),
        ("smallserial", FieldType.INTEGER),
        ("serial2", FieldType.INTEGER),
        # FLOAT family
        ("numeric", FieldType.FLOAT),
        ("decimal", FieldType.FLOAT),
        ("real", FieldType.FLOAT),
        ("float4", FieldType.FLOAT),
        ("double precision", FieldType.FLOAT),
        ("float8", FieldType.FLOAT),
        # BOOLEAN
        ("boolean", FieldType.BOOLEAN),
        ("bool", FieldType.BOOLEAN),
        # TIMESTAMP / DATE
        ("timestamp", FieldType.TIMESTAMP),
        ("timestamp without time zone", FieldType.TIMESTAMP),
        ("timestamptz", FieldType.TIMESTAMP),
        ("timestamp with time zone", FieldType.TIMESTAMP),
        ("date", FieldType.DATE),
        # JSON / BYTES / UUID
        ("json", FieldType.JSON),
        ("jsonb", FieldType.JSON),
        ("bytea", FieldType.BYTES),
        ("uuid", FieldType.STRING),
    ],
)
def test_postgres_to_field_type_known(pg_type: str, expected: FieldType) -> None:
    assert type_mapping.postgres_to_field_type(pg_type) is expected


@pytest.mark.parametrize(
    "pg_type",
    ["TEXT", "Integer", "  jsonb  ", "TIMESTAMPTZ"],
)
def test_postgres_to_field_type_case_insensitive_and_strips(pg_type: str) -> None:
    """The mapper accepts any case and trims surrounding whitespace."""
    type_mapping.postgres_to_field_type(pg_type)  # does not raise


def test_postgres_to_field_type_unknown_type_raises_clear_error() -> None:
    """Unknown types must raise — never a silent STRING fallback."""
    with pytest.raises(ValueError, match=r"unknown Postgres type 'inet'"):
        type_mapping.postgres_to_field_type("inet")


def test_postgres_to_field_type_non_string_raises() -> None:
    with pytest.raises(ValueError, match="expects a string"):
        type_mapping.postgres_to_field_type(42)  # type: ignore[arg-type]


def test_introspect_schema_builds_schema_from_information_schema() -> None:
    """``introspect_schema`` round-trips a canned cursor into a typed ``Schema``."""
    fake_cursor = MagicMock()
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_cursor.fetchall.return_value = [
        ("id", "integer", "NO"),
        ("email", "text", "YES"),
        ("created_at", "timestamp with time zone", "NO"),
        ("payload", "jsonb", "YES"),
    ]
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    schema = type_mapping.introspect_schema(fake_conn, "public", "users")

    assert schema.fields == (
        Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
        Field(name="email", type=FieldType.STRING, mode=FieldMode.NULLABLE),
        Field(name="created_at", type=FieldType.TIMESTAMP, mode=FieldMode.REQUIRED),
        Field(name="payload", type=FieldType.JSON, mode=FieldMode.NULLABLE),
    )

    # SQL parameterised, never f-stringed: schema_name + table_name are bound
    # values, not interpolated identifiers.
    sql_executed, params = fake_cursor.execute.call_args.args
    assert "information_schema.columns" in sql_executed
    assert "WHERE table_schema = %s AND table_name = %s" in sql_executed
    assert params == ("public", "users")


def test_introspect_schema_empty_table_raises() -> None:
    """A table the connecting user cannot see must not silently produce ``Schema()``."""
    fake_cursor = MagicMock()
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_cursor.fetchall.return_value = []
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    with pytest.raises(ValueError, match="no columns found"):
        type_mapping.introspect_schema(fake_conn, "public", "ghost_table")


# ===========================================================================
# Identifier quoting — the "; DROP TABLE x" guard
# ===========================================================================


def test_quote_identifier_neutralises_classic_injection() -> None:
    """A name carrying ``"; DROP TABLE x;`` must be safely quoted, not executed."""
    name = '"; DROP TABLE x; --'
    composed = client.quote_identifier(name)
    rendered = composed.as_string(None)
    # The composed form is the safely-quoted name — every embedded `"` is
    # doubled, the whole thing is wrapped in double quotes, and the DROP
    # keyword is sandwiched inside the quoted identifier.
    assert rendered.startswith('"') and rendered.endswith('"')
    assert '""' in rendered  # the embedded `"` was doubled
    # The dangerous keyword survives only as a column-name literal *inside*
    # the quoted identifier — Postgres would interpret the whole thing as one
    # silly column name, not as a statement.
    assert rendered.count('"') >= 3


def test_qualified_table_is_dot_joined_and_quoted() -> None:
    composed = client.qualified_table("public", "users")
    rendered = composed.as_string(None)
    assert rendered == '"public"."users"'


def test_qualified_table_quotes_evil_table_name() -> None:
    composed = client.qualified_table("public", '"; DROP TABLE x; --')
    rendered = composed.as_string(None)
    assert rendered.startswith('"public".') and rendered.endswith('"')
    assert '""' in rendered  # the embedded `"` was doubled


# ===========================================================================
# SQL-construction helpers — keyset / query / full-scan
# ===========================================================================


def _normalise_sql(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def test_keyset_select_sql_shape_includes_cursor_pk_limit() -> None:
    composed = client.keyset_select_sql(
        schema_name="public",
        table_name="users",
        cursor_field="updated_at",
        primary_key=("id",),
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT * FROM "public"."users" '
        'WHERE "updated_at" > %s '
        'ORDER BY "updated_at", "id" '
        'LIMIT %s'
    )


def test_keyset_select_sql_with_composite_primary_key() -> None:
    composed = client.keyset_select_sql(
        schema_name="public",
        table_name="orders",
        cursor_field="updated_at",
        primary_key=("tenant_id", "order_id"),
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert 'ORDER BY "updated_at", "tenant_id", "order_id"' in sql_text


def test_keyset_select_sql_with_no_primary_key_orders_by_cursor_only() -> None:
    composed = client.keyset_select_sql(
        schema_name="public",
        table_name="events",
        cursor_field="created_at",
        primary_key=(),
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert 'ORDER BY "created_at" LIMIT %s' in sql_text


def test_query_select_sql_wraps_user_query_as_subquery() -> None:
    composed = client.query_select_sql(
        user_query="SELECT id, occurred_at FROM events WHERE kind = 'click'",
        cursor_field="occurred_at",
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text.startswith(
        'SELECT * FROM (SELECT id, occurred_at FROM events WHERE kind = \'click\') '
        'AS _det_sub WHERE "occurred_at" > %s ORDER BY "occurred_at" LIMIT %s'
    )


def test_full_scan_select_sql_is_unconditional_select() -> None:
    composed = client.full_scan_select_sql("public", "products")
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == 'SELECT * FROM "public"."products"'


def test_declare_cursor_sql_quotes_cursor_name() -> None:
    body = client.full_scan_select_sql("public", "products")
    composed = client.declare_cursor_sql("det_x", body)
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text.startswith('DECLARE "det_x" CURSOR FOR ')


def test_fetch_forward_sql_uses_literal_batch_size_and_quoted_name() -> None:
    composed = client.fetch_forward_sql("det_x", 1000)
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == 'FETCH FORWARD 1000 FROM "det_x"'


def test_fetch_forward_sql_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="positive int"):
        client.fetch_forward_sql("det_x", 0)
    with pytest.raises(ValueError, match="positive int"):
        client.fetch_forward_sql("det_x", -1)


def test_close_cursor_sql_quotes_name() -> None:
    composed = client.close_cursor_sql("det_x")
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == 'CLOSE "det_x"'


# ===========================================================================
# Pagination math + cursor observation — fake-connection test
# ===========================================================================


class _FakeColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    """A psycopg-shaped cursor that yields pre-staged result batches.

    The body of the connector calls (in order)::

        cur.execute(sql, params)
        column_names = [d.name for d in (cur.description or [])]
        rows = cur.fetchall()

    in a loop. This fake records each ``execute`` (so a test can assert on
    the SQL + bound params), then returns the next staged batch from a
    pre-set queue.
    """

    def __init__(
        self,
        batches: list[list[tuple[Any, ...]]],
        column_names: list[str],
    ) -> None:
        self._batches = list(batches)
        self.description = [_FakeColumn(n) for n in column_names]
        self.executes: list[tuple[Any, ...]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: Any, params: Any = None) -> None:
        self.executes.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._batches:
            return self._batches.pop(0)
        return []


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def silent_log() -> logging.Logger:
    log = logging.getLogger("test.postgres")
    log.addHandler(logging.NullHandler())
    return log


def test_extract_table_keyset_batches_and_observes_cursor(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """The keyset path yields one batch per server page and observes each cursor."""
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 10), (2, "b", 20)],
        [(3, "c", 30), (4, "d", 40)],
        # short page (< batch_size) — terminates the loop
        [(5, "e", 50)],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    config = Config(
        params={
            "host": "localhost", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 2,
        },
        secrets={"password": "redacted"},
    )
    cursor = Cursor(
        cursor_field="updated_at",
        cursor_type=CursorType.INT,
        start_value=0,
    )

    batches = list(
        source.extract_stream(
            stream_name="users",
            config=config, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )

    # 3 batches yielded, exactly as staged.
    assert len(batches) == 3
    assert batches[0] == [
        {"id": 1, "label": "a", "updated_at": 10},
        {"id": 2, "label": "b", "updated_at": 20},
    ]
    # Cursor observed the max — 50 was the highest updated_at across all batches.
    assert cursor.observed_max == 50

    # Three executes — each bound the current floor + page size.
    assert len(fake_cursor.executes) == 3
    _, params0 = fake_cursor.executes[0]
    _, params1 = fake_cursor.executes[1]
    _, params2 = fake_cursor.executes[2]
    assert params0 == (0, 2)        # initial floor
    assert params1 == (20, 2)       # last cursor of batch 0
    assert params2 == (40, 2)       # last cursor of batch 1

    # The connection was closed when the generator exhausted.
    assert fake_conn.closed is True


def test_extract_table_keyset_stops_on_stagnant_cursor(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """A full page whose cursor does not advance must not loop forever."""
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 10), (2, "b", 10)],  # full page (batch_size=2), cursor unchanged
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    config = Config(
        params={
            "host": "h", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 2,
        },
        secrets={"password": "."},
    )
    cursor = Cursor(
        cursor_field="updated_at", cursor_type=CursorType.INT, start_value=10,
    )

    list(
        source.extract_stream(
            stream_name="users",
            config=config, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    # One execute, then bailout — would loop forever otherwise.
    assert len(fake_cursor.executes) == 1


def test_extract_query_mode_wraps_user_query(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    pages: list[list[tuple[Any, ...]]] = [
        [(100, "click", 5), (101, "view", 7)],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "kind", "occurred_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    config = Config(
        params={
            "host": "h", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 100,
        },
        secrets={"password": "."},
    )
    cursor = Cursor(
        cursor_field="occurred_at", cursor_type=CursorType.INT, start_value=0,
    )

    batches = list(
        source.extract_stream(
            stream_name="events",
            config=config, cursor=cursor, log=silent_log,
            query="SELECT id, kind, occurred_at FROM events WHERE kind = 'click'",
            cursor_field="occurred_at",
        )
    )

    assert batches == [
        [
            {"id": 100, "kind": "click", "occurred_at": 5},
            {"id": 101, "kind": "view", "occurred_at": 7},
        ]
    ]
    sql_text = _normalise_sql(fake_cursor.executes[0][0].as_string(None))
    assert "AS _det_sub" in sql_text
    assert '"occurred_at"' in sql_text
    assert cursor.observed_max == 7


def test_extract_stream_rejects_both_table_and_query(silent_log: logging.Logger) -> None:
    config = Config(params={"batch_size": 1}, secrets={})
    cursor = Cursor(cursor_field="x", cursor_type=CursorType.INT, start_value=0)
    with pytest.raises(ValueError, match="mutually exclusive"):
        list(
            source.extract_stream(
                stream_name="bad",
                config=config, cursor=cursor, log=silent_log,
                table_name="t", query="SELECT 1",
            )
        )


def test_extract_stream_rejects_neither_table_nor_query(silent_log: logging.Logger) -> None:
    config = Config(params={"batch_size": 1}, secrets={})
    cursor = Cursor(cursor_field="x", cursor_type=CursorType.INT, start_value=0)
    with pytest.raises(ValueError, match="exactly one of table_name / query"):
        list(
            source.extract_stream(
                stream_name="bad",
                config=config, cursor=cursor, log=silent_log,
            )
        )


def test_extract_stream_rejects_query_without_cursor(silent_log: logging.Logger) -> None:
    config = Config(params={"batch_size": 1}, secrets={})
    with pytest.raises(ValueError, match="incremental-only"):
        list(
            source.extract_stream(
                stream_name="bad",
                config=config, cursor=None, log=silent_log,
                query="SELECT 1", cursor_field="x",
            )
        )


def test_extract_full_scan_runs_declare_fetch_close(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """The no-cursor path runs DECLARE → FETCH → ... → CLOSE inside a transaction."""
    # FETCH forward returns 2 rows, then 1 row (short page), then loop ends.
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "alpha"), (2, "beta")],
        [(3, "gamma")],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "name"])

    class _FakeTxn:
        def __enter__(self) -> _FakeTxn:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeConnWithTxn(_FakeConn):
        def transaction(self) -> _FakeTxn:
            return _FakeTxn()

    fake_conn = _FakeConnWithTxn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    config = Config(
        params={
            "host": "h", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 2,
        },
        secrets={"password": "."},
    )

    batches = list(
        source.extract_stream(
            stream_name="things",
            config=config, cursor=None, log=silent_log,
            schema_name="public", table_name="things",
        )
    )

    assert batches == [
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        [{"id": 3, "name": "gamma"}],
    ]
    # Executes recorded: DECLARE, FETCH, FETCH, then CLOSE (after the short
    # page broke the loop, the finally branch closed the server cursor).
    rendered = [
        e[0].as_string(None) if hasattr(e[0], "as_string") else str(e[0])
        for e in fake_cursor.executes
    ]
    assert rendered[0].startswith("DECLARE ")
    assert rendered[1].startswith("FETCH FORWARD ")
    assert rendered[-1].startswith("CLOSE ")
    assert fake_conn.closed is True


def test_connection_closes_on_exception(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """The connection is closed even if the underlying cursor raises mid-stream."""

    class _ExplodingCursor(_FakeCursor):
        def execute(self, sql: Any, params: Any = None) -> None:
            raise RuntimeError("kaboom")

    fake_cursor = _ExplodingCursor([], column_names=[])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    config = Config(
        params={
            "host": "h", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 1,
        },
        secrets={"password": "."},
    )
    cursor = Cursor(cursor_field="x", cursor_type=CursorType.INT, start_value=0)

    with pytest.raises(RuntimeError, match="kaboom"):
        list(
            source.extract_stream(
                stream_name="users",
                config=config, cursor=cursor, log=silent_log,
                schema_name="public", table_name="users",
                cursor_field="x", primary_key=("x",),
            )
        )
    assert fake_conn.closed is True


# ===========================================================================
# Password redaction — assert the password is not in any rendered SQL
# ===========================================================================


def test_password_does_not_appear_in_rendered_sql(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """A secret password must never reach a composed SQL fragment, even by accident."""
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "alpha", 10)],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(source, "connect", lambda config: fake_conn)

    secret = "S3cr3t_p@ssw0rd_DO_NOT_LEAK"
    config = Config(
        params={
            "host": "h", "port": 5432, "database": "x", "user": "u",
            "sslmode": "prefer", "application_name": "det",
            "connect_timeout_seconds": 30, "batch_size": 100,
        },
        secrets={"password": secret},
    )
    cursor = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=0)

    list(
        source.extract_stream(
            stream_name="users",
            config=config, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    rendered = " ".join(
        e[0].as_string(None) if hasattr(e[0], "as_string") else str(e[0])
        for e in fake_cursor.executes
    )
    assert secret not in rendered


# ===========================================================================
# Connector discovery — manifest parses + @stream decorators register
# ===========================================================================


def test_postgres_connector_discovers_and_validates(tmp_path: Path) -> None:
    """The full discovery flow finds the source folder and validates it."""
    from det.engine.discovery import resolve_source

    (tmp_path / "det_project.yml").write_text(
        "name: test_project\nversion: '1.0.0'\nsource_paths: [sources]\n"
    )
    (tmp_path / "sources").mkdir()  # empty — forces fallback to baked

    loaded = resolve_source("postgres", tmp_path)
    assert loaded.manifest.name == "postgres"
    assert loaded.manifest.kind.value == "source"
    assert {s.name for s in loaded.manifest.streams} == {"users", "events"}
    assert {n for n in loaded.registry.stream_names} == {"users", "events"}
    # Every stream has the (config, cursor, log) inject list the engine expects.
    for name in ("users", "events"):
        reg = loaded.registry.stream(name)
        assert reg is not None
        assert set(reg.inject) == {"config", "cursor", "log"}


# ===========================================================================
# Integration tests — gated by POSTGRES_TEST_URL
# ===========================================================================


pytestmark_integration = pytest.mark.skipif(
    not os.getenv("POSTGRES_TEST_URL"),
    reason="needs a live Postgres at POSTGRES_TEST_URL",
)


@pytest.fixture
def live_pg_conn() -> Iterator[psycopg.Connection[Any]]:
    """A live Postgres connection from ``POSTGRES_TEST_URL`` for integration tests."""
    url = os.environ["POSTGRES_TEST_URL"]
    conn = psycopg.connect(url)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.integration
@pytestmark_integration
def test_integration_end_to_end_first_run_loads_then_resumes(
    live_pg_conn: psycopg.Connection[Any], tmp_path: Path
) -> None:
    """End-to-end: 100 rows land on run 1, 0 new rows on run 2 (cursor resume).

    Stands up a temp schema with a tiny ``users`` table, runs ``det.run``
    into a tmp DuckDB twice, and asserts the spec's resume property.
    """
    import det

    schema = f"det_pg_it_{os.getpid()}"
    with live_pg_conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(
            f'CREATE TABLE "{schema}".users ('
            "id integer PRIMARY KEY, "
            "email text, "
            "full_name text, "
            "created_at timestamptz, "
            "updated_at timestamptz NOT NULL)"
        )
        cur.executemany(
            f'INSERT INTO "{schema}".users VALUES (%s, %s, %s, NOW(), NOW())',
            [(i, f"u{i}@x.com", f"u{i}") for i in range(100)],
        )
    live_pg_conn.commit()

    # A throwaway project that binds the postgres connector via the baked
    # folder. We override the postgres schema_name + table_name by overriding
    # the @stream body inside a project-local fork of the connector.
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "det_project.yml").write_text(
        "name: it\nversion: '1.0.0'\nsource_paths: [sources]\n"
        "destination_paths: [destinations]\nconfig_paths: [configs]\n"
    )
    (project_dir / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n      path: "
        f"'{tmp_path}/wh.duckdb'\n"
    )
    (project_dir / "configs").mkdir()
    (project_dir / "configs" / "pg_dev.yml").write_text(
        "name: pg_dev\nsource: postgres\ndestination: duckdb\ntarget: dev\n"
    )
    fork = project_dir / "sources" / "postgres"
    fork.mkdir(parents=True)
    (fork / "register.yaml").write_text(
        "name: postgres\nkind: source\nversion: '1.0.0'\n"
        "params:\n"
        "  host: {type: string, required: true}\n"
        "  port: {type: int, default: 5432}\n"
        "  database: {type: string, required: true}\n"
        "  user: {type: string, required: true}\n"
        "  sslmode: {type: string, default: prefer}\n"
        "  application_name: {type: string, default: det}\n"
        "  connect_timeout_seconds: {type: int, default: 30}\n"
        "  batch_size: {type: int, default: 30}\n"
        "secrets:\n  - {name: password, ref: '${env.PGPASSWORD}'}\n"
        "streams:\n"
        "  - name: users\n"
        "    table: pg_users\n"
        "    primary_key: id\n"
        "    write_disposition: merge\n"
        "    incremental:\n"
        "      cursor_field: updated_at\n"
        "      cursor_type: timestamp\n"
        "      initial_value: '1970-01-01T00:00:00'\n"
    )
    (fork / "source.py").write_text(
        "from det import stream\n"
        "from det.sources.postgres.source import extract_stream\n\n"
        "@stream(name='users')\n"
        "def users(config, cursor, log):\n"
        "    yield from extract_stream(\n"
        "        stream_name='users', config=config, cursor=cursor, log=log,\n"
        f"        schema_name='{schema}', table_name='users',\n"
        "        cursor_field='updated_at', primary_key=('id',),\n"
        "    )\n"
    )

    info = live_pg_conn.info
    overrides = {
        "host": info.host,
        "port": info.port,
        "database": info.dbname,
        "user": info.user,
    }
    # Run 1 — every row lands.
    r1 = det.run(
        config="pg_dev", project_dir=str(project_dir),
        params_override=overrides,
    )
    assert r1.status.value == "succeeded", r1.error
    assert (r1.stream("users").rows_loaded if r1.stream("users") else 0) == 100  # type: ignore[union-attr]

    # Run 2 — cursor resumes, nothing new to fetch.
    r2 = det.run(
        config="pg_dev", project_dir=str(project_dir),
        params_override=overrides,
    )
    assert r2.status.value == "succeeded", r2.error
    assert (r2.stream("users").rows_loaded if r2.stream("users") else 0) == 0  # type: ignore[union-attr]

    # Cleanup the test schema.
    with live_pg_conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    live_pg_conn.commit()
